from __future__ import annotations

import itertools
import math
import random
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Dict, List, Optional

import numpy as np
import simpy

from .config import CapacityPlan, FarmSpec, SimulationConfig, derive_capacity
from flow_writer import FlowWriter
from .logger import EventLogger


@dataclass
class CartResult:
    cart_id: str
    shipment_id: str
    eggs: int
    discarded: int
    fertile: int
    hatch_losses: int
    chicks: int


@dataclass
class BarnPlace:
    farm_name: str
    place_id: str
    capacity: int
    occupants: Dict[str, int] = field(default_factory=dict)
    occupied: int = 0
    cleaning_until: float = 0.0

    @property
    def remaining_capacity(self) -> int:
        return max(0, self.capacity - self.occupied)

    def add(self, shipment_id: str, amount: int) -> None:
        self.occupants[shipment_id] = self.occupants.get(shipment_id, 0) + amount
        self.occupied += amount

    def decrement(self, shipment_id: str, amount: int) -> int:
        if shipment_id not in self.occupants:
            return 0
        current = self.occupants[shipment_id]
        removal = min(current, amount)
        if removal >= current:
            self.occupants.pop(shipment_id, None)
        else:
            self.occupants[shipment_id] = current - removal
        self.occupied = max(0, self.occupied - removal)
        return removal


@dataclass
class SlaughterRecord:
    day: float
    shipment_id: str
    quantity: int
    farm: str


class ChickSimulation:
    """Coordinate all SimPy processes for the hatchery → slaughter workflow.

    Writes two streams:
    - SQLite events (optional, for audit/debug; JSONL fallback if sqlite3 missing)
    - Parquet flow log (authoritative input for graphs/analysis).

    Parent‑pair is embedded into the Parquet `metadata` of the initial
    `inventory` flow entry to avoid any SQLite dependency downstream.
    """

    def __init__(self, cfg: SimulationConfig, logger: EventLogger) -> None:
        self.cfg = cfg
        self.logger = logger
        self.env = simpy.Environment()
        self.capacity_plan: CapacityPlan = derive_capacity(cfg)

        self.pre_hatch_slots = simpy.FilterStore(self.env, capacity=cfg.total_pre_hatch_slots)
        self.hatch_slots = simpy.FilterStore(self.env, capacity=cfg.total_hatch_slots)
        self._populate_machine_slots()

        self.truck_slots = simpy.Container(
            self.env, capacity=cfg.active_trucks, init=cfg.active_trucks
        )

        self.barn_places: List[BarnPlace] = self._initialise_barn_places(cfg.farm_specs)
        self._barn_index: int = 0
        self.barn_lock = simpy.Resource(self.env, capacity=1)

        self.shipment_counter = itertools.count()
        self.parent_pairs = itertools.cycle(range(1, 16))
        # Always-on cohesion: prefer keeping a shipment's carts on the same setter/hatcher machine
        self._preferred_setter: Dict[str, str] = {}
        self._preferred_hatcher: Dict[str, str] = {}

        self.slaughter_records: List[SlaughterRecord] = []
        self._timestamp_cache: Optional[str] = None
        self.flow_writer = FlowWriter(self.cfg.flow_log_path)

    def _populate_machine_slots(self) -> None:
        """Pre‑allocate setter/hatcher slot identifiers into SimPy stores."""
        for machine in range(1, self.cfg.pre_hatch_machines + 1):
            for slot in range(1, self.cfg.pre_hatch_carts_per_machine + 1):
                slot_id = f"setter-{machine:02d}-cart-{slot:02d}"
                self.pre_hatch_slots.put(slot_id)  # type: ignore[arg-type]
        for machine in range(1, self.cfg.hatch_machines + 1):
            for slot in range(1, self.cfg.hatch_carts_per_machine + 1):
                slot_id = f"hatcher-{machine:02d}-cart-{slot:02d}"
                self.hatch_slots.put(slot_id)  # type: ignore[arg-type]

    def _initialise_barn_places(self, specs: List[FarmSpec]) -> List[BarnPlace]:
        """Expand farm capacity into evenly split barn places with capacities."""
        places: List[BarnPlace] = []
        for spec in specs:
            base = spec.capacity_chicks // spec.barns
            extra = spec.capacity_chicks % spec.barns
            for barn_idx in range(spec.barns):
                capacity = base + (1 if barn_idx < extra else 0)
                place_id = f"{spec.name}-barn-{barn_idx + 1:02d}"
                places.append(BarnPlace(spec.name, place_id, capacity))
        return places

    def run(self) -> None:
        """Run the simulation until configured horizon and close flow log."""
        self.env.process(self._generate_shipments())
        self.env.run(until=self.cfg.simulation_days)
        self.flow_writer.close()

    def _current_timestamp(self) -> str:
        return (self.cfg.start_date + timedelta(days=self.env.now)).isoformat()

    def _log(
        self,
        entity_id: str,
        stage: str,
        status: str,
        quantity: Optional[float],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.logger.log(
            self.env.now,
            entity_id,
            stage,
            status,
            quantity,
            metadata,
            real_ts=self._current_timestamp(),
        )

    def _generate_shipments(self):  # type: ignore[override]
        """Poisson process of shipments per day with cycling parent pairs."""
        mean_shipments = self.capacity_plan.shipments_per_day
        while True:
            shipments_today = max(1, np.random.poisson(mean_shipments))
            interval = 1.0 / shipments_today
            for _ in range(shipments_today):
                shipment_id = f"shipment-{next(self.shipment_counter)}"
                parent_pair_id = next(self.parent_pairs)
                self.env.process(self._handle_shipment(shipment_id, parent_pair_id))
                yield self.env.timeout(interval)
            yield self.env.timeout(max(0.0, 1.0 - shipments_today * interval))

    def _handle_shipment(self, shipment_id: str, parent_pair_id: int):  # type: ignore[override]
        """Process one shipment through setter, hatcher, processing and trucks."""
        eggs = self.cfg.shipment_eggs
        farm_cars = math.ceil(eggs / self.cfg.farm_car_eggs)
        self._log(
            shipment_id,
            "inventory",
            "arrived",
            eggs,
            {
                "parent_pair": f"parent-pair-{parent_pair_id}",
                "farm_cars": farm_cars,
            },
        )

        cart_processes = []
        remaining = eggs
        cart_index = 0
        while remaining > 0:
            cart_index += 1
            cart_eggs = min(self.cfg.pre_hatch_cart_eggs, remaining)
            cart_id = f"cart-{shipment_id}-{cart_index:02d}"
            proc = self.env.process(self._process_cart(shipment_id, cart_id, cart_eggs))
            cart_processes.append(proc)
            remaining -= cart_eggs

        if cart_processes:
            yield simpy.events.AllOf(self.env, cart_processes)
        cart_results = [proc.value for proc in cart_processes]

        total_discarded = sum(result.discarded for result in cart_results)
        total_fertile = sum(result.fertile for result in cart_results)
        total_chicks = sum(result.chicks for result in cart_results)
        total_hatch_losses = sum(result.hatch_losses for result in cart_results)

        # Write the parent_pair into the flow log so downstream analysis doesn't need SQLite
        self.flow_writer.log(
            shipment_id,
            resource_id="inventory",
            resource_type="inventory",
            from_state={"status": "arrived"},
            to_state={"status": "to_pre_hatch", "carts": len(cart_results)},
            event_ts=self._current_timestamp(),
            quantity=eggs,
            metadata={"parent_pair": f"parent-pair-{parent_pair_id}"},
        )
        self.flow_writer.log(
            shipment_id,
            resource_id="xray",
            resource_type="process",
            from_state={"status": "before"},
            to_state={"status": "discarded"},
            event_ts=self._current_timestamp(),
            quantity=total_discarded,
        )
        self.flow_writer.log(
            shipment_id,
            resource_id="hatch_room",
            resource_type="process",
            from_state={"fertile": total_fertile + total_discarded},
            to_state={"fertile": total_fertile},
            event_ts=self._current_timestamp(),
            quantity=total_fertile,
        )
        self.flow_writer.log(
            shipment_id,
            resource_id="hatch_room",
            resource_type="process",
            from_state={"fertile": total_fertile},
            to_state={"chicks": total_chicks, "losses": total_hatch_losses},
            event_ts=self._current_timestamp(),
            quantity=total_chicks,
        )

        if total_chicks <= 0:
            self.flow_writer.log(
                shipment_id,
                resource_id="hatch_room",
                resource_type="process",
                from_state={"status": "empty"},
                to_state=None,
                event_ts=self._current_timestamp(),
                quantity=0,
            )
            return

        yield self.env.timeout(self.cfg.sort_and_vaccinate_hours / 24)
        self.flow_writer.log(
            shipment_id,
            resource_id="processing",
            resource_type="process",
            from_state={"status": "pre_processing"},
            to_state={"status": "processed"},
            event_ts=self._current_timestamp(),
            quantity=total_chicks,
        )

        yield self.env.timeout(self.cfg.load_to_transport_hours / 24)
        trucks_needed = max(1, math.ceil(total_chicks / self.cfg.chicks_per_truck))
        self.flow_writer.log(
            shipment_id,
            resource_id="transport_loading",
            resource_type="logistics",
            from_state={"status": "scheduled"},
            to_state={"trucks": trucks_needed},
            event_ts=self._current_timestamp(),
            quantity=total_chicks,
        )

        remaining_chicks = total_chicks
        truck_counter = 0
        while remaining_chicks > 0:
            truck_counter += 1
            load = min(self.cfg.chicks_per_truck, remaining_chicks)
            remaining_chicks -= load
            yield self.truck_slots.get(1)
            truck_id = f"{shipment_id}-truck-{truck_counter:02d}"
            self.flow_writer.log(
                shipment_id,
                resource_id=truck_id,
                resource_type="truck",
                from_state={"status": "loading"},
                to_state={"status": "in_transit"},
                event_ts=self._current_timestamp(),
                quantity=load,
            )
            yield self.env.timeout(self.cfg.transport_time_days)
            self.flow_writer.log(
                shipment_id,
                resource_id=truck_id,
                resource_type="truck",
                from_state={"status": "in_transit"},
                to_state={"status": "arrived"},
                event_ts=self._current_timestamp(),
                quantity=load,
            )
            yield from self._place_truck(shipment_id, truck_id, load)
            self.truck_slots.put(1)

    def _process_cart(
        self, shipment_id: str, cart_id: str, eggs: int
    ) -> simpy.events.Event:
        """Run one cart through setter and hatcher, logging flow transitions."""
        pref_setter = self._preferred_setter.get(shipment_id)
        if pref_setter:
            slot_id = yield self.pre_hatch_slots.get(lambda s: str(s).startswith(pref_setter))  # type: ignore[arg-type]
        else:
            slot_id = yield self.pre_hatch_slots.get()
        self.flow_writer.log(
            shipment_id,
            resource_id=slot_id,
            resource_type="setter_slot",
            from_state={"status": "empty"},
            to_state={"cart_id": cart_id, "eggs": eggs},
            event_ts=self._current_timestamp(),
            quantity=eggs,
        )
        yield self.env.timeout(self.cfg.pre_hatch_days)
        yield self.pre_hatch_slots.put(slot_id)
        if shipment_id not in self._preferred_setter:
            self._preferred_setter[shipment_id] = str(slot_id).split('-cart')[0]
        self.flow_writer.log(
            shipment_id,
            resource_id=slot_id,
            resource_type="setter_slot",
            from_state={"cart_id": cart_id, "eggs": eggs},
            to_state={"status": "released"},
            event_ts=self._current_timestamp(),
            quantity=eggs,
        )

        pass_rate = self.rng.betavariate(self.cfg.xray_alpha, self.cfg.xray_beta)
        fertile = int(round(eggs * pass_rate))
        discarded = eggs - fertile

        pref_hatcher = self._preferred_hatcher.get(shipment_id)
        if pref_hatcher:
            hatch_slot_id = yield self.hatch_slots.get(lambda s: str(s).startswith(pref_hatcher))  # type: ignore[arg-type]
        else:
            hatch_slot_id = yield self.hatch_slots.get()
        self.flow_writer.log(
            shipment_id,
            resource_id=hatch_slot_id,
            resource_type="hatcher_slot",
            from_state={"status": "empty"},
            to_state={"cart_id": cart_id, "fertile": fertile},
            event_ts=self._current_timestamp(),
            quantity=fertile,
        )
        hatch_duration = self.rng.uniform(*self.cfg.hatch_days_range)
        yield self.env.timeout(hatch_duration)
        if shipment_id not in self._preferred_hatcher:
            self._preferred_hatcher[shipment_id] = str(hatch_slot_id).split('-cart')[0]

        hatch_rate = self.rng.betavariate(self.cfg.hatch_alpha, self.cfg.hatch_beta)
        chicks = int(round(fertile * hatch_rate))
        hatch_losses = fertile - chicks

        yield self.hatch_slots.put(hatch_slot_id)
        self.flow_writer.log(
            shipment_id,
            resource_id=hatch_slot_id,
            resource_type="hatcher_slot",
            from_state={"cart_id": cart_id, "fertile": fertile},
            to_state={"chicks": chicks, "losses": hatch_losses},
            event_ts=self._current_timestamp(),
            quantity=chicks,
        )

        return CartResult(
            cart_id=cart_id,
            shipment_id=shipment_id,
            eggs=eggs,
            discarded=discarded,
            fertile=fertile,
            hatch_losses=hatch_losses,
            chicks=chicks,
        )

    def _place_truck(self, shipment_id: str, truck_id: str, load: int):
        """Atomically place a truck's load: one barn log per affected place.

        This avoids producing multiple incremental barn state changes per truck,
        which previously led to small +/- corrections in timeline diffs.
        """
        # Build a cohesive placement plan: prefer empty places, then places already
        # containing this shipment, then mix with others only if necessary.
        plan: list[tuple[str, int]] = []
        remaining = load
        def recompute_order() -> list[str]:
            empty = [p.place_id for p in self.barn_places if self.env.now >= p.cleaning_until and p.occupied == 0 and p.remaining_capacity > 0]
            own = [p.place_id for p in self.barn_places if self.env.now >= p.cleaning_until and shipment_id in p.occupants and p.remaining_capacity > 0]
            other = [p.place_id for p in self.barn_places if self.env.now >= p.cleaning_until and p.occupied > 0 and shipment_id not in p.occupants and p.remaining_capacity > 0]
            return empty + own + other

        ordered_ids = recompute_order()
        idx = 0
        while remaining > 0:
            if idx >= len(ordered_ids):
                # wait for capacity to free up or cleaning to complete
                yield self.env.timeout(0.5)
                ordered_ids = recompute_order()
                idx = 0
                if not ordered_ids:
                    continue
            candidate_id = ordered_ids[idx]
            idx += 1
            with self.barn_lock.request() as req:
                yield req
                confirmed = self._find_specific_place(candidate_id)
                if confirmed is None or confirmed.remaining_capacity <= 0:
                    continue
                portion = min(confirmed.remaining_capacity, remaining)
                if portion <= 0:
                    continue
                plan.append((confirmed.place_id, portion))
                remaining -= portion

        # Apply the plan: mutate places and write a single event per place
        for place_id, amount in plan:
            with self.barn_lock.request() as req:
                yield req
                confirmed_place = self._find_specific_place(place_id)
                if confirmed_place is None:
                    continue
                before_mix = confirmed_place.occupants.copy()
                confirmed_place.add(shipment_id, amount)
                after_mix = confirmed_place.occupants.copy()
            # Log concise intake event
            self._log(
                shipment_id,
                "farm_intake",
                "placed",
                amount,
                {
                    "truck_id": truck_id,
                    "place_id": place_id,
                    "farm": confirmed_place.farm_name,
                    "remaining_capacity": confirmed_place.remaining_capacity,
                },
            )
            # Maintain compatibility: still write barn state snapshot, but once
            self.flow_writer.log(
                shipment_id,
                resource_id=place_id,
                resource_type="barn",
                from_state=before_mix,
                to_state=after_mix,
                event_ts=self._current_timestamp(),
                quantity=amount,
                metadata={"farm": confirmed_place.farm_name, "truck_id": truck_id},
            )

    def _find_next_place(self) -> Optional[BarnPlace]:
        total_places = len(self.barn_places)
        for offset in range(total_places):
            idx = (self._barn_index + offset) % total_places
            place = self.barn_places[idx]
            if self.env.now < place.cleaning_until:
                continue
            if place.remaining_capacity > 0:
                self._barn_index = (idx + 1) % total_places
                return place
        return None

    def _find_specific_place(self, place_id: str) -> Optional[BarnPlace]:
        for place in self.barn_places:
            if place.place_id == place_id and self.env.now >= place.cleaning_until:
                return place
        return None

    def _manage_farm_cycle(self, shipment_id: str, amount: int, place: BarnPlace):
        grow_out_time = self.rng.uniform(*self.cfg.grow_out_days_range)
        yield self.env.timeout(grow_out_time)
        survival_rate = self.rng.betavariate(self.cfg.farm_alpha, self.cfg.farm_beta)
        survivors = int(round(amount * survival_rate))
        losses = amount - survivors
        self._log(
            shipment_id,
            "grow_out",
            "completed",
            survivors,
            {
                "losses": losses,
                "place_id": place.place_id,
                "farm": place.farm_name,
            },
        )
        self._log(
            shipment_id,
            "slaughter",
            "shipped",
            survivors,
            {"place_id": place.place_id, "farm": place.farm_name},
        )
        self.slaughter_records.append(
            SlaughterRecord(
                day=self.env.now,
                shipment_id=shipment_id,
                quantity=survivors,
                farm=place.farm_name,
            )
        )

        before_mix = place.occupants.copy()
        removed = place.decrement(shipment_id, amount)
        after_mix = place.occupants.copy()
        self.flow_writer.log(
            shipment_id,
            resource_id=place.place_id,
            resource_type="barn",
            from_state=before_mix,
            to_state=after_mix,
            event_ts=self._current_timestamp(),
            quantity=removed,
            metadata={"farm": place.farm_name, "status": "removal"},
        )
        if place.occupied == 0:
            place.cleaning_until = self.env.now + self.cfg.cleaning_days
            self._log(
                shipment_id,
                "farm_place",
                "available",
                removed,
                {
                    "place_id": place.place_id,
                    "farm": place.farm_name,
                    "removed_shipment": shipment_id,
                    "removed_amount": removed,
                },
            )
            yield self.env.timeout(self.cfg.cleaning_days)
            place.cleaning_until = 0.0
        else:
            self._log(
                shipment_id,
                "farm_place",
                "partial_release",
                removed,
                {
                    "place_id": place.place_id,
                    "farm": place.farm_name,
                    "remaining_capacity": place.remaining_capacity,
                    "mix": place.occupants.copy(),
                    "removed_shipment": shipment_id,
                    "removed_amount": removed,
                },
            )

    @property
    def slaughtered_after_warmup(self) -> List[int]:
        cutoff = self.cfg.warmup_days
        return [
            record.quantity
            for record in self.slaughter_records
            if record.day >= cutoff
        ]

    def summarize(self) -> Dict[str, Optional[float]]:
        harvested = self.slaughtered_after_warmup
        if not harvested:
            total = sum(record.quantity for record in self.slaughter_records)
            return {"avg_slaughter_per_day": None, "total_slaughtered": total}
        total_output = sum(harvested)
        days_tracked = max(1, self.cfg.simulation_days - self.cfg.warmup_days)
        avg_per_day = total_output / days_tracked
        total = sum(record.quantity for record in self.slaughter_records)
        return {"avg_slaughter_per_day": avg_per_day, "total_slaughtered": total}

    @property
    def rng(self) -> random.Random:
        if not hasattr(self, "_rng"):
            self._rng = random.Random()
        return self._rng
