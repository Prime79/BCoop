from __future__ import annotations

import itertools
import math
import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np
import simpy

from .config import CapacityPlan, FarmSpec, SimulationConfig, derive_capacity
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
    """Coordinate all SimPy processes for the hatchery-to-slaughter workflow."""

    def __init__(self, cfg: SimulationConfig, logger: EventLogger) -> None:
        self.cfg = cfg
        self.logger = logger
        self.env = simpy.Environment()
        self.capacity_plan: CapacityPlan = derive_capacity(cfg)

        self.pre_hatch_slots = simpy.Store(self.env, capacity=cfg.total_pre_hatch_slots)
        self.hatch_slots = simpy.Store(self.env, capacity=cfg.total_hatch_slots)
        self._populate_machine_slots()

        self.truck_slots = simpy.Container(
            self.env, capacity=cfg.active_trucks, init=cfg.active_trucks
        )

        self.barn_places: List[BarnPlace] = self._initialise_barn_places(cfg.farm_specs)
        self._barn_index: int = 0
        self.barn_lock = simpy.Resource(self.env, capacity=1)

        self.shipment_counter = itertools.count()
        self.egg_sources = itertools.cycle(range(1, 16))

        self.slaughter_records: List[SlaughterRecord] = []

    def _populate_machine_slots(self) -> None:
        for machine in range(1, self.cfg.pre_hatch_machines + 1):
            for slot in range(1, self.cfg.pre_hatch_carts_per_machine + 1):
                slot_id = f"setter-{machine:02d}-cart-{slot:02d}"
                self.pre_hatch_slots.items.append(slot_id)
        for machine in range(1, self.cfg.hatch_machines + 1):
            for slot in range(1, self.cfg.hatch_carts_per_machine + 1):
                slot_id = f"hatcher-{machine:02d}-cart-{slot:02d}"
                self.hatch_slots.items.append(slot_id)

    def _initialise_barn_places(self, specs: List[FarmSpec]) -> List[BarnPlace]:
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
        self.env.process(self._generate_shipments())
        self.env.run(until=self.cfg.simulation_days)

    def _generate_shipments(self):  # type: ignore[override]
        mean_shipments = self.capacity_plan.shipments_per_day
        while True:
            shipments_today = max(1, np.random.poisson(mean_shipments))
            interval = 1.0 / shipments_today
            for _ in range(shipments_today):
                shipment_id = f"shipment-{next(self.shipment_counter)}"
                source_id = next(self.egg_sources)
                self.env.process(self._handle_shipment(shipment_id, source_id))
                yield self.env.timeout(interval)
            yield self.env.timeout(max(0.0, 1.0 - shipments_today * interval))

    def _handle_shipment(self, shipment_id: str, source_id: int):  # type: ignore[override]
        eggs = self.cfg.shipment_eggs
        farm_cars = math.ceil(eggs / self.cfg.farm_car_eggs)
        self.logger.log(
            self.env.now,
            shipment_id,
            stage="inventory",
            status="arrived",
            quantity=eggs,
            metadata={"source": f"egg-farm-{source_id}", "farm_cars": farm_cars},
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

        self.logger.log(
            self.env.now,
            shipment_id,
            stage="pre_hatch",
            status="loaded",
            quantity=eggs,
            metadata={"cart_count": len(cart_results)},
        )
        self.logger.log(
            self.env.now,
            shipment_id,
            stage="xray_screen",
            status="discarded",
            quantity=total_discarded,
            metadata={"cart_count": len(cart_results)},
        )
        self.logger.log(
            self.env.now,
            shipment_id,
            stage="hatch_room",
            status="loaded",
            quantity=total_fertile,
            metadata={"cart_count": len(cart_results)},
        )
        self.logger.log(
            self.env.now,
            shipment_id,
            stage="hatch_room",
            status="hatched",
            quantity=total_chicks,
            metadata={"losses": total_hatch_losses},
        )

        if total_chicks <= 0:
            self.logger.log(
                self.env.now,
                shipment_id,
                stage="hatch_room",
                status="empty_shipment",
                quantity=0,
            )
            return

        yield self.env.timeout(self.cfg.sort_and_vaccinate_hours / 24)
        self.logger.log(
            self.env.now,
            shipment_id,
            stage="processing",
            status="sorted",
            quantity=total_chicks,
        )

        yield self.env.timeout(self.cfg.load_to_transport_hours / 24)
        trucks_needed = max(1, math.ceil(total_chicks / self.cfg.chicks_per_truck))
        self.logger.log(
            self.env.now,
            shipment_id,
            stage="transport_loading",
            status="scheduled",
            quantity=total_chicks,
            metadata={"trucks": trucks_needed},
        )

        remaining_chicks = total_chicks
        truck_counter = 0
        while remaining_chicks > 0:
            truck_counter += 1
            load = min(self.cfg.chicks_per_truck, remaining_chicks)
            remaining_chicks -= load
            yield self.truck_slots.get(1)
            truck_id = f"{shipment_id}-truck-{truck_counter:02d}"
            self.logger.log(
                self.env.now,
                shipment_id,
                stage="transport_loading",
                status="departed",
                quantity=load,
                metadata={"truck_id": truck_id},
            )
            yield self.env.timeout(self.cfg.transport_time_days)
            self.logger.log(
                self.env.now,
                shipment_id,
                stage="transport",
                status="arrived",
                quantity=load,
                metadata={"truck_id": truck_id},
            )
            yield from self._place_truck(shipment_id, truck_id, load)
            self.truck_slots.put(1)

    def _process_cart(
        self, shipment_id: str, cart_id: str, eggs: int
    ) -> simpy.events.Event:
        slot_id = yield self.pre_hatch_slots.get()
        self.logger.log(
            self.env.now,
            cart_id,
            stage="pre_hatch_cart",
            status="loaded",
            quantity=eggs,
            metadata={"shipment_id": shipment_id, "machine_slot": slot_id},
        )
        yield self.env.timeout(self.cfg.pre_hatch_days)
        yield self.pre_hatch_slots.put(slot_id)
        self.logger.log(
            self.env.now,
            cart_id,
            stage="pre_hatch_cart",
            status="released",
            quantity=eggs,
            metadata={"shipment_id": shipment_id, "machine_slot": slot_id},
        )

        pass_rate = self.rng.betavariate(self.cfg.xray_alpha, self.cfg.xray_beta)
        fertile = int(round(eggs * pass_rate))
        discarded = eggs - fertile

        hatch_slot_id = yield self.hatch_slots.get()
        self.logger.log(
            self.env.now,
            cart_id,
            stage="hatch_cart",
            status="loaded",
            quantity=fertile,
            metadata={"shipment_id": shipment_id, "machine_slot": hatch_slot_id},
        )
        hatch_duration = self.rng.uniform(*self.cfg.hatch_days_range)
        yield self.env.timeout(hatch_duration)

        hatch_rate = self.rng.betavariate(self.cfg.hatch_alpha, self.cfg.hatch_beta)
        chicks = int(round(fertile * hatch_rate))
        hatch_losses = fertile - chicks

        yield self.hatch_slots.put(hatch_slot_id)
        self.logger.log(
            self.env.now,
            cart_id,
            stage="hatch_cart",
            status="hatched",
            quantity=chicks,
            metadata={"shipment_id": shipment_id, "machine_slot": hatch_slot_id, "losses": hatch_losses},
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
        remaining = load
        while remaining > 0:
            place: Optional[BarnPlace] = None
            wait_time: Optional[float] = None
            while place is None:
                with self.barn_lock.request() as req:
                    yield req
                    place = self._find_next_place()
                    if place is None:
                        next_clean = min(
                            (
                                p.cleaning_until
                                for p in self.barn_places
                                if p.cleaning_until > self.env.now
                            ),
                            default=None,
                        )
                        wait_time = (
                            (next_clean - self.env.now) if next_clean is not None else 1.0
                        )
                if place is None:
                    yield self.env.timeout(max(0.1, wait_time or 1.0))

            with self.barn_lock.request() as req:
                yield req
                # re-fetch to ensure capacity is still available after acquiring the lock
                confirmed_place = self._find_specific_place(place.place_id)
                if confirmed_place is None or confirmed_place.remaining_capacity <= 0:
                    place = None
                    continue
                available = confirmed_place.remaining_capacity
                portion = min(available, remaining)
                confirmed_place.add(shipment_id, portion)
                remaining -= portion
                self.logger.log(
                    self.env.now,
                    shipment_id,
                    stage="farm_intake",
                    status="placed",
                    quantity=portion,
                    metadata={
                        "truck_id": truck_id,
                        "place_id": confirmed_place.place_id,
                        "farm": confirmed_place.farm_name,
                        "remaining_capacity": confirmed_place.remaining_capacity,
                        "mix": confirmed_place.occupants.copy(),
                    },
                )
                self.env.process(
                    self._manage_farm_cycle(
                        shipment_id=shipment_id,
                        amount=portion,
                        place=confirmed_place,
                    )
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
        self.logger.log(
            self.env.now,
            shipment_id,
            stage="grow_out",
            status="completed",
            quantity=survivors,
            metadata={"losses": losses, "place_id": place.place_id, "farm": place.farm_name},
        )
        self.logger.log(
            self.env.now,
            shipment_id,
            stage="slaughter",
            status="shipped",
            quantity=survivors,
            metadata={"place_id": place.place_id, "farm": place.farm_name},
        )
        self.slaughter_records.append(
            SlaughterRecord(
                day=self.env.now,
                shipment_id=shipment_id,
                quantity=survivors,
                farm=place.farm_name,
            )
        )

        removed = place.decrement(shipment_id, amount)
        if place.occupied == 0:
            place.cleaning_until = self.env.now + self.cfg.cleaning_days
            self.logger.log(
                self.env.now,
                shipment_id,
                stage="farm_place",
                status="available",
                quantity=removed,
                metadata={"place_id": place.place_id, "farm": place.farm_name},
            )
            yield self.env.timeout(self.cfg.cleaning_days)
            place.cleaning_until = 0.0

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
