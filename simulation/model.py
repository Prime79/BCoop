from __future__ import annotations

import itertools
import random
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Generator, Optional

import numpy as np
import simpy

from .config import CapacityPlan, SimulationConfig, derive_capacity
from .logger import EventLogger


@dataclass
class EggBatch:
    """Represents a single car-load of eggs moving through the pipeline."""

    batch_id: str
    source: str
    eggs: int
    target_segment: str  # "qs" or "standard"
    pre_hatch_slot_id: Optional[str] = None
    hatch_slot_id: Optional[str] = None


@dataclass
class BarnPlace:
    """Represents a barn place that can mix multiple batches."""

    place_id: str
    capacity: int
    segment: str
    occupants: dict[str, dict[str, int]] = field(default_factory=dict)
    occupied: int = 0
    cleaning_until: float = 0.0

    def can_accept(self, amount: int, now: float) -> bool:
        return now >= self.cleaning_until and (self.capacity - self.occupied) >= amount

    def add_batch(
        self,
        batch_id: str,
        chicks: int,
        pre_hatch_slot_id: Optional[str] = None,
        hatch_slot_id: Optional[str] = None,
    ) -> None:
        entry = self.occupants.setdefault(
            batch_id,
            {
                "chicks": 0,
                "pre_hatch_slot_id": pre_hatch_slot_id,
                "hatch_slot_id": hatch_slot_id,
            },
        )
        entry["chicks"] += chicks
        if pre_hatch_slot_id is not None:
            entry["pre_hatch_slot_id"] = pre_hatch_slot_id
        if hatch_slot_id is not None:
            entry["hatch_slot_id"] = hatch_slot_id
        self.occupied += chicks

    def remove_batch(self, batch_id: str) -> dict[str, int | str] | None:
        entry = self.occupants.get(batch_id)
        if not entry:
            return None
        chicks = int(entry.get("chicks", 0))
        self.occupied = max(0, self.occupied - chicks)
        self.occupants.pop(batch_id, None)
        entry["chicks"] = chicks
        return entry

    @property
    def remaining_capacity(self) -> int:
        return self.capacity - self.occupied


@dataclass
class SlaughterRecord:
    """Capture slaughter-ready output for downstream summaries."""

    day: float
    batch_id: str
    quantity: int
    segment: str


class ChickSimulation:
    """Coordinate all SimPy processes for the hatchery-to-slaughter workflow."""

    def __init__(
        self,
        cfg: SimulationConfig,
        logger: EventLogger,
        rng: Optional[random.Random] = None,
        poisson_rng: Optional[np.random.Generator] = None,
    ) -> None:
        """Prepare the simulation environment, capacities, and reusable state."""

        self.cfg = cfg
        self.logger = logger
        self.rng = rng or random.Random()
        self.poisson_rng = poisson_rng or np.random.default_rng()
        self.env = simpy.Environment()
        self.capacity_plan: CapacityPlan = derive_capacity(cfg)

        self.pre_hatch_slots = simpy.Store(
            self.env, capacity=self.capacity_plan.total_pre_hatch_slots
        )
        self.hatch_slots = simpy.Store(
            self.env, capacity=self.capacity_plan.total_hatch_slots
        )
        self.pre_hatch_slot_map: dict[str, dict[str, int]] = {}
        self.hatch_slot_map: dict[str, dict[str, int]] = {}

        self._initialise_room_slots()
        self.truck_slots = simpy.Container(
            self.env,
            capacity=cfg.num_trucks * cfg.car_capacity_per_truck,
            init=cfg.num_trucks * cfg.car_capacity_per_truck,
        )

        qs_places, standard_places = self._initialize_barn_places()
        self.barn_places: dict[str, list[BarnPlace]] = {
            "qs": qs_places,
            "standard": standard_places,
        }
        self.barn_allocation_lock = simpy.Resource(self.env, capacity=1)

        self.batch_counter = itertools.count()
        self.egg_sources = itertools.cycle(range(1, cfg.num_egg_sources + 1))

        self.slaughter_records: list[SlaughterRecord] = []

    def run(self) -> None:
        """Kick off the event generation process and advance the simulation clock."""
        self.env.process(self._generate_egg_batches())
        self.env.run(until=self.cfg.simulation_days)

    def _initialise_room_slots(self) -> None:
        """Populate pre-hatch and hatch slot stores with room/slot identifiers."""

        total_sites = self.capacity_plan.sites_used
        for site in range(total_sites):
            for room in range(self.cfg.rooms_per_site):
                for slot in range(self.cfg.cars_per_pre_hatch_room):
                    slot_id = (
                        f"pre_site-{site+1:02d}_room-{room+1:03d}_slot-{slot+1:02d}"
                    )
                    self.pre_hatch_slot_map[slot_id] = {
                        "site": site + 1,
                        "room": room + 1,
                        "slot": slot + 1,
                    }
                    self.pre_hatch_slots.items.append(slot_id)

                    hatch_slot_id = (
                        f"hatch_site-{site+1:02d}_room-{room+1:03d}_slot-{slot+1:02d}"
                    )
                    self.hatch_slot_map[hatch_slot_id] = {
                        "site": site + 1,
                        "room": room + 1,
                        "slot": slot + 1,
                    }
                    self.hatch_slots.items.append(hatch_slot_id)

    def _initialize_barn_places(self) -> tuple[list[BarnPlace], list[BarnPlace]]:
        """Create barn places for QS and standard segments."""

        total_places = self.cfg.num_farms * self.cfg.places_per_farm
        qs_place_count = int(round(total_places * self.cfg.qs_ratio))
        qs_places: list[BarnPlace] = []
        standard_places: list[BarnPlace] = []

        for index in range(total_places):
            farm_idx = index // self.cfg.places_per_farm + 1
            place_idx = index % self.cfg.places_per_farm + 1
            place_id = f"farm-{farm_idx:02d}-place-{place_idx:02d}"
            segment = "qs" if index < qs_place_count else "standard"
            place = BarnPlace(
                place_id=place_id,
                capacity=self.cfg.chicks_per_place,
                segment=segment,
            )
            if segment == "qs":
                qs_places.append(place)
            else:
                standard_places.append(place)

        return qs_places, standard_places

    @staticmethod
    def _hours_to_days(hours: float) -> float:
        """Convert an hour duration to the equivalent days in the simulation clock."""

        return hours / 24.0

    @staticmethod
    def _arrival_interval(cars_today: int) -> float:
        """Spread arrivals throughout a day based on the number of incoming cars."""

        return 1.0 / max(cars_today, 1)

    def _generate_egg_batches(self) -> Generator[simpy.events.Event, None, None]:
        """Yield car-level batches throughout each simulated day."""

        mean_cars = self.capacity_plan.cars_per_day
        while True:
            cars_today = int(self.poisson_rng.poisson(mean_cars))
            if cars_today <= 0:
                cars_today = 1
            interval = self._arrival_interval(cars_today)
            for _ in range(cars_today):
                batch = self._build_batch()
                self.env.process(self._handle_batch(batch))
                yield self.env.timeout(interval)
            yield self.env.timeout(max(0.0, 1.0 - cars_today * interval))

    def _build_batch(self) -> EggBatch:
        """Construct a new batch descriptor with origin and target segment metadata."""
        batch_id = f"batch-{next(self.batch_counter)}"
        farm_id = next(self.egg_sources)
        segment = "qs" if self.rng.random() < self.cfg.qs_ratio else "standard"
        return EggBatch(
            batch_id=batch_id,
            source=f"egg-farm-{farm_id}",
            eggs=self.cfg.eggs_per_car,
            target_segment=segment,
        )

    def _handle_batch(self, batch: EggBatch) -> Generator[simpy.events.Event, None, None]:
        """Progress a batch through each stage, logging outcomes and scheduling follow-ups."""

        if batch.eggs <= 0:
            self.logger.log(
                self.env.now,
                batch.batch_id,
                stage="validation",
                status="invalid_batch",
                quantity=batch.eggs,
            )
            return

        self._log_inventory_arrival(batch)
        yield from self._process_pre_hatch(batch)

        fertile_eggs = self._screen_eggs(batch)
        if fertile_eggs <= 0:
            return

        chicks = yield from self._process_hatching(batch, fertile_eggs)
        if chicks <= 0:
            return

        yield from self._process_post_hatch_flow(batch, chicks)

    def _log_inventory_arrival(self, batch: EggBatch) -> None:
        """Record the arrival of a batch into inventory."""

        self.logger.log(
            self.env.now,
            batch.batch_id,
            stage="inventory",
            status="arrived",
            quantity=batch.eggs,
            metadata={"source": batch.source},
        )

    def _process_pre_hatch(self, batch: EggBatch) -> Generator[simpy.events.Event, None, None]:
        """Load a batch into pre-hatch capacity and wait for incubation."""

        slot_id = yield self.pre_hatch_slots.get()
        slot_info = self.pre_hatch_slot_map[slot_id]
        batch.pre_hatch_slot_id = slot_id
        self.logger.log(
            self.env.now,
            batch.batch_id,
            stage="pre_hatch",
            status="loaded",
            quantity=batch.eggs,
            metadata={
                "slot_id": slot_id,
                "site": slot_info["site"],
                "room": slot_info["room"],
                "slot": slot_info["slot"],
            },
        )
        yield self.env.timeout(self.cfg.pre_hatch_days)
        yield self.pre_hatch_slots.put(slot_id)

    def _screen_eggs(self, batch: EggBatch) -> int:
        """Apply X-ray screening and vaccination losses."""

        pass_rate = self.rng.betavariate(self.cfg.xray_alpha, self.cfg.xray_beta)
        fertile_eggs = int(batch.eggs * pass_rate)
        discarded = batch.eggs - fertile_eggs
        self.logger.log(
            self.env.now,
            batch.batch_id,
            stage="xray_screen",
            status="discarded",
            quantity=discarded,
            metadata={"pass_rate": pass_rate},
        )
        if fertile_eggs <= 0:
            self.logger.log(
                self.env.now,
                batch.batch_id,
                stage="xray_screen",
                status="failed",
                quantity=0,
            )
        return fertile_eggs

    def _process_hatching(
        self,
        batch: EggBatch,
        fertile_eggs: int,
    ) -> Generator[simpy.events.Event, None, int]:
        """Move screened eggs through hatching and capture chick output."""

        hatch_slot_id = yield self.hatch_slots.get()
        hatch_slot_info = self.hatch_slot_map[hatch_slot_id]
        batch.hatch_slot_id = hatch_slot_id
        hatch_duration = self.rng.uniform(*self.cfg.hatch_days_range)
        self.logger.log(
            self.env.now,
            batch.batch_id,
            stage="hatch_room",
            status="loaded",
            quantity=fertile_eggs,
            metadata={
                "duration_days": hatch_duration,
                "slot_id": hatch_slot_id,
                "site": hatch_slot_info["site"],
                "room": hatch_slot_info["room"],
                "slot": hatch_slot_info["slot"],
            },
        )
        yield self.env.timeout(hatch_duration)
        yield self.hatch_slots.put(hatch_slot_id)

        hatch_rate = self.rng.betavariate(self.cfg.hatch_alpha, self.cfg.hatch_beta)
        chicks = int(fertile_eggs * hatch_rate)
        losses = fertile_eggs - chicks
        self.logger.log(
            self.env.now,
            batch.batch_id,
            stage="hatch_room",
            status="hatched",
            quantity=chicks,
            metadata={
                "hatch_rate": hatch_rate,
                "losses": losses,
                "slot_id": hatch_slot_id,
                "site": hatch_slot_info["site"],
                "room": hatch_slot_info["room"],
                "slot": hatch_slot_info["slot"],
            },
        )
        if chicks <= 0:
            self.logger.log(
                self.env.now,
                batch.batch_id,
                stage="hatch_room",
                status="empty_batch",
                quantity=0,
            )
        return chicks

    def _process_post_hatch_flow(
        self,
        batch: EggBatch,
        chicks: int,
    ) -> Generator[simpy.events.Event, None, None]:
        """Handle processing, transport, and farm placement for viable chicks."""

        yield from self._process_processing(batch, chicks)
        yield from self._transport_to_farm(batch, chicks)
        yield from self._place_on_farm(batch, chicks)

    def _process_processing(
        self,
        batch: EggBatch,
        chicks: int,
    ) -> Generator[simpy.events.Event, None, None]:
        """Model chick sorting, measuring, and vaccination."""

        yield self.env.timeout(self._hours_to_days(self.cfg.sort_and_vaccinate_hours))
        self.logger.log(
            self.env.now,
            batch.batch_id,
            stage="processing",
            status="sorted",
            quantity=chicks,
        )
        yield self.env.timeout(self._hours_to_days(self.cfg.load_to_transport_hours))

    def _transport_to_farm(
        self,
        batch: EggBatch,
        chicks: int,
    ) -> Generator[simpy.events.Event, None, None]:
        """Send processed chicks to farms via shared transport capacity."""

        yield self.truck_slots.get(1)
        self.logger.log(
            self.env.now,
            batch.batch_id,
            stage="transport_loading",
            status="departed",
            quantity=chicks,
            metadata={"segment": batch.target_segment},
        )
        yield self.env.timeout(self.cfg.transport_time_days)
        self.truck_slots.put(1)
        self.logger.log(
            self.env.now,
            batch.batch_id,
            stage="transport",
            status="arrived",
            quantity=chicks,
        )

    def _place_on_farm(
        self,
        batch: EggBatch,
        chicks: int,
    ) -> Generator[simpy.events.Event, None, None]:
        """Reserve farm capacity for the batch and schedule the grow-out cycle."""

        place = yield from self._acquire_barn_place(batch, chicks)
        self.logger.log(
            self.env.now,
            batch.batch_id,
            stage="farm_intake",
            status="placed",
            quantity=chicks,
            metadata={
                "segment": batch.target_segment,
                "place_id": place.place_id,
                "remaining_capacity": place.remaining_capacity,
                "occupants": {
                    occupant_id: info["chicks"]
                    for occupant_id, info in place.occupants.items()
                },
            },
        )
        self.env.process(self._manage_farm_cycle(batch, chicks, place))

    def _acquire_barn_place(
        self,
        batch: EggBatch,
        chicks: int,
    ) -> Generator[simpy.events.Event, None, BarnPlace]:
        """Find a barn place with enough free capacity, retrying until available."""

        segment = batch.target_segment
        while True:
            with self.barn_allocation_lock.request() as req:
                yield req
                now = self.env.now
                candidates = self.barn_places[segment]
                for place in candidates:
                    if place.can_accept(chicks, now):
                        place.add_batch(
                            batch.batch_id,
                            chicks,
                            pre_hatch_slot_id=batch.pre_hatch_slot_id,
                            hatch_slot_id=batch.hatch_slot_id,
                        )
                        return place
            yield self.env.timeout(0.5)

    def _manage_farm_cycle(
        self,
        batch: EggBatch,
        chicks: int,
        place: BarnPlace,
    ):
        """Simulate the grow-out, slaughter, and cleaning cycle for a farm placement."""
        # Stage 8 continued: grow-out period
        yield self.env.timeout(self.cfg.grow_out_days)
        survival_rate = self.rng.betavariate(self.cfg.farm_alpha, self.cfg.farm_beta)
        survivors = int(chicks * survival_rate)
        losses = chicks - survivors
        self.logger.log(
            self.env.now,
            batch.batch_id,
            stage="grow_out",
            status="completed",
            quantity=survivors,
            metadata={
                "losses": losses,
                "survival_rate": survival_rate,
                "place_id": place.place_id,
            },
        )

        # Stage 9: send to slaughter
        self.logger.log(
            self.env.now,
            batch.batch_id,
            stage="slaughter",
            status="shipped",
            quantity=survivors,
            metadata={"place_id": place.place_id},
        )
        self.slaughter_records.append(
            SlaughterRecord(
                day=self.env.now,
                batch_id=batch.batch_id,
                quantity=survivors,
                segment=batch.target_segment,
            )
        )

        # Stage 10: cleaning window keeps capacity reserved
        occupant_details = place.remove_batch(batch.batch_id)
        if place.occupied == 0:
            place.cleaning_until = self.env.now + self.cfg.cleaning_days
            yield self.env.timeout(self.cfg.cleaning_days)
            place.cleaning_until = 0.0
            self.logger.log(
                self.env.now,
                batch.batch_id,
                stage="farm_place",
                status="available",
                quantity=chicks,
                metadata={"place_id": place.place_id},
            )
        else:
            self.logger.log(
                self.env.now,
                batch.batch_id,
                stage="farm_place",
                status="partial_release",
                quantity=chicks,
                metadata={
                    "place_id": place.place_id,
                    "remaining_capacity": place.remaining_capacity,
                    "occupants": {
                        occupant_id: info["chicks"]
                        for occupant_id, info in place.occupants.items()
                    },
                },
            )

    @property
    def slaughtered_after_warmup(self) -> list[int]:
        """Return slaughter-ready counts once the warmup window has elapsed."""
        cutoff = self.cfg.warmup_days
        return [
            record.quantity
            for record in self.slaughter_records
            if record.day >= cutoff
        ]

    def _harvest_slaughter_curve(self) -> list[tuple[float, int]]:
        """Provide the chronological slaughter output captured in-memory."""
        return [(record.day, record.quantity) for record in self.slaughter_records]

    def summarize(self) -> dict[str, Optional[float]]:
        """Aggregate harvested volume to compute a post-warmup daily average."""
        per_day = defaultdict(float)
        last_complete_day = int(self.cfg.simulation_days - self.cfg.cleaning_days)
        for record in self.slaughter_records:
            if record.day < self.cfg.warmup_days:
                continue
            per_day[int(record.day)] += record.quantity

        if not per_day:
            total = sum(r.quantity for r in self.slaughter_records)
            return {"avg_slaughter_per_day": None, "total_slaughtered": total}

        valid_days = [day for day in per_day if day <= last_complete_day]
        if not valid_days:
            valid_days = list(per_day.keys())

        total_output = sum(per_day[day] for day in valid_days)
        avg_per_day = total_output / len(valid_days)
        total = sum(r.quantity for r in self.slaughter_records)
        return {"avg_slaughter_per_day": avg_per_day, "total_slaughtered": total}
