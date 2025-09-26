from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class SimulationConfig:
    """Immutable parameter set that drives the hatchery and farm simulation."""

    target_slaughter_per_day: int = 300_000
    pre_hatch_days: int = 24
    hatch_days_range: Tuple[int, int] = (3, 5)
    sort_and_vaccinate_hours: float = 6.0
    load_to_transport_hours: float = 6.0
    transport_time_days: float = 1.0
    grow_out_days: int = 56
    cleaning_days: int = 7

    boxes_per_car: int = 10
    eggs_per_box: int = 40
    cars_per_pre_hatch_room: int = 18
    rooms_per_site: int = 60
    site_buffer: int = 3

    num_egg_sources: int = 15
    num_farms: int = 60
    places_per_farm: int = 12
    chicks_per_place: int = 27_000
    qs_ratio: float = 0.30
    overproduction_factor: float = 1.05

    num_trucks: int = 80
    car_capacity_per_truck: int = 16

    xray_alpha: float = 85.0
    xray_beta: float = 15.0
    hatch_alpha: float = 95.0
    hatch_beta: float = 5.0
    farm_alpha: float = 92.0
    farm_beta: float = 8.0

    simulation_days: int = 220
    warmup_days: int = 120

    db_path: str = "hatchery_events.sqlite"

    @property
    def eggs_per_car(self) -> int:
        """Return the number of eggs that fit in a single transport car."""

        return self.boxes_per_car * self.eggs_per_box


@dataclass(frozen=True)
class CapacityPlan:
    """Derived sizing information for facilities and logistics given a configuration."""

    cars_per_day: float
    pre_hatch_slots_needed: float
    pre_hatch_rooms_needed: float
    hatch_slots_needed: float
    hatch_rooms_needed: float
    sites_needed_for_pre_hatch: int
    sites_used: int
    total_pre_hatch_slots: int
    total_hatch_slots: int
    chicks_per_day_to_farms: float
    eggs_per_day_required: float


def derive_capacity(cfg: SimulationConfig) -> CapacityPlan:
    """Calculate steady-state capacity requirements for the supplied configuration."""
    farm_survival_mean = cfg.farm_alpha / (cfg.farm_alpha + cfg.farm_beta)
    hatch_success_mean = cfg.hatch_alpha / (cfg.hatch_alpha + cfg.hatch_beta)
    xray_pass_mean = cfg.xray_alpha / (cfg.xray_alpha + cfg.xray_beta)

    chicks_to_farms = cfg.target_slaughter_per_day / farm_survival_mean
    eggs_after_hatch = chicks_to_farms / hatch_success_mean
    eggs_needed = eggs_after_hatch / xray_pass_mean
    eggs_needed *= cfg.overproduction_factor

    cars_per_day = eggs_needed / cfg.eggs_per_car
    pre_hatch_slots_needed = cars_per_day * cfg.pre_hatch_days
    pre_hatch_rooms_needed = pre_hatch_slots_needed / cfg.cars_per_pre_hatch_room

    hatch_days_mean = sum(cfg.hatch_days_range) / 2
    hatch_slots_needed = cars_per_day * hatch_days_mean
    hatch_rooms_needed = hatch_slots_needed / cfg.cars_per_pre_hatch_room

    sites_needed = int(-(-pre_hatch_rooms_needed // cfg.rooms_per_site))  # ceiling division
    sites_used = sites_needed + max(cfg.site_buffer, 0)
    total_pre_hatch_slots = sites_used * cfg.rooms_per_site * cfg.cars_per_pre_hatch_room
    total_hatch_slots = sites_used * cfg.rooms_per_site * cfg.cars_per_pre_hatch_room

    return CapacityPlan(
        cars_per_day=cars_per_day,
        pre_hatch_slots_needed=pre_hatch_slots_needed,
        pre_hatch_rooms_needed=pre_hatch_rooms_needed,
        hatch_slots_needed=hatch_slots_needed,
        hatch_rooms_needed=hatch_rooms_needed,
        sites_needed_for_pre_hatch=sites_needed,
        sites_used=sites_used,
        total_pre_hatch_slots=total_pre_hatch_slots,
        total_hatch_slots=total_hatch_slots,
        chicks_per_day_to_farms=chicks_to_farms,
        eggs_per_day_required=eggs_needed,
    )
