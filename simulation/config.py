from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Sequence, Tuple


@dataclass(frozen=True)
class FarmSpec:
    """Static definition of a grow-out farm and its chick capacity."""

    name: str
    capacity_chicks: int
    barns: int = 5

    @property
    def capacity_per_barn(self) -> Tuple[int, int]:
        base = self.capacity_chicks // self.barns
        remainder = self.capacity_chicks % self.barns
        return base, remainder


@dataclass(frozen=True)
class SimulationConfig:
    """Immutable parameter set that drives the hatchery and farm simulation."""

    target_slaughter_per_day: int = 300_000
    overproduction_factor: float = 1.05

    shipment_eggs: int = 100_000
    farm_car_eggs: int = 3_520
    pre_hatch_cart_eggs: int = 7_040

    pre_hatch_machines: int = 56
    pre_hatch_carts_per_machine: int = 18
    hatch_machines: int = 56
    hatch_carts_per_machine: int = 18

    pre_hatch_days: int = 24
    hatch_days_range: Tuple[int, int] = (3, 5)
    sort_and_vaccinate_hours: float = 6.0
    load_to_transport_hours: float = 6.0
    transport_time_days: float = 1.0
    grow_out_days_range: Tuple[int, int] = (35, 42)
    cleaning_days: int = 12

    chicks_per_truck: int = 57_600
    active_trucks: int = 120

    xray_alpha: float = 85.0
    xray_beta: float = 15.0
    hatch_alpha: float = 95.0
    hatch_beta: float = 5.0
    farm_alpha: float = 92.0
    farm_beta: float = 8.0

    simulation_days: int = 365
    warmup_days: int = 120

    db_path: str = "hatchery_events.sqlite"
    start_date: datetime = datetime(2025, 1, 1)
    flow_log_path: Path = Path("flow_log.parquet")

    @property
    def farm_specs(self) -> Sequence[FarmSpec]:
        """Return the configured farm capacities (net placement, converted to chicks)."""

        return _FARM_SPECS

    @property
    def total_pre_hatch_slots(self) -> int:
        return self.pre_hatch_machines * self.pre_hatch_carts_per_machine

    @property
    def total_hatch_slots(self) -> int:
        return self.hatch_machines * self.hatch_carts_per_machine


@dataclass(frozen=True)
class CapacityPlan:
    """Derived sizing information for facilities and logistics given a configuration."""

    shipments_per_day: float
    eggs_per_day_required: float
    pre_hatch_carts_per_day: float
    pre_hatch_slots_needed: float
    hatch_carts_per_day: float
    hatch_slots_needed: float
    chicks_per_day_to_farms: float


def derive_capacity(cfg: SimulationConfig) -> CapacityPlan:
    """Calculate steady-state capacity requirements for the supplied configuration."""

    farm_survival_mean = cfg.farm_alpha / (cfg.farm_alpha + cfg.farm_beta)
    hatch_success_mean = cfg.hatch_alpha / (cfg.hatch_alpha + cfg.hatch_beta)
    xray_pass_mean = cfg.xray_alpha / (cfg.xray_alpha + cfg.xray_beta)

    chicks_to_farms = cfg.target_slaughter_per_day / farm_survival_mean
    eggs_after_hatch = chicks_to_farms / hatch_success_mean
    eggs_needed = eggs_after_hatch / xray_pass_mean
    eggs_needed *= cfg.overproduction_factor

    pre_hatch_carts_per_day = eggs_needed / cfg.pre_hatch_cart_eggs
    hatch_carts_per_day = pre_hatch_carts_per_day  # carts carry through the process

    pre_hatch_slots_needed = pre_hatch_carts_per_day * cfg.pre_hatch_days
    hatch_days_mean = sum(cfg.hatch_days_range) / 2
    hatch_slots_needed = hatch_carts_per_day * hatch_days_mean

    shipments_per_day = eggs_needed / cfg.shipment_eggs

    return CapacityPlan(
        shipments_per_day=shipments_per_day,
        eggs_per_day_required=eggs_needed,
        pre_hatch_carts_per_day=pre_hatch_carts_per_day,
        pre_hatch_slots_needed=pre_hatch_slots_needed,
        hatch_carts_per_day=hatch_carts_per_day,
        hatch_slots_needed=hatch_slots_needed,
        chicks_per_day_to_farms=chicks_to_farms,
    )


_FARM_SPECS: List[FarmSpec] = [
    FarmSpec("Kisvarsany", int(142_420)),
    FarmSpec("Gavavencsello", int(202_910)),
    FarmSpec("Foldes", int(279_000)),
    FarmSpec("Kaba", int(444_000)),
    FarmSpec("Nyiribrony", int(82_700)),
    FarmSpec("Aranyosapati", int(312_000)),
    FarmSpec("Blhaza_VI", int(312_000)),
    FarmSpec("Blhaza_III", int(260_000)),
    FarmSpec("Blhaza_II", int(260_000)),
    FarmSpec("Blhaza_I", int(312_000)),
    FarmSpec("Blhaza_V", int(312_000)),
    FarmSpec("IstvanMajor_B_IV", int(260_000)),
    FarmSpec("Levelek_I", int(312_000)),
    FarmSpec("Levelek_II", int(312_000)),
    FarmSpec("Nyirmada", int(260_000)),
    FarmSpec("Hodmezovasarhely_Nanas", int(207_000)),
    FarmSpec("Tiszakarad", int(182_000)),
    FarmSpec("Sarospatak", int(199_000)),
    FarmSpec("Pusztadobos", int(145_500)),
    FarmSpec("Laskod", int(312_000)),
    FarmSpec("Ibrany", int(260_000)),
    FarmSpec("Petnehaza_I", int(260_000)),
    FarmSpec("Petnehaza_II", int(260_000)),
    FarmSpec("Fabianhaza", int(260_000)),
    FarmSpec("Nyirkarasz_I", int(312_000)),
    FarmSpec("Nyirkercs_I", int(312_000)),
    FarmSpec("Cigand_I", int(260_000)),
    FarmSpec("Nyirkercs_II", int(312_000)),
    FarmSpec("Vojka_Farm_Veke", int(220_000)),
    FarmSpec("Nyirkercs_III", int(312_000)),
    FarmSpec("Eperjeske", int(312_000)),
    FarmSpec("Nagyhalasz", int(312_000)),
    FarmSpec("Nagyecsed", int(286_000)),
    FarmSpec("Beregdaroc", int(312_000)),
    FarmSpec("Cigand_II", int(312_000)),
    FarmSpec("Kantorjanosi", int(312_000)),
]
