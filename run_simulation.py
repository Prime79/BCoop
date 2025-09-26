from pathlib import Path

from simulation.config import SimulationConfig, derive_capacity
from simulation.logger import EventLogger
from simulation.model import ChickSimulation


def main() -> None:
    """Execute a single simulation run and print capacity plus throughput metrics."""
    cfg = SimulationConfig()
    plan = derive_capacity(cfg)

    db_path = Path(cfg.db_path)
    if db_path.exists():
        db_path.unlink()

    logger = EventLogger(cfg.db_path)
    simulation = ChickSimulation(cfg, logger)
    simulation.run()
    summary = simulation.summarize()
    logger.close()

    print("Capacity plan:")
    print(f"  Eggs per day required: {plan.eggs_per_day_required:,.0f}")
    print(f"  Cars per day: {plan.cars_per_day:,.1f}")
    print(f"  Pre-hatch rooms needed: {plan.pre_hatch_rooms_needed:,.1f}")
    print(f"  Sites used (60 rooms each): {plan.sites_used}")
    print(f"  Total pre-hatch slots available: {plan.total_pre_hatch_slots:,}")

    print("\nSimulation summary:")
    avg_slaughter = summary["avg_slaughter_per_day"]
    if avg_slaughter is None:
        print("  Not enough data beyond warmup period to compute average.")
    else:
        print(f"  Average slaughter-ready chickens per day: {avg_slaughter:,.0f}")
    print(f"  Total slaughtered over run: {summary['total_slaughtered']:,}")
    print(f"  Events stored in SQLite at: {db_path.resolve()}")


if __name__ == "__main__":
    main()
