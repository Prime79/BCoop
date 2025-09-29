"""Full simulation run entry point.

Writes a fresh SQLite events DB (optional) and a Parquet flow log consumed by
the graphing tools. Use `quick_run.py` for a short test run.
"""

import argparse
from pathlib import Path

from simulation.config import SimulationConfig, derive_capacity
from simulation.logger import EventLogger, NoOpEventLogger
from simulation.model import ChickSimulation


def _build_logger(cfg: SimulationConfig, audit: bool):
    return EventLogger(cfg.db_path) if audit else NoOpEventLogger()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full hatchery simulation")
    parser.add_argument(
        "--mode",
        choices=("exec", "debug"),
        default="exec",
        help="Use 'debug' to enable audit logging to SQLite; default is 'exec' with logging disabled.",
    )
    return parser.parse_args()


def main() -> None:
    """Execute a simulation run and print capacity/throughput metrics."""
    args = _parse_args()
    cfg = SimulationConfig()
    plan = derive_capacity(cfg)

    db_path = Path(cfg.db_path)
    if db_path.exists():
        db_path.unlink()
    flow_path = Path(cfg.flow_log_path)
    if flow_path.exists():
        flow_path.unlink()

    logger = _build_logger(cfg, audit=args.mode == "debug")
    simulation = ChickSimulation(cfg, logger)
    simulation.run()
    summary = simulation.summarize()
    logger.close()

    print("Capacity plan:")
    print(f"  Eggs per day required: {plan.eggs_per_day_required:,.0f}")
    print(f"  Shipments per day: {plan.shipments_per_day:,.2f}")
    print(f"  Pre-hatch carts per day: {plan.pre_hatch_carts_per_day:,.1f}")
    print(f"  Pre-hatch slots needed: {plan.pre_hatch_slots_needed:,.1f}")
    print(f"  Hatch slots needed: {plan.hatch_slots_needed:,.1f}")

    print("\nSimulation summary:")
    avg_slaughter = summary["avg_slaughter_per_day"]
    if avg_slaughter is None:
        print("  Not enough data beyond warmup period to compute average.")
    else:
    print(f"  Average slaughter-ready chickens per day: {avg_slaughter:,.0f}")
    print(f"  Total slaughtered over run: {summary['total_slaughtered']:,}")
    if args.mode == "debug":
        print(f"  Events stored in SQLite at: {db_path.resolve()}")
    else:
        print("  Audit logging disabled (execution mode).")


if __name__ == "__main__":
    main()
