import argparse
from pathlib import Path

from simulation.config import SimulationConfig, derive_capacity
from simulation.logger import EventLogger, NoOpEventLogger
from simulation.model import ChickSimulation


def _build_logger(cfg: SimulationConfig, audit: bool):
    return EventLogger(cfg.db_path) if audit else NoOpEventLogger()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a shortened simulation for testing")
    parser.add_argument(
        "--mode",
        choices=("exec", "debug"),
        default="exec",
        help="Enable SQLite audit logging in 'debug' mode; default 'exec' disables it.",
    )
    return parser.parse_args()


def main() -> None:
    # Shorter run for quick regeneration of flow_log.parquet with parent_pair metadata
    args = _parse_args()
    cfg = SimulationConfig(simulation_days=90, warmup_days=15)

    # Clean previous outputs
    db_path = Path(cfg.db_path)
    if db_path.exists():
        try:
            db_path.unlink()
        except Exception:
            pass
    flow_path = Path(cfg.flow_log_path)
    if flow_path.exists():
        try:
            flow_path.unlink()
        except Exception:
            pass

    logger = _build_logger(cfg, audit=args.mode == "debug")
    sim = ChickSimulation(cfg, logger)
    sim.run()
    logger.close()
    print(f"Flow written to: {flow_path.resolve()}")


if __name__ == "__main__":
    main()
