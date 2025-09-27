from pathlib import Path

from simulation.config import SimulationConfig, derive_capacity
from simulation.logger import EventLogger
from simulation.model import ChickSimulation


def main() -> None:
    # Shorter run for quick regeneration of flow_log.parquet with parent_pair metadata
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

    logger = EventLogger(cfg.db_path)
    sim = ChickSimulation(cfg, logger)
    sim.run()
    logger.close()
    print(f"Flow written to: {flow_path.resolve()}")


if __name__ == "__main__":
    main()

