# Hatchery Simulation Overview

This simulation models the full flow from egg arrival through slaughter and post-cycle cleaning. It is implemented with [SimPy](https://simpy.readthedocs.io/) and persists every event into SQLite for downstream analytics.

## Process Stages

1. **Inventory Arrival** – Egg batches sourced from 15 farms enter the system as car-sized loads. Each batch is tagged for either standard or QS destinations according to the configured ratio.
2. **Pre-Hatch Rooms** – Cars occupy pre-hatch rooms for 24 simulated days. The number of concurrent cars is capped by the derived capacity plan (27 sites × 60 rooms × 18 cars/room).
3. **X-Ray & Vaccination** – After pre-hatch, unfertile eggs are discarded using a beta-distributed pass rate. Surviving eggs are vaccinated and moved forward.
4. **Hatch Rooms** – Fertile eggs spend 3–5 days in hatch rooms before chicks emerge, again subject to a beta-distributed hatch rate.
5. **Processing** – Chicks are sorted, measured, and vaccinated. Processing and container loading are modeled as short-duration delays.
6. **Transport & Logistics** – Transport containers compete for a shared pool of trucks (80 trucks × 16 cars each). Travel to farms takes one day.
7. **Farm Placement** – Farms have limited places (60 farms × 12 places × 27,000 chicks). Thirty percent of capacity is reserved for QS batches. Farm slots stay occupied through grow-out and cleaning.
8. **Grow-Out & Slaughter** – Chicks grow for 56 days. Survival is sampled from a beta distribution. Survivors are logged as slaughter-ready output.
9. **Cleaning & Reset** – After slaughter shipment, farm places remain blocked for a 7-day cleaning period before capacity is released.

## Key Modules & Functions

- `simulation.config`
  - `SimulationConfig`: Dataclass containing all tunable parameters (durations, capacities, ratios, stochastic loss parameters, DB path).
  - `CapacityPlan`: Dataclass summarizing derived capacity numbers used to size resources.
  - `derive_capacity(cfg)`: Computes expected daily cars, site counts, and slot capacities from the configuration.
- `simulation.logger`
  - `EventLogger`: Lightweight SQLite writer. `log()` batches inserts, `close()` flushes pending rows.
- `simulation.model`
  - `EggBatch`: Represents a single car of eggs with source metadata and target segment.
  - `ChickSimulation`: Encapsulates the SimPy environment, resources, and processes. Notable methods:
    - `run()`: Starts event generation and advances the simulation clock.
    - `_generate_egg_batches()`: Produces daily car loads using a Poisson process.
    - `_handle_batch(batch)`: Runs a batch through inventory, pre-hatch, x-ray, hatch, processing, transport, farm placement.
    - `_manage_farm_cycle(batch, chicks, container)`: Covers grow-out, slaughter logging, and cleaning release.
    - `summarize()`: Aggregates post-warmup slaughter output to compute daily averages.
- `run_simulation.main()`: Entry point that instantiates the configuration, resets the SQLite database, executes the simulation, and prints headline metrics.

## Running the Simulation

```bash
source .venv/bin/activate
python run_simulation.py
```

This produces a capacity summary and the average number of slaughter-ready chickens per steady-state day. All stage-level events are stored in `hatchery_events.sqlite` for further analysis.
