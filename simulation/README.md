# Hatchery Simulation Overview

The simulation tracks end-to-end poultry logistics starting with 100 000-egg shipments and ending with slaughter-ready birds. It relies on [SimPy](https://simpy.readthedocs.io/) for discrete-event modeling and logs every step to SQLite for downstream analytics and traceability.

## Process Outline

1. **Shipment Intake** – Each shipment (~100 000 eggs) arrives on a transport manifest. Metadata records the source egg farm and the number of farm cars (3 520 eggs each) used.
2. **Setter Loading** – Shipments are decomposed into 7 040-egg setter carts and assigned to one of 56 setter machines (18 carts per machine). Setter events capture the machine/slot and timestamps for every cart.
3. **X-ray & Vaccination** – Cart-level betavariate losses model candling and in-ovo operations. Discard counts roll up to the parent shipment for aggregate reporting.
4. **Hatcher Loading** – Fertile eggs transfer onto hatcher carts, again tracked per machine slot. Hatch duration varies between 3–5 days and produces the chick count plus hatch losses.
5. **Processing** – Chicks undergo sorting, measuring, and vaccination before loading onto 3 200-chick transport trucks.
6. **Farm Allocation** – Truck loads are routed into a curated list of grow-out farms. Each farm has five barns; capacities reflect the supplied net placement figures. Mixing is logged per barn so we can reconstruct which shipments occupy the same house.
7. **Grow-Out & Slaughter** – Barn cohorts grow for 56 days with survival sampled from a beta distribution. Survivors feed slaughter stats; losses remain associated with the source shipment and barn.
8. **Cleaning** – When a barn empties, it enters a 7-day cleaning window during which no new chicks can be placed. Events note the cleaning start/finish.

## Key Modules

- `simulation.config`
  - `FarmSpec`: Captures farm-level placement capacity and barn count.
  - `SimulationConfig`: Aggregates all tunable constants (shipment sizing, machine counts, durations, stochastic parameters, database path).
  - `derive_capacity(cfg)`: Computes steady-state requirements (shipments per day, setter/hatcher slot pressure, chicks needed at farms).
- `simulation.model`
  - `CartResult`: Captures the output of a single setter/hatcher cart.
  - `BarnPlace`: Tracks occupancy, mix, and cleaning window for each barn.
  - `ChickSimulation`: The orchestrator that spawns shipments, routes carts through machines, dispatches trucks, manages grow-out, and records slaughter output.
- `simulation.logger.EventLogger`: Handles batched persistence to SQLite.
- `run_simulation.py`: Convenience script that wipes the database, runs the simulation, and prints capacity + throughput summaries.

## Running the Simulation

```bash
source .venv/bin/activate
python run_simulation.py
```

Artifacts are written to `hatchery_events.sqlite`. Cart-level events expose setter/hatcher machine usage, while shipment-level events (inventory → farm placement) enable Sankey visualisations or provenance queries.
