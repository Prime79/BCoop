# BAromfiCoop POC — Flow Simulation and Graphs

This repository contains a discrete‑event simulation of hatchery → logistics → barn placement, and tools to render farm/barn‑centric flow graphs directly from an append‑only Parquet flow log. The workflow no longer depends on SQLite for plotting; parent‑pair (“szülőpár”) is embedded in the Parquet log metadata.

## What’s here

- `simulation/` — SimPy‑based model and configuration
  - `model.py` — end‑to‑end process (inventory → setter → hatcher → processing → trucks → barns → grow‑out/slaughter)
  - `config.py` — simulation parameters and farm capacities
  - `logger.py` — event logger (SQLite if available, JSONL fallback)
- `flow_writer.py` — append‑only Parquet sink for flow events (read by Polars)
- `analysis/barn_flow.py` — Parquet‑only analysis builder producing a structured, barn‑specific view (shipments, carts, timeline, state changes)
- `barn_flow_graph.py` — static SVG graph generator (Parent → Setter → Hatcher → Truck → Barn) with weighted edges
- `quick_run.py` — short simulation run (90 days) to regenerate `flow_log.parquet`
- `run_simulation.py` — full simulation run (default configuration)

## Key changes (2025 Q4)

- Parent‑pair moved to Parquet: when a shipment arrives, `model.py` writes `metadata:{"parent_pair": "parent-pair-XX"}` into the flow log. All plotting and analysis can run from the Parquet file alone.
- Atomic truck placement: the simulation now plans a truck’s drop across barns first, then writes exactly one barn state update per affected barn. This eliminates the small +/- “reallocation noise” in the barn timeline.

## Environment

This project uses Python 3.11+ with:

- Polars (data loading)
- SimPy, NumPy (simulation)

Install (example):

```
pip install polars simpy numpy
```

SQLite is optional. If the `sqlite3` Python module is unavailable, the simulation falls back to writing a `events.jsonl` audit stream; the Parquet flow log is unaffected.

## Quick start

1) Run a short simulation (writes a fresh `flow_log.parquet` with parent‑pair metadata):

```
python quick_run.py
```

2) Render a barn‑centric flow graph (no SQLite needed):

```
python barn_flow_graph.py Kaba-barn-01 --output notebooks/notebooks/outputs/barn_flow_Kaba-barn-01.svg
```

The graph displays:

- Left: Parent pairs (“Tojásfarmok / szülőpárok”)
- Middle: Setter and hatcher machines (top‑N shown; the rest grouped)
- Right: Trucks and target barn
- Edge widths are proportional to the quantity flowing through.

## Producing multiple plots

You can compute the barns with largest current occupancy and generate plots in a loop using Polars over `flow_log.parquet`, then call `barn_flow_graph.py` for each barn id. (An example can be added as a small script on request.)

## Design notes

- Parquet schema (see `flow_writer.py`):
  - `shipment_id`, `resource_id`, `resource_type`, `from_state`, `to_state`, `event_ts`, `quantity`, `metadata`
- Parent pairs are read from `metadata` on `resource_type == "inventory"` rows.
- The analysis builder (`analysis/barn_flow.py`) reconstructs shipment cart flows and barn state without touching SQLite.

## Troubleshooting

- “Szülőpár ismeretlen” a grafikonon: futtasd újra a szimulációt (pl. `quick_run.py`), hogy az új Parquet fájlban benne legyen a parent‑pair metadata.
- Túl sok “Egyéb …” csoport: növeld a `TOP_PARENTS`, `TOP_SETTERS`, `TOP_HATCHERS`, `TOP_TRUCKS` értékeket a `barn_flow_graph.py`‑ben.

