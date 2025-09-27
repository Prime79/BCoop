#!/usr/bin/env python3
"""Render SVG flow graphs for top-occupancy barns.

Finds barns with highest current occupancy at the latest timestamp in the
Parquet flow log and renders barn-centric SVGs using barn_flow_graph.

Usage:
  python generate_barn_svgs.py --top 10 --flow-log flow_log.parquet \
    --out-dir notebooks/notebooks/outputs
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict

import polars as pl

from analysis.barn_flow import BarnFlowBuilder
from barn_flow_graph import render_svg


def load_flow(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise SystemExit(f"flow log not found: {path}")
    df = pl.read_parquet(path)
    return df.with_columns(pl.col("event_ts").str.strptime(pl.Datetime, strict=False).alias("event_dt"))


def latest_barn_states(flow: pl.DataFrame) -> Dict[str, Dict[str, float]]:
    subset = flow.filter(pl.col("resource_type") == "barn")
    if subset.is_empty():
        return {}
    ordered = subset.sort("event_dt")
    latest = ordered.group_by("resource_id", maintain_order=True).tail(1)
    result: Dict[str, Dict[str, float]] = {}
    for row in latest.iter_rows(named=True):
        to_state = row.get("to_state")
        if not to_state:
            continue
        try:
            import json

            state = json.loads(to_state)
        except Exception:
            state = {}
        shipments = {str(k): float(v) for k, v in state.items() if float(v or 0) > 0}
        if shipments:
            result[str(row["resource_id"])] = shipments
    return result


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--flow-log", default="flow_log.parquet", help="Path to flow_log.parquet")
    p.add_argument("--db-path", default="hatchery_events.sqlite", help="Path to hatchery events DB (unused; for compatibility)")
    p.add_argument("--top", type=int, default=10, help="How many barns to render")
    p.add_argument("--out-dir", default="notebooks/notebooks/outputs", help="Directory to write SVGs")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    flow_path = Path(args.flow_log)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    flow = load_flow(flow_path)
    states = latest_barn_states(flow)
    if not states:
        raise SystemExit("No barn states found in flow log.")

    # Rank barns by occupancy
    ranked = sorted(states.items(), key=lambda kv: sum(kv[1].values()), reverse=True)[: args.top]

    builder = BarnFlowBuilder(flow_path, Path(args.db_path))
    for barn_id, _ in ranked:
        bf = builder.build(barn_id)
        svg = render_svg(bf)
        out_path = out_dir / f"barn_flow_{barn_id}.svg"
        out_path.write_text(svg, encoding="utf-8")
        print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()

