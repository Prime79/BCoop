#!/usr/bin/env python3
"""Build a vertically scrollable HTML wall of barn flow graphs.

Finds barns by prefix (e.g., "Kaba-") from the Parquet flow log, renders each
barn's SVG using the existing renderer, and stacks them into one HTML page.

Usage:
  python multi_barn_flow_wall.py --barn-prefix Kaba- \
    --flow-log flow_log.parquet \
    --out notebooks/notebooks/outputs/barn_flow_wall_Kaba.html
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import polars as pl

from analysis.barn_flow import BarnFlowBuilder
from barn_flow_graph import render_svg


def load_flow(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise SystemExit(f"flow log not found: {path}")
    df = pl.read_parquet(path)
    return df.with_columns(pl.col("event_ts").str.strptime(pl.Datetime, strict=False).alias("event_dt"))


def find_barns(flow: pl.DataFrame, prefix: str) -> List[str]:
    subset = flow.filter((pl.col("resource_type") == "barn") & pl.col("resource_id").str.starts_with(prefix))
    if subset.is_empty():
        return []
    latest = subset.sort("event_dt").group_by("resource_id", maintain_order=True).tail(1)
    return sorted(str(r["resource_id"]) for r in latest.iter_rows(named=True))


def build_html(title: str, panels: List[str]) -> str:
    sections = []
    for panel in panels:
        sections.append(panel)
    return """<!DOCTYPE html>
<html lang=\"hu\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{title}</title>
  <style>
    :root {{ --bg: #0b1220; --panel-bg: #0b1220; --text: #e6f0ff; }}
    html, body {{ margin: 0; padding: 0; background: var(--bg); color: var(--text); }}
    header {{ position: sticky; top: 0; background: rgba(11,18,32,0.95); backdrop-filter: saturate(180%) blur(6px); padding: 16px 24px; border-bottom: 1px solid #223; z-index: 10; }}
    h1 {{ margin: 0; font: 600 18px system-ui, -apple-system, Segoe UI, Roboto; }}
    .container {{ display: flex; flex-direction: column; gap: 24px; padding: 16px; }}
    .panel {{ background: var(--panel-bg); border: 1px solid #2a3450; border-radius: 12px; overflow: hidden; }}
    .panel header {{ padding: 12px 16px; border-bottom: 1px solid #223; font: 500 14px system-ui; opacity: 0.9; }}
    .panel svg {{ display: block; width: 100%; height: auto; }}
    .back-to-top {{ position: fixed; right: 16px; bottom: 16px; background: #1b2745; color: #e6f0ff; border: 1px solid #2a3450; border-radius: 20px; padding: 8px 12px; cursor: pointer; }}
  </style>
  </head>
<body>
  <header><h1>{title}</h1></header>
  <main class=\"container\">{sections}</main>
  <button class=\"back-to-top\" onclick=\"window.scrollTo({{top:0,behavior:'smooth'}})\">Vissza az elejére</button>
</body>
</html>
""".format(title=title, sections="\n".join(sections))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--barn-prefix", required=True, help="Prefix to select barns, e.g. 'Kaba-'")
    p.add_argument("--flow-log", default="flow_log.parquet", help="Path to flow_log.parquet")
    p.add_argument("--db-path", default="hatchery_events.sqlite", help="Path to hatchery SQLite (unused here)")
    p.add_argument("--out", default="notebooks/notebooks/outputs/barn_flow_wall.html", help="Output HTML path")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    flow_path = Path(args.flow_log)
    out_path = Path(args.out)
    flow = load_flow(flow_path)
    barns = find_barns(flow, args.barn_prefix)
    if not barns:
        raise SystemExit(f"No barns found with prefix '{args.barn_prefix}'.")

    builder = BarnFlowBuilder(flow_path, Path(args.db_path))
    panels: List[str] = []
    for barn_id in barns:
        bf = builder.build(barn_id)
        svg = render_svg(bf)
        meta = f"{barn_id} · Aktuális létszám: {int(bf.current_occupancy):,} · Szállítmányok: {len(bf.shipments)}".replace(',', ' ')
        panels.append(f"<section class=\"panel\"><header>{meta}</header>{svg}</section>")

    html = build_html(f"Barn flow wall · {args.barn_prefix}", panels)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
