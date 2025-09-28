#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import polars as pl

from analysis.barn_flow import BarnFlowBuilder
from barn_flow_graph_html import build_html_page as build_single_html
from barn_flow_graph import render_svg, build_context


def load_flow(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path).with_columns(
        pl.col("event_ts").str.strptime(pl.Datetime, strict=False).alias("event_dt")
    )


def find_barns(flow: pl.DataFrame, prefix: str) -> List[str]:
    subset = flow.filter((pl.col("resource_type") == "barn") & pl.col("resource_id").str.starts_with(prefix))
    if subset.is_empty():
        return []
    latest = subset.sort("event_dt").group_by("resource_id", maintain_order=True).tail(1)
    return sorted(str(r["resource_id"]) for r in latest.iter_rows(named=True))


def main() -> None:
    ap = argparse.ArgumentParser(description="Build an interactive wall page as a stack of per-barn interactive HTML iframes.")
    ap.add_argument("--barn-prefix", required=True)
    ap.add_argument("--flow-log", default="flow_log.parquet")
    ap.add_argument("--out", default="notebooks/notebooks/outputs/barn_flow_wall_iframes.html")
    args = ap.parse_args()

    flow_path = Path(args.flow_log)
    flow = load_flow(flow_path)
    barns = find_barns(flow, args.barn_prefix)
    if not barns:
        raise SystemExit(f"No barns found for prefix {args.barn_prefix}")

    # Ensure individual interactive pages exist
    out_dir = Path(args.out).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    builder = BarnFlowBuilder(flow_path, Path('hatchery_events.sqlite'))
    iframe_paths: List[str] = []
    for barn in barns:
        bf = builder.build(barn)
        ctx = build_context(bf)
        svg = render_svg(bf)
        html = build_single_html(bf, svg, ctx, flow_path)
        fp = out_dir / f"barn_flow_{barn}_interactive.html"
        fp.write_text(html, encoding='utf-8')
        iframe_paths.append(fp.name)

    wall = """<!DOCTYPE html>
<html lang=\"hu\">
<head>
  <meta charset=\"utf-8\" />
  <title>Interactive wall · {prefix}</title>
  <style>
    body {{ margin:0; background:#0b1220; color:#e6f0ff; font: 14px system-ui; }}
    header {{ position:sticky; top:0; background:rgba(11,18,32,0.95); padding:12px 16px; border-bottom:1px solid #223; z-index:10; }}
    .grid {{ display:flex; flex-direction:column; gap:24px; padding:16px; }}
    iframe {{ width:100%; height:1020px; border:1px solid #2a3450; border-radius:12px; background:#0b1220; }}
  </style>
</head>
<body>
  <header><h1 style=\"margin:0\">Interactive wall · {prefix}</h1></header>
  <div class=\"grid\">
    {iframes}
  </div>
</body>
</html>
""".format(
        prefix=args.barn_prefix,
        iframes="\n".join(f"<iframe src=\"{name}\"></iframe>" for name in iframe_paths),
    )
    Path(args.out).write_text(wall, encoding='utf-8')
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
