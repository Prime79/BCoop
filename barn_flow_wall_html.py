#!/usr/bin/env python3
"""Interactive, scrollable HTML wall for all barns of a given site (prefix).

Stacks all selected barns (by `--barn-prefix`) on one page, each as an
interactive SVG: clicking an utókeltető (hatcher) node opens a Highcharts
modal with shipment contributions to that barn via the selected hatcher.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import polars as pl

from analysis.barn_flow import BarnFlow, BarnFlowBuilder
from barn_flow_graph import build_context, render_svg


def load_flow(path: Path) -> pl.DataFrame:
    df = pl.read_parquet(path)
    return df.with_columns(pl.col("event_ts").str.strptime(pl.Datetime, strict=False).alias("event_dt"))


def find_barns(flow: pl.DataFrame, prefix: str) -> List[str]:
    subset = flow.filter((pl.col("resource_type") == "barn") & pl.col("resource_id").str.starts_with(prefix))
    if subset.is_empty():
        return []
    latest = subset.sort("event_dt").group_by("resource_id", maintain_order=True).tail(1)
    return sorted(str(r["resource_id"]) for r in latest.iter_rows(named=True))


def compute_hatcher_breakdown(bf: BarnFlow) -> Dict[str, List[Tuple[str, float]]]:
    data: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for shipment in bf.shipments:
        sid = shipment.shipment_id
        for cart in shipment.cart_contributions:
            if not cart.hatcher_id:
                continue
            machine = cart.hatcher_id.split('-cart')[0]
            qty = float(cart.barn_chicks)
            if qty > 0:
                data[machine][sid] += qty
    result: Dict[str, List[Tuple[str, float]]] = {}
    for machine, per_ship in data.items():
        items = sorted(per_ship.items(), key=lambda kv: kv[1], reverse=True)
        result[machine] = items
    return result


def inject_hotspots(svg: str, ctx: Dict[str, object], barn_id: str) -> Tuple[str, Dict[str, List[Dict[str, float]]]]:
    nodes = ctx["nodes"]  # type: ignore
    hatcher_nodes = {k: v for k, v in nodes.items() if v.get("column") == "hatcher"}  # type: ignore
    # Placeholder; per-barn data will be filled by caller
    hotspot_elems: List[str] = []
    for key, spec in hatcher_nodes.items():
        x = float(spec["x"])  # type: ignore
        y = float(spec["y"])  # type: ignore
        title = f"Utókeltető {key.split('-')[-1]} — kattints a részletekért"
        hotspot_elems.append(
            f'<g class="hotspot" data-key="{barn_id}::{key}">'
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="32" fill="#000" fill-opacity="0.01" stroke="none" />'
            f'<title>{title}</title>'
            f'</g>'
        )
    hotspot_group = '<g id="hotspots">' + ''.join(hotspot_elems) + '</g>'
    injected_svg = svg.replace("</svg>", hotspot_group + "\n</svg>")
    return injected_svg, {"__hatchers__": []}


def build_page(title: str, panels: List[Tuple[str, str, Dict[str, List[Tuple[str, float]]]]]) -> str:
    # Build global data map keyed by "barn_id::hatcher_key"
    data_map: Dict[str, List[Dict[str, float]]] = {}
    sections: List[str] = []
    for barn_id, svg, breakdown in panels:
        # transform breakdown to series
        for hatch_key, items in breakdown.items():
            data_map[f"{barn_id}::{hatch_key}"] = [{"name": sid, "y": qty} for sid, qty in items]
        head = f"{barn_id} · Aktuális létszám a grafikonon látható"
        sections.append(f"<section class=\"panel\"><header>{head}</header>{svg}</section>")

    highcharts_js = Path('Highcharts-12/code/highcharts.js').read_text(encoding='utf-8') if Path('Highcharts-12/code/highcharts.js').exists() else ''
    sankey_js = Path('Highcharts-12/code/modules/sankey.js').read_text(encoding='utf-8') if Path('Highcharts-12/code/modules/sankey.js').exists() else ''

    html = """<!DOCTYPE html>
<html lang=\"hu\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{title}</title>
  <style>
    :root {{ --bg: #0b1220; --panel-bg: #0b1220; --text: #e6f0ff; }}
    html, body {{ margin: 0; padding: 0; background: var(--bg); color: var(--text); }}
    header.sticky {{ position: sticky; top: 0; background: rgba(11,18,32,0.95); backdrop-filter: saturate(180%) blur(6px); padding: 16px 24px; border-bottom: 1px solid #223; z-index: 10; }}
    h1 {{ margin: 0; font: 600 18px system-ui, -apple-system, Segoe UI, Roboto; }}
    .container {{ display: flex; flex-direction: column; gap: 24px; padding: 16px; }}
    .panel {{ background: var(--panel-bg); border: 1px solid #2a3450; border-radius: 12px; overflow: hidden; }}
    .panel header {{ padding: 12px 16px; border-bottom: 1px solid #223; font: 500 14px system-ui; opacity: 0.9; }}
    .panel svg {{ display: block; width: 100%; height: auto; }}
    .hotspot {{ cursor: pointer; pointer-events: all; }}
    .hotspot circle {{ pointer-events: all; }}
    .back-to-top {{ position: fixed; right: 16px; bottom: 16px; background: #1b2745; color: #e6f0ff; border: 1px solid #2a3450; border-radius: 20px; padding: 8px 12px; cursor: pointer; }}
    .modal {{ position: fixed; inset: 0; background: rgba(0,0,0,0.55); display: none; align-items: center; justify-content: center; }}
    .modal .panel {{ width: min(880px, 92vw); background: #0e172a; border: 1px solid #2a3450; border-radius: 12px; padding: 12px; }}
    .modal header {{ display:flex; align-items:center; justify-content:space-between; padding: 8px 8px 12px; color:#e6f0ff; font: 600 16px system-ui; }}
    .close {{ cursor:pointer; border:none; background:transparent; color:#9aa4b2; font-size:18px; }}
    #chart {{ height: 420px; }}
  </style>
</head>
<body>
  <header class=\"sticky\"><h1>{title}</h1></header>
  <main class=\"container\">{sections}</main>
  <button class=\"back-to-top\" onclick=\"window.scrollTo({{top:0,behavior:'smooth'}})\">Vissza az elejére</button>

  <div id=\"modal\" class=\"modal\" onclick=\"if(event.target===this)this.style.display='none'\">
    <div class=\"panel\">
      <header>
        <span id=\"modal-title\">Utókeltető</span>
        <button class=\"close\" onclick=\"document.getElementById('modal').style.display='none'\">✕</button>
      </header>
      <div id=\"chart\"></div>
    </div>
  </div>

  <script>{hc_js}</script>
  <script>{sankey_js_code}</script>
  <script>
    const DATA = {data_json};
    const modal = document.getElementById('modal');
    const titleEl = document.getElementById('modal-title');
    const chartEl = document.getElementById('chart');
    function openChartFor(key) {{
      const series = DATA[key] || [];
      const pretty = (key.split('::')[1] || key);
      titleEl.textContent = `Utókeltető ${{pretty.split('-').pop()}} — szállítmány megoszlás`;
      modal.style.display = 'flex';
      if (typeof Highcharts === 'undefined') {{
        chartEl.innerHTML = '<div style="color:#e6f0ff;padding:12px">Highcharts nincs betöltve.</div>';
        return;
      }}
      Highcharts.chart(chartEl, {{
        chart: {{ type: 'column', backgroundColor: '#0e172a' }},
        title: {{ text: null }},
        xAxis: {{ type: 'category', labels: {{ style: {{ color: '#c8cfdb' }} }} }},
        yAxis: {{ title: {{ text: 'db' }}, gridLineColor: '#223' }},
        legend: {{ enabled: false }},
        tooltip: {{ pointFormat: '<b>{{point.y:.0f}}</b> csibe' }},
        series: [{{ name: 'Szállítmány', data: series, color: '#5f6d85' }}],
        credits: {{ enabled: false }},
      }});
    }}

    document.addEventListener('click', (e) => {{
      const target = e.target.closest('.hotspot');
      if (!target) return;
      const key = target.dataset.key; // 'barn::hatcher'
      openChartFor(key);
    }});
    // Also support keyboard navigation (Enter) on hotspots
    document.addEventListener('keydown', (e) => {{
      if (e.key !== 'Enter') return;
      const active = document.querySelector('.hotspot:hover');
      if (!active) return;
      openChartFor(active.dataset.key);
    }});
  </script>
</body>
</html>
""".format(
        title=title,
        sections="\n".join(sections),
        data_json=json.dumps(data_map, ensure_ascii=False),
        hc_js=highcharts_js,
        sankey_js_code=sankey_js,
        )
    return html


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--barn-prefix", required=True, help="Prefix to select barns, e.g. 'Kaba-'")
    p.add_argument("--flow-log", default="flow_log.parquet", help="Path to flow_log.parquet")
    p.add_argument("--db-path", default="hatchery_events.sqlite", help="Path to hatchery (unused)")
    p.add_argument("--out", default="notebooks/notebooks/outputs/barn_flow_wall_interactive.html", help="Output HTML path")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    flow_path = Path(args.flow_log)
    flow = load_flow(flow_path)
    barns = find_barns(flow, args.barn_prefix)
    if not barns:
        raise SystemExit(f"No barns found with prefix '{args.barn_prefix}'.")

    builder = BarnFlowBuilder(flow_path, Path(args.db_path))
    panels: List[Tuple[str, str, Dict[str, List[Tuple[str, float]]]]] = []
    for barn_id in barns:
        bf = builder.build(barn_id)
        ctx = build_context(bf)
        svg = render_svg(bf)
        # inject hotspots per-barn with barn-qualified data-key
        nodes = ctx["nodes"]  # type: ignore
        hatcher_nodes = {k: v for k, v in nodes.items() if v.get("column") == "hatcher"}  # type: ignore
        hotspot_elems: List[str] = []
        for key, spec in hatcher_nodes.items():
            x = float(spec["x"])  # type: ignore
            y = float(spec["y"])  # type: ignore
            title = f"Utókeltető {key.split('-')[-1]} — kattints a részletekért"
            hotspot_elems.append(
                f'<g class="hotspot" data-key="{barn_id}::{key}">'
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="32" fill="#000" fill-opacity="0.01" stroke="none" />'
                f'<title>{title}</title>'
                f'</g>'
            )
        hotspot_group = '<g id="hotspots">' + ''.join(hotspot_elems) + '</g>'
        injected_svg = svg.replace("</svg>", hotspot_group + "\n</svg>")
        panels.append((barn_id, injected_svg, compute_hatcher_breakdown(bf)))

    html = build_page(f"Barn flow wall · {args.barn_prefix}", panels)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding='utf-8')
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
