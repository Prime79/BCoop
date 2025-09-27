#!/usr/bin/env python3
"""Interactive HTML graph for a single barn with clickable hatcher nodes.

Reuses the analysis builder and the static SVG renderer, then injects an
overlay of invisible hotspots on hatcher nodes. Clicking a hatcher opens a
Highcharts modal that shows shipment contributions to the barn via that
specific hatcher.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

from analysis.barn_flow import BarnFlow, BarnFlowBuilder
from barn_flow_graph import build_context, render_svg


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
    # normalize to list and sort by quantity desc
    result: Dict[str, List[Tuple[str, float]]] = {}
    for machine, per_ship in data.items():
        items = sorted(per_ship.items(), key=lambda kv: kv[1], reverse=True)
        result[machine] = items
    return result


def build_html_page(bf: BarnFlow, svg: str, ctx: Dict[str, object]) -> str:
    nodes = ctx["nodes"]  # type: ignore
    hatcher_nodes = {k: v for k, v in nodes.items() if v.get("column") == "hatcher"}  # type: ignore
    breakdown = compute_hatcher_breakdown(bf)

    # Prepare hotspot layer to insert into the SVG before closing tag
    hotspot_elems: List[str] = []
    for key, spec in hatcher_nodes.items():
        x = float(spec["x"])  # type: ignore
        y = float(spec["y"])  # type: ignore
        payload = breakdown.get(key, [])
        title = f"Utókeltető {key.split('-')[-1]} — kattints a részletekért"
        hotspot_elems.append(
            f'<g class="hotspot" data-key="{key}">'
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="32" fill="#000" fill-opacity="0.01" stroke="none" />'
            f'<title>{title}</title>'
            f'</g>'
        )
    hotspot_group = '<g id="hotspots">' + ''.join(hotspot_elems) + '</g>'
    injected_svg = svg.replace("</svg>", hotspot_group + "\n</svg>")

    # Build a compact JS data object for charts
    chart_data = {
        key: [{"name": sid, "y": qty} for sid, qty in breakdown.get(key, [])]
        for key in hatcher_nodes.keys()
    }

    html = """<!DOCTYPE html>
<html lang=\"hu\">
<head>
  <meta charset=\"utf-8\" />
  <title>{title}</title>
  <style>
    body {{ margin: 0; background:#0b1220; }}
    .wrap {{ max-width: 1600px; margin: 0 auto; }}
    svg {{ width: 100%; height: auto; display:block; }}
    .hotspot {{ cursor: pointer; pointer-events: all; }}
    .modal {{ position: fixed; inset: 0; background: rgba(0,0,0,0.55); display: none; align-items: center; justify-content: center; }}
    .modal .panel {{ width: min(880px, 92vw); background: #0e172a; border: 1px solid #2a3450; border-radius: 12px; padding: 12px; }}
    .modal header {{ display:flex; align-items:center; justify-content:space-between; padding: 8px 8px 12px; color:#e6f0ff; font: 600 16px system-ui; }}
    .close {{ cursor:pointer; border:none; background:transparent; color:#9aa4b2; font-size:18px; }}
    #chart {{ height: 420px; }}
  </style>
</head>
<body>
  <div class=\"wrap\">{svg}</div>

  <div id=\"modal\" class=\"modal\" onclick=\"if(event.target===this)this.style.display='none'\">
    <div class=\"panel\">
      <header>
        <span id=\"modal-title\">Utókeltető</span>
        <button class=\"close\" onclick=\"document.getElementById('modal').style.display='none'\">✕</button>
      </header>
      <div id=\"chart\"></div>
    </div>
  </div>

  <script>{highcharts_js}</script>
  <script>{sankey_js}</script>
  <script>
    const HATCHER_DATA = {data_json};
    const modal = document.getElementById('modal');
    const titleEl = document.getElementById('modal-title');
    const chartEl = document.getElementById('chart');
    const svgEl = document.querySelector('svg');
    svgEl.addEventListener('click', (e) => {{
      const target = e.target.closest('.hotspot');
      if (!target) return;
      const key = target.dataset.key;
      const series = HATCHER_DATA[key] || [];
      titleEl.textContent = `Utókeltető ${{key.split('-').pop()}} — szállítmány megoszlás`;
      modal.style.display = 'flex';
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
    }});
  </script>
</body>
</html>
""".format(
        title=f"{bf.barn_id} · {bf.cutoff.date().isoformat()}",
        svg=injected_svg,
        data_json=json.dumps(chart_data, ensure_ascii=False),
        highcharts_js=(Path('Highcharts-12/code/highcharts.js').read_text(encoding='utf-8') if Path('Highcharts-12/code/highcharts.js').exists() else ''),
        sankey_js=(Path('Highcharts-12/code/modules/sankey.js').read_text(encoding='utf-8') if Path('Highcharts-12/code/modules/sankey.js').exists() else ''),
    )
    return html


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("barn", help="Barn identifier, e.g. 'Kaba-barn-05'")
    p.add_argument("--flow-log", default="flow_log.parquet", help="Path to flow_log.parquet")
    p.add_argument("--db-path", default="hatchery_events.sqlite", help="Path to hatchery events (unused)")
    p.add_argument("--out", default="notebooks/notebooks/outputs/barn_flow_interactive.html", help="Output HTML path")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    builder = BarnFlowBuilder(Path(args.flow_log), Path(args.db_path))
    bf = builder.build(args.barn)
    ctx = build_context(bf)
    svg = render_svg(bf)
    html = build_html_page(bf, svg, ctx)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding='utf-8')
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
