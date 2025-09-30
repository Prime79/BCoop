#!/usr/bin/env python3
"""Generate an interactive telephely flow HTML page."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

import polars as pl

from analysis.barn_flow import BarnFlow, BarnFlowBuilder
from barn_flow_graph import build_telep_context, render_telep_svg


def load_flow(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise SystemExit(f"flow log not found: {path}")
    frame = pl.read_parquet(path)
    return frame.with_columns(
        pl.col("event_ts").str.strptime(pl.Datetime, strict=False).alias("event_dt")
    )


def find_barns(flow: pl.DataFrame, telep: str) -> List[str]:
    prefix = f"{telep}-barn"
    subset = flow.filter(
        (pl.col("resource_type") == "barn") & pl.col("resource_id").str.starts_with(prefix)
    )
    if subset.is_empty():
        return []
    latest = subset.sort("event_dt").group_by("resource_id", maintain_order=True).tail(1)
    return sorted(str(row["resource_id"]) for row in latest.iter_rows(named=True))


def summarise_barns(barn_flows: List[BarnFlow]) -> Tuple[Dict[str, Dict[str, float]], Dict[str, float]]:
    barn_stats: Dict[str, Dict[str, float]] = {}
    total_current = 0.0
    total_arrived = 0.0
    total_departed = 0.0
    for bf in barn_flows:
        key = f"barn:{bf.barn_id}"
        current = bf.current_occupancy
        arrived = bf.total_arrived
        departed = bf.total_departed
        barn_stats[key] = {
            "current": current,
            "arrived": arrived,
            "departed": departed,
            "shipments": float(len(bf.shipments)),
        }
        total_current += current
        total_arrived += arrived
        total_departed += departed
    telep_summary = {
        "current": total_current,
        "arrived": total_arrived,
        "departed": total_departed,
        "barns": len(barn_flows),
    }
    return barn_stats, telep_summary


def build_html(
    title: str,
    svg: str,
    node_data: Dict[str, object],
    edge_data: List[Dict[str, object]],
    barn_data: Dict[str, object],
    telep_summary: Dict[str, float],
) -> str:
    nodes_json = json.dumps(node_data, ensure_ascii=False)
    edges_json = json.dumps(edge_data, ensure_ascii=False)
    barns_json = json.dumps(barn_data, ensure_ascii=False)
    telep_json = json.dumps(telep_summary, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang=\"hu\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{title}</title>
  <style>
    :root {{ color-scheme: dark; }}
    body {{ margin: 0; background:#0b1220; color:#e6f0ff; font: 15px/1.5 'Inter', system-ui, sans-serif; }}
    header {{ padding: 16px 24px; border-bottom: 1px solid #223; position:sticky; top:0; background:rgba(11,18,32,0.95); z-index:20; backdrop-filter: blur(8px); }}
    h1 {{ margin: 0; font-size: 22px; }}
    .layout {{ display:flex; gap:24px; padding:20px 24px 32px; min-height: calc(100vh - 76px); }}
    .canvas {{ flex:1 1 70%; position:relative; }}
    svg {{ width: 100%; height: auto; display:block; border-radius:18px; border:1px solid #1a2338; box-shadow:0 30px 80px rgba(0,0,0,0.35); background:#0b1220; }}
    .node {{ cursor:pointer; transition:transform 0.18s ease; }}
    .node:hover {{ transform:translateZ(0) scale(1.04); }}
    .node.active rect:first-child {{ stroke:#00ddeb; stroke-width:1.6; }}
    path[data-src] {{ transition:opacity 0.2s ease, stroke-width 0.2s ease; }}
    path.edge-muted {{ opacity:0.15 !important; }}
    path.edge-active {{ opacity:1 !important; stroke-width: calc(var(--base-width, 4) * 1.35); filter:drop-shadow(0 0 8px rgba(0,221,235,0.35)); }}
    .tooltip {{ position:absolute; pointer-events:none; background:rgba(11,18,32,0.94); border:1px solid #2a3450; border-radius:10px; padding:6px 10px; font-size:13px; color:#e6f0ff; box-shadow:0 6px 18px rgba(0,0,0,0.4); display:none; z-index:30; min-width:160px; }}
    aside {{ flex:1 1 30%; background:#0e172a; border:1px solid #1f2a44; border-radius:18px; padding:18px 20px 22px; box-shadow:0 22px 60px rgba(0,0,0,0.32); position:relative; overflow:auto; max-height:calc(100vh - 140px); }}
    aside h2 {{ margin:0 0 6px; font-size:20px; }}
    aside h3 {{ margin:18px 0 6px; font-size:15px; text-transform:uppercase; letter-spacing:0.08em; opacity:0.7; }}
    .stat-grid {{ display:grid; gap:10px; grid-template-columns:repeat(auto-fit, minmax(140px,1fr)); margin:14px 0; }}
    .stat-card {{ background:#101d34; border:1px solid #1f2b45; border-radius:12px; padding:10px 12px; }}
    .stat-card span {{ display:block; }}
    .stat-card span.value {{ font-size:20px; font-weight:600; }}
    .neighbors {{ margin:0; padding:0; list-style:none; display:flex; flex-direction:column; gap:6px; }}
    .neighbors li {{ background:#101d34; border:1px solid #1f2b45; border-radius:10px; padding:8px 10px; }}
    .neighbors li strong {{ display:block; font-size:14px; }}
    .neighbors li span {{ font-size:13px; opacity:0.75; }}
    .empty {{ opacity:0.7; font-style:italic; margin:6px 0; }}
  </style>
</head>
<body>
  <header><h1>{title}</h1></header>
  <div class=\"layout\">
    <div class=\"canvas\">
      <div id=\"telep-tooltip\" class=\"tooltip\"></div>
      {svg}
    </div>
    <aside id=\"panel\">
      <h2>Telephely összefoglaló</h2>
      <div id=\"panel-content\" class=\"panel-content\">
        <div class=\"stat-grid\">
          <div class=\"stat-card\"><span>Aktuális állomány</span><span class=\"value\" id=\"telep-current\"></span></div>
          <div class=\"stat-card\"><span>Beérkezett</span><span class=\"value\" id=\"telep-arrived\"></span></div>
          <div class=\"stat-card\"><span>Elment</span><span class=\"value\" id=\"telep-departed\"></span></div>
          <div class=\"stat-card\"><span>Istállók</span><span class=\"value\" id=\"telep-barns\"></span></div>
        </div>
        <p class=\"empty\">Válassz egy csomópontot a részletes adatokhoz.</p>
      </div>
    </aside>
  </div>

  <script>
    const NODE_DATA = {nodes_json};
    const EDGE_DATA = {edges_json};
    const BARN_DATA = {barns_json};
    const TELEP_DATA = {telep_json};

    const tooltip = document.getElementById('telep-tooltip');
    const panelContent = document.getElementById('panel-content');
    const svg = document.querySelector('svg');

    function formatQty(value) {{
      if (value == null) return '—';
      const num = Number(value);
      if (!Number.isFinite(num)) return '—';
      if (Math.abs(num) >= 1_000_000) return (num/1_000_000).toFixed(2).replace('.', ',') + ' M';
      if (Math.abs(num) >= 1_000) return Math.round(num).toLocaleString('hu-HU');
      return Math.round(num).toLocaleString('hu-HU');
    }}

    function renderSummary(data) {{
      document.getElementById('telep-current').textContent = formatQty(data.current) + ' db';
      document.getElementById('telep-arrived').textContent = formatQty(data.arrived) + ' db';
      document.getElementById('telep-departed').textContent = formatQty(data.departed) + ' db';
      document.getElementById('telep-barns').textContent = data.barns?.toString?.() || '—';
    }}

    function showTooltip(node, ev) {{
      const label = node.dataset.label || '';
      const weight = node.dataset.weight || '0';
      tooltip.innerHTML = `<strong>${{label}}</strong><br>${{Number(weight).toLocaleString('hu-HU')}} db`;
      tooltip.style.display = 'block';
      positionTooltip(ev);
    }}

    function hideTooltip() {{ tooltip.style.display = 'none'; }}

    function positionTooltip(ev) {{
      if (tooltip.style.display !== 'block') return;
      tooltip.style.left = (ev.clientX + 14) + 'px';
      tooltip.style.top = (ev.clientY + 14) + 'px';
    }}

    function highlightNode(key) {{
      document.querySelectorAll('.node').forEach(n => n.classList.remove('active'));
      document.querySelectorAll('path[data-src]').forEach(path => {{
        path.classList.remove('edge-active');
        path.classList.remove('edge-muted');
        const base = parseFloat(path.getAttribute('stroke-width') || '4');
        path.style.setProperty('--base-width', base);
      }});
      if (!key) return;
      document.querySelectorAll(`.node[data-key="${{key}}"]`).forEach(n => n.classList.add('active'));
      const related = new Set();
      EDGE_DATA.forEach(edge => {{
        if (edge.src === key || edge.dst === key) {{
          related.add(edge.src + '>' + edge.dst);
        }}
      }});
      document.querySelectorAll('path[data-src]').forEach(path => {{
        const src = path.dataset.src || '';
        const dst = path.dataset.dst || '';
        const code = src + '>' + dst;
        if (related.has(code)) {{
          path.classList.add('edge-active');
        }} else {{
          path.classList.add('edge-muted');
        }}
      }});
    }}

    function neighborList(key) {{
      const inbound = [];
      const outbound = [];
      EDGE_DATA.forEach(edge => {{
        if (edge.dst === key) inbound.push(edge);
        if (edge.src === key) outbound.push(edge);
      }});
      inbound.sort((a,b) => (b.weight||0) - (a.weight||0));
      outbound.sort((a,b) => (b.weight||0) - (a.weight||0));

      function toItems(items, label) {{
        if (!items.length) return '<p class="empty">Nincs ' + label + ' kapcsolat.</p>';
        const parts = items.slice(0,6).map(edge => {{
          const other = edge.src === key ? edge.dst : edge.src;
          const meta = NODE_DATA[other] || {{ label: other, column: '' }};
          const dir = edge.src === key ? '→' : '←';
          return `<li><strong>${{dir}} ${{meta.label || other}}</strong><span>${{formatQty(edge.weight)}} db</span></li>`;
        }});
        return '<ul class="neighbors">' + parts.join('') + '</ul>';
      }}

      return `
        <h3>Bejövő</h3>
        ${{toItems(inbound, 'bejövő')}}
        <h3>Kimenő</h3>
        ${{toItems(outbound, 'kimenő')}}`;
    }}

    function describeNode(key) {{
      const meta = NODE_DATA[key];
      if (!meta) return '<p class="empty">Nincs adat a kiválasztott csomóponthoz.</p>';
      let html = `<h2>${{meta.label}}</h2>`;
      html += `
        <div class="stat-grid">
          <div class="stat-card"><span>Áramlás</span><span class="value">${{formatQty(meta.weight)}} db</span></div>
          <div class="stat-card"><span>Oszlop</span><span class="value">${{meta.column || '—'}}</span></div>
        </div>`;
      if (Array.isArray(meta.members) && meta.members.length && !['telep', 'barn'].includes(meta.column || '')) {{
        html += `<h3>Tagok</h3><p style=\"opacity:0.75\">${{meta.members.map(String).join(', ')}}</p>`;
      }}
      if (key.startsWith('barn:')) {{
        const barn = BARN_DATA[key];
        if (barn) {{
          html += `
            <h3>Istálló mutatók</h3>
            <div class=\"stat-grid\">
              <div class=\"stat-card\"><span>Aktuális</span><span class=\"value\">${{formatQty(barn.current)}} db</span></div>
              <div class=\"stat-card\"><span>Beérkezett</span><span class=\"value\">${{formatQty(barn.arrived)}} db</span></div>
              <div class=\"stat-card\"><span>Elhagyta</span><span class=\"value\">${{formatQty(barn.departed)}} db</span></div>
              <div class=\"stat-card\"><span>Szállítmányok</span><span class=\"value\">${{barn.shipments ?? '—'}}</span></div>
            </div>`;
        }}
      }} else if (key.startsWith('telep:')) {{
        html += `
          <h3>Telephely összesítés</h3>
          <div class=\"stat-grid\">
            <div class=\"stat-card\"><span>Aktuális</span><span class=\"value\">${{formatQty(TELEP_DATA.current)}} db</span></div>
            <div class=\"stat-card\"><span>Beérkezett</span><span class=\"value\">${{formatQty(TELEP_DATA.arrived)}} db</span></div>
            <div class=\"stat-card\"><span>Elment</span><span class=\"value\">${{formatQty(TELEP_DATA.departed)}} db</span></div>
            <div class=\"stat-card\"><span>Istállók</span><span class=\"value\">${{TELEP_DATA.barns ?? '—'}}</span></div>
          </div>`;
      }}
      html += neighborList(key);
      return html;
    }}

    renderSummary(TELEP_DATA);

    document.querySelectorAll('.node').forEach(node => {{
      node.addEventListener('mouseenter', ev => showTooltip(node, ev));
      node.addEventListener('mouseleave', hideTooltip);
      node.addEventListener('mousemove', positionTooltip);
      node.addEventListener('click', ev => {{
        const key = node.dataset.key;
        highlightNode(key);
        panelContent.innerHTML = describeNode(key);
        ev.stopPropagation();
      }});
    }});

    document.body.addEventListener('click', ev => {{
      if (ev.target.closest('.node') || ev.target.closest('aside')) return;
      highlightNode(null);
      panelContent.innerHTML = '<p class=\"empty\">Válassz egy csomópontot a részletes adatokhoz.</p>';
    }});

    svg.addEventListener('mouseleave', hideTooltip);
    svg.addEventListener('mousemove', positionTooltip);
  </script>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("telep", help="Telephely neve, pl. 'Kaba'")
    parser.add_argument("--flow-log", default="flow_log.parquet", help="Parquet flow log")
    parser.add_argument("--db-path", default="hatchery_events.sqlite", help="SQLite audit log (opcionális)")
    parser.add_argument("--out", default="notebooks/notebooks/outputs/telep_flow.html", help="Kimeneti HTML útvonala")
    parser.add_argument("--title", help="Egyedi cím a HTML fejléchez")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    flow_path = Path(args.flow_log)
    out_path = Path(args.out)

    flow = load_flow(flow_path)
    barns = find_barns(flow, args.telep)
    if not barns:
        raise SystemExit(f"Nem található istálló a megadott telephez: {args.telep}")

    builder = BarnFlowBuilder(flow_path, Path(args.db_path))
    barn_flows = [builder.build(barn_id) for barn_id in barns]
    context = build_telep_context(args.telep, barn_flows)
    svg = render_telep_svg(args.telep, barn_flows)

    node_data = {
        key: {
            "label": spec.get("label"),
            "column": spec.get("column"),
            "weight": float(spec.get("weight", 0.0) or 0.0),
            "members": spec.get("members", []),
        }
        for key, spec in context['nodes'].items()
    }
    edge_data = [
        {
            "src": edge.get("src"),
            "dst": edge.get("dst"),
            "weight": float(edge.get("weight", 0.0) or 0.0),
            "stage": edge.get("stage"),
        }
        for edge in context['edges']
        if edge.get("src") and edge.get("dst")
    ]
    barn_stats, telep_stats = summarise_barns(barn_flows)

    title = args.title or f"{args.telep} telephely összefűzött anyagárama"
    html = build_html(title, svg, node_data, edge_data, barn_stats, telep_stats)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
