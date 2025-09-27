from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
import os

from flask import Flask, jsonify, request, send_from_directory

from analysis.barn_flow import BarnFlowBuilder
from barn_flow_graph import build_context, context_to_highcharts

# Resolve paths relative to this file, with optional env overrides
BASE_DIR = Path(__file__).resolve().parent
FLOW_LOG_PATH = Path(
    os.environ.get("FLOW_LOG_PATH", (BASE_DIR / "flow_log.parquet").as_posix())
)
DB_PATH = Path(
    os.environ.get("DB_PATH", (BASE_DIR / "hatchery_events.sqlite").as_posix())
)
HCHARTS_DIR = Path(
    os.environ.get("HCHARTS_DIR", (BASE_DIR / "Highcharts-12/code").as_posix())
)
DEFAULT_BARN = 'Kisvarsany-barn-01'
DEFAULT_TIMESTAMP = '2025-02-07T00:00:00'

app = Flask(__name__)

_builder: BarnFlowBuilder | None = None


def get_builder() -> BarnFlowBuilder:
    global _builder
    if _builder is None:
        _builder = BarnFlowBuilder(FLOW_LOG_PATH, DB_PATH)
    return _builder


@app.get('/')
def index() -> str:
    return f"""<!DOCTYPE html>
<html lang="hu">
<head>
  <meta charset="utf-8" />
  <title>Istálló anyagáram</title>
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <style>
    body {{ background:#0b1220; color:#cbd2e0; font-family: 'Inter', system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif; margin:0; padding:0; }}
    header {{ padding:24px 32px; }}
    main {{ padding:0 32px 48px; display:flex; gap:32px; flex-wrap:wrap; }}
    form {{ background:#121b30; padding:16px 20px; border-radius:16px; display:flex; gap:12px; align-items:flex-end; flex-wrap:wrap; box-shadow:0 12px 40px rgba(0,0,0,0.25); }}
    label {{ font-size:14px; display:flex; flex-direction:column; gap:4px; }}
    input {{ background:#0d1628; border:1px solid #1f2a42; border-radius:8px; padding:8px 10px; color:#f3f5ff; min-width:200px; }}
    button {{ background:#00e0ff; color:#07101f; border:none; border-radius:8px; padding:9px 18px; font-weight:600; cursor:pointer; }}
    button:hover {{ background:#36ecff; }}
    #chart-container {{ width: min(880px, 100%); height:600px; margin-top:24px; background:#0f182c; border-radius:20px; padding:12px; box-shadow:0 18px 50px rgba(0,0,0,0.35); }}
    #sidebar {{ flex:1 1 280px; background:#121b30; padding:20px; border-radius:16px; box-shadow:0 12px 40px rgba(0,0,0,0.25); max-width:360px; }}
    h1 {{ margin:0; font-size:28px; color:#f2f5ff; }}
    h2 {{ margin:0 0 12px 0; font-size:18px; color:#9fefff; }}
    ul {{ list-style:none; padding:0; margin:0; display:flex; flex-direction:column; gap:6px; }}
    li {{ font-size:14px; color:#b8c2d4; }}
    .error {{ color:#ff7676; margin-top:12px; }}
  </style>
</head>
<body>
  <header>
    <h1 id="chart-title">Istálló anyagáram</h1>
  </header>
  <main>
    <section>
      <form id="controls">
        <label>Istálló azonosító
          <input type="text" name="barn" id="barn-input" value="{DEFAULT_BARN}" required />
        </label>
        <label>Időpont (ISO)
          <input type="text" name="timestamp" id="timestamp-input" value="{DEFAULT_TIMESTAMP}" placeholder="2025-02-07T00:00:00" />
        </label>
        <button type="submit">Frissítés</button>
        <span class="error" id="error-message"></span>
      </form>
      <div id="chart-container"></div>
    </section>
    <aside id="sidebar">
      <h2>Legutóbbi állapotváltozások</h2>
      <ul id="timeline-list"></ul>
    </aside>
  </main>
  <script src="/assets/highcharts.js"></script>
  <script src="/assets/modules/sankey.js"></script>
  <script>
    const DEFAULT_BARN = '{DEFAULT_BARN}';
    const DEFAULT_TIMESTAMP = '{DEFAULT_TIMESTAMP}';
    let activeChart = null;

    async function fetchFlow(barnId, timestamp) {{
      const url = new URL('/barn-flow', window.location.origin);
      if (barnId) url.searchParams.set('barn', barnId);
      if (timestamp) url.searchParams.set('timestamp', timestamp);
      const response = await fetch(url.toString());
      if (!response.ok) {{
        const text = await response.text();
        throw new Error(text || 'Ismeretlen hiba');
      }}
      return response.json();
    }}

    function renderTimeline(entries) {{
      const list = document.getElementById('timeline-list');
      list.innerHTML = '';
      if (!entries || !entries.length) {{
        const item = document.createElement('li');
        item.textContent = 'Nincs adat.';
        list.appendChild(item);
        return;
      }}
      entries.forEach((line) => {{
        const item = document.createElement('li');
        item.textContent = line;
        list.appendChild(item);
      }});
    }}

    function renderChart(payload) {{
      const nodes = payload.nodes.map((node) => ({{
        ...node,
        color: payload.highlightNodes.includes(node.id) ? '#00e0ff' : '#3b4556',
      }}));
      const links = payload.links.map((link) => ({{
        from: link.from,
        to: link.to,
        weight: link.weight,
        color: link.isHighlight ? '#00e0ff' : 'rgba(59,69,86,0.55)',
      }}));
      const options = {{
        title: {{ text: payload.metadata.title || 'Istálló anyagáram' }},
        chart: {{ backgroundColor: 'transparent' }},
        tooltip: {{
          pointFormatter: function () {{
            return '<b>' + Highcharts.numberFormat(this.weight || 0, 0) + ' db</b>';
          }},
        }},
        plotOptions: {{
          sankey: {{
            allowPointSelect: true,
            curveFactor: 0.35,
            nodeWidth: 30,
            dataLabels: {{
              color: '#f2f5ff',
              style: {{ textOutline: 'none' }},
            }},
          }},
        }},
        series: [{{
          type: 'sankey',
          name: payload.metadata.title || 'Istálló anyagáram',
          nodes,
          data: links,
        }}],
      }};
      if (activeChart) {{
        activeChart.update(options, true, true);
      }} else {{
        activeChart = Highcharts.chart('chart-container', options);
      }}
      renderTimeline(payload.metadata.timeline || []);
      document.getElementById('chart-title').textContent = payload.metadata.title || 'Istálló anyagáram';
    }}

    async function loadAndRender(barnId, timestamp) {{
      const errorNode = document.getElementById('error-message');
      errorNode.textContent = '';
      try {{
        const data = await fetchFlow(barnId, timestamp);
        renderChart(data);
      }} catch (err) {{
        console.error(err);
        errorNode.textContent = err.message;
      }}
    }}

    document.getElementById('controls').addEventListener('submit', (event) => {{
      event.preventDefault();
      const barn = document.getElementById('barn-input').value;
      const ts = document.getElementById('timestamp-input').value;
      loadAndRender(barn, ts);
    }});

    document.addEventListener('DOMContentLoaded', () => {{
      loadAndRender(DEFAULT_BARN, DEFAULT_TIMESTAMP);
    }});
  </script>
</body>
</html>
"""


@app.get('/assets/<path:filename>')
def highcharts_assets(filename: str):
    return send_from_directory(HCHARTS_DIR, filename)


@app.get('/barn-flow')
def barn_flow_endpoint():
    barn_id = request.args.get('barn', DEFAULT_BARN)
    timestamp = request.args.get('timestamp')
    cutoff = None
    if timestamp:
        try:
            cutoff = datetime.fromisoformat(timestamp)
        except ValueError:
            return jsonify({'error': 'Érvénytelen időbélyeg. Használj ISO formátumot (YYYY-MM-DD vagy YYYY-MM-DDTHH:MM:SS).'}), 400

    builder = get_builder()
    try:
        barn_flow = builder.build(barn_id, cutoff)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 404

    context = build_context(barn_flow)
    payload = context_to_highcharts(context)
    return jsonify(payload)


# ---------- Native SVG graph endpoints ----------

def _context_to_svg_payload(context: dict) -> dict:
    # Convert tuples to lists for JSON and pass through groups svg
    nodes_map = context.get('nodes', {})
    nodes = [
        {
            'id': key,
            'x': float(data.get('x', 0.0)),
            'y': float(data.get('y', 0.0)),
            'label': str(data.get('label', key)),
            'column': str(data.get('column', '')),
            'weight': float(data.get('weight', 0.0)),
        }
        for key, data in nodes_map.items()
    ]
    edges = []
    for e in context.get('edges', []):
        a = e.get('a', (0.0, 0.0))
        b = e.get('b', (0.0, 0.0))
        edges.append({
            'a': [float(a[0]), float(a[1])],
            'b': [float(b[0]), float(b[1])],
            'weight': float(e.get('weight', 0.0)),
            'color': e.get('color', '#3b4556'),
            'stage': e.get('stage', ''),
        })
    highlight = context.get('highlight') or {}
    points = [list(map(float, p)) for p in highlight.get('points', [])]
    groups_svg = context.get('groups', [])
    payload = {
        'title': context.get('title'),
        'nodes': nodes,
        'edges': edges,
        'highlight': {
            'points': points,
            'nodes': highlight.get('nodes', []),
            'edges': highlight.get('edges', []),
        },
        'groupsSvg': groups_svg,
        'timeline': context.get('timeline_entries', []),
    }
    return payload


@app.get('/graph')
def graph_page() -> str:
    html = """<!DOCTYPE html>
<html lang=\"hu\">
<head>
  <meta charset=\"utf-8\" />
  <title>Istálló anyagáram – Gráf</title>
  <style>
    body {{ background:#0b1220; color:#cbd2e0; font-family: 'Inter', system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif; margin:0; padding:0; }}
    header {{ padding:24px 32px; }}
    main {{ padding:0 32px 48px; display:flex; gap:32px; flex-wrap:wrap; }}
    form {{ background:#121b30; padding:16px 20px; border-radius:16px; display:flex; gap:12px; align-items:flex-end; flex-wrap:wrap; box-shadow:0 12px 40px rgba(0,0,0,0.25); }}
    label {{ font-size:14px; display:flex; flex-direction:column; gap:4px; }}
    input {{ background:#0d1628; border:1px solid #1f2a42; border-radius:8px; padding:8px 10px; color:#f3f5ff; min-width:200px; }}
    button {{ background:#00e0ff; color:#07101f; border:none; border-radius:8px; padding:9px 18px; font-weight:600; cursor:pointer; }}
    button:hover {{ background:#36ecff; }}
    #svg-wrap {{ width: min(1000px, 100%); background:#0f182c; border-radius:20px; padding:12px; box-shadow:0 18px 50px rgba(0,0,0,0.35); margin-top:24px; }}
    #chart-svg {{ width: 1600px; height: 1000px; max-width: 100%; }}
    #sidebar {{ flex:1 1 280px; background:#121b30; padding:20px; border-radius:16px; box-shadow:0 12px 40px rgba(0,0,0,0.25); max-width:360px; }}
    h1 {{ margin:0; font-size:28px; color:#f2f5ff; }}
    h2 {{ margin:0 0 12px 0; font-size:18px; color:#9fefff; }}
    ul {{ list-style:none; padding:0; margin:0; display:flex; flex-direction:column; gap:6px; }}
    li {{ font-size:14px; color:#b8c2d4; }}
    .label {{ fill:#c8cfdb; font-family: Inter, system-ui, -apple-system, 'Segoe UI', Roboto; }}
  </style>
  <script>
    const DEFAULT_BARN = '{DEFAULT_BARN}';
    const DEFAULT_TIMESTAMP = '{DEFAULT_TIMESTAMP}';
    const W = 1600, H = 1000;

    function svgDefs() {{
      return `
      <defs>
        <radialGradient id="nodeGlow" cx="50%" cy="50%" r="60%">
          <stop offset="0%" stop-color="#ffffff" stop-opacity="0.15"/>
          <stop offset="100%" stop-color="#00ddeb" stop-opacity="0.08"/>
        </radialGradient>
        <filter id="softGlow" x="-50%" y="-50%" width="200%" height="200%">
          <feGaussianBlur stdDeviation="6" result="blur"/>
          <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
        </filter>
        <marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
          <path d="M0,0 L6,3 L0,6 Z" fill="#9aa4b2" />
        </marker>
        <marker id="arrowBright" markerWidth="10" markerHeight="10" refX="7" refY="5" orient="auto">
          <path d="M0,0 L9,5 L0,10 Z" fill="#00e0ff" />
        </marker>
      </defs>`;
    }}

    function bezier(a, b, bend=0.35) {{
      const [x1,y1] = a, [x2,y2] = b;
      const cx1 = x1 + (x2-x1)*bend, cy1 = y1;
      const cx2 = x2 - (x2-x1)*bend, cy2 = y2;
      return `M${x1},${y1} C${cx1},${cy1} ${cx2},${cy2} ${x2},${y2}`;
    }}

    function scaleWidth(weight, maxWeight) {{
      if (!maxWeight || maxWeight <= 0) return 2.0;
      const base=1.6, span=6.0;
      return base + (weight/maxWeight)*span;
    }}

    function draw(payload) {{
      const svg = document.getElementById('chart-svg');
      svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
      svg.innerHTML = '';
      svg.insertAdjacentHTML('beforeend', svgDefs());
      // Groups
      (payload.groupsSvg || []).forEach(g => svg.insertAdjacentHTML('beforeend', g));
      const maxW = Math.max(1, ...payload.edges.map(e => e.weight || 0));
      // Edges
      payload.edges.forEach(e => {{
        const path = document.createElementNS('http://www.w3.org/2000/svg','path');
        path.setAttribute('d', bezier(e.a, e.b));
        path.setAttribute('fill','none');
        path.setAttribute('stroke', e.color || '#3b4556');
        path.setAttribute('stroke-width', scaleWidth(e.weight, maxW));
        path.setAttribute('opacity', e.stage === 'truck' ? '0.85' : '0.75');
        path.setAttribute('marker-end','url(#arrow)');
        svg.appendChild(path);
      }});
      // Highlight
      if (payload.highlight && (payload.highlight.points||[]).length >= 2) {{
        const pts = payload.highlight.points;
        for (let i=0;i<pts.length-1;i++) {{
          const p = document.createElementNS('http://www.w3.org/2000/svg','path');
          p.setAttribute('d', bezier(pts[i], pts[i+1]));
          p.setAttribute('fill','none');
          p.setAttribute('stroke','#00e0ff');
          p.setAttribute('stroke-width','5');
          p.setAttribute('opacity','0.95');
          p.setAttribute('filter','url(#softGlow)');
          p.setAttribute('marker-end','url(#arrowBright)');
          svg.appendChild(p);
        }}
      }}
      // Nodes
      payload.nodes.forEach(n => {{
        let rOuter = 40, rInner = 28, textY = 56;
        if (n.column === 'setter' && n.id === 'setter-all') {{
          rOuter = 60; rInner = 42; textY = 78;
        }}
        const g = document.createElementNS('http://www.w3.org/2000/svg','g');
        g.setAttribute('transform', `translate(${n.x},${n.y})`);
        const c1 = document.createElementNS('http://www.w3.org/2000/svg','circle');
        c1.setAttribute('cx','0'); c1.setAttribute('cy','0'); c1.setAttribute('r', String(rOuter));
        c1.setAttribute('fill','url(#nodeGlow)'); c1.setAttribute('stroke','#8a93a6'); c1.setAttribute('stroke-width','1.2');
        const c2 = document.createElementNS('http://www.w3.org/2000/svg','circle');
        c2.setAttribute('cx','0'); c2.setAttribute('cy','0'); c2.setAttribute('r', String(rInner));
        c2.setAttribute('fill','#0f1a2e'); c2.setAttribute('stroke','#2c3650'); c2.setAttribute('stroke-width','2');
        const t = document.createElementNS('http://www.w3.org/2000/svg','text');
        t.setAttribute('class','label'); t.setAttribute('x','0'); t.setAttribute('y', String(textY)); t.setAttribute('text-anchor','middle'); t.setAttribute('font-size','13');
        t.textContent = n.label;
        g.appendChild(c1); g.appendChild(c2); g.appendChild(t);
        svg.appendChild(g);
      }});
      document.getElementById('chart-title').textContent = payload.title || 'Istálló anyagáram';
      const list = document.getElementById('timeline-list');
      list.innerHTML='';
      (payload.timeline||[]).forEach(line => {{ const li=document.createElement('li'); li.textContent=line; list.appendChild(li); }});
    }}

    async function fetchGraph(barnId, timestamp) {{
      const url = new URL('/barn-graph', window.location.origin);
      if (barnId) url.searchParams.set('barn', barnId);
      if (timestamp) url.searchParams.set('timestamp', timestamp);
      const resp = await fetch(url.toString());
      if (!resp.ok) throw new Error(await resp.text());
      return resp.json();
    }}

    document.addEventListener('DOMContentLoaded', async () => {{
      const form = document.getElementById('controls');
      form.addEventListener('submit', async (ev) => {{
        ev.preventDefault();
        const barn = document.getElementById('barn-input').value;
        const ts = document.getElementById('timestamp-input').value;
        const data = await fetchGraph(barn, ts);
        draw(data);
      }});
      const data = await fetchGraph(DEFAULT_BARN, DEFAULT_TIMESTAMP);
      draw(data);
    }});
  </script>
</head>
<body>
  <header>
    <h1 id=\"chart-title\">Istálló anyagáram – Gráf</h1>
  </header>
  <main>
    <section>
      <form id=\"controls\">
        <label>Istálló azonosító
          <input type=\"text\" name=\"barn\" id=\"barn-input\" value=\"{DEFAULT_BARN}\" required />
        </label>
        <label>Időpont (ISO)
          <input type=\"text\" name=\"timestamp\" id=\"timestamp-input\" value=\"{DEFAULT_TIMESTAMP}\" placeholder=\"2025-02-07T00:00:00\" />
        </label>
        <button type=\"submit\">Frissítés</button>
      </form>
      <div id=\"svg-wrap\">
        <svg id=\"chart-svg\"></svg>
      </div>
    </section>
    <aside id=\"sidebar\">
      <h2>Legutóbbi állapotváltozások</h2>
      <ul id=\"timeline-list\"></ul>
    </aside>
  </main>
 </body>
 </html>
"""
    return html.replace('{DEFAULT_BARN}', DEFAULT_BARN).replace('{DEFAULT_TIMESTAMP}', DEFAULT_TIMESTAMP)


@app.get('/barn-graph')
def barn_graph_endpoint():
    barn_id = request.args.get('barn', DEFAULT_BARN)
    timestamp = request.args.get('timestamp')
    cutoff = None
    if timestamp:
        try:
            cutoff = datetime.fromisoformat(timestamp)
        except ValueError:
            return jsonify({'error': 'Érvénytelen időbélyeg. Használj ISO formátumot (YYYY-MM-DD vagy YYYY-MM-DDTHH:MM:SS).'}), 400

    builder = get_builder()
    try:
        barn_flow = builder.build(barn_id, cutoff)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 404

    context = build_context(barn_flow)
    payload = _context_to_svg_payload(context)
    return jsonify(payload)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
