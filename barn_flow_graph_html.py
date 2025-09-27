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
from string import Template


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
    setter_nodes = {k: v for k, v in nodes.items() if v.get("column") == "setter"}  # type: ignore
    transfer_nodes = {k: v for k, v in nodes.items() if v.get("column") == "transfer"}  # type: ignore
    breakdown = compute_hatcher_breakdown(bf)

    # Transfer (parent-pair) stats per barn
    transfer_stats: Dict[str, Dict[str, object]] = {}
    for shipment in bf.shipments:
        parent = shipment.parent_pair
        st = transfer_stats.setdefault(parent, {
            'shipments': 0,
            'eggs_to_barn': 0.0,
            'chicks_to_barn': 0.0,
            'first_inventory': None,
        })
        st['shipments'] = int(st['shipments']) + 1
        st['chicks_to_barn'] = float(st['chicks_to_barn']) + float(shipment.barn_quantity or 0.0)
        st['eggs_to_barn'] = float(st['eggs_to_barn']) + float(sum(c.barn_eggs for c in shipment.cart_contributions))
        inv = shipment.timeline.inventory_ready or shipment.timeline.barn_arrival
        if inv is not None and (st['first_inventory'] is None or inv < st['first_inventory']):
            st['first_inventory'] = inv
    total_chicks = sum(v['chicks_to_barn'] for v in transfer_stats.values()) or 1.0
    for v in transfer_stats.values():
        v['share_pct'] = round(100.0 * float(v['chicks_to_barn']) / float(total_chicks), 1)
        v['age_days'] = (bf.cutoff - v['first_inventory']).days if v['first_inventory'] else None

    # Prepare hotspot layer to insert into the SVG before closing tag
    hotspot_elems: List[str] = []
    for key, spec in hatcher_nodes.items():
        x = float(spec["x"])  # type: ignore
        y = float(spec["y"])  # type: ignore
        payload = breakdown.get(key, [])
        title = f"Utókeltető {key.split('-')[-1]} — kattints a részletekért"
        hotspot_elems.append(
            f'<g class="hotspot" data-key="{key}">'
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="40" fill="#000" fill-opacity="0.08" stroke="none" />'
            f'<title>{title}</title>'
            f'</g>'
        )
    # Also add setter hotspots using same modal (shows extra Turn chart)
    for key, spec in setter_nodes.items():
        x = float(spec["x"])  # type: ignore
        y = float(spec["y"])  # type: ignore
        title = f"Előkeltető {key.split('-')[-1]} — kattints a részletekért"
        hotspot_elems.append(
            f'<g class="hotspot" data-key="{key}">'
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="40" fill="#000" fill-opacity="0.08" stroke="none" />'
            f'<title>{title}</title>'
            f'</g>'
        )
    # Transfer hotspots (parent-pair batches)
    for key, spec in transfer_nodes.items():
        x = float(spec["x"])  # type: ignore
        y = float(spec["y"])  # type: ignore
        title = f"Transzfer {key.split('-')[-1]} — kattints a részletekért"
        hotspot_elems.append(
            f'<g class="hotspot" data-key="{key}">'
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="40" fill="#000" fill-opacity="0.08" stroke="none" />'
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

    tpl = Template("""<!DOCTYPE html>
<html lang=\"hu\">
<head>
  <meta charset=\"utf-8\" />
  <title>$title</title>
  <style>
    body {{ margin: 0; background:#0b1220; }}
    .wrap {{ max-width: 1600px; margin: 0 auto; }}
    svg {{ width: 100%; height: auto; display:block; }}
    .hotspot {{ cursor: pointer; pointer-events: all; }}
    .popover { position: fixed; display: none; z-index: 9999; width: min(880px, 92vw); max-height: 86vh; overflow: auto; background: #0e172a; border: 1px solid #2a3450; border-radius: 12px; box-shadow: 0 12px 48px rgba(0,0,0,0.45); }
    .popover header { display:flex; align-items:center; justify-content:space-between; padding: 8px 12px 10px; color:#e6f0ff; font: 600 16px system-ui; position: sticky; top: 0; background:#0e172a; border-bottom: 1px solid #223; }
    .close {{ cursor:pointer; border:none; background:transparent; color:#9aa4b2; font-size:18px; }}
    .charts {{ display: grid; grid-template-columns: 1fr; gap: 16px; padding: 8px; }}
    .chart {{ height: 260px; }}
  </style>
</head>
<body>
  <div class=\"wrap\">$svg</div>

  <div id=\"popover\" class=\"popover\">
    <header>
      <span id=\"modal-title\">Részletek</span>
      <button class=\"close\" onclick=\"document.getElementById('popover').style.display='none'\">✕</button>
    </header>
    <div id=\"charts\" class=\"charts\"> 
      <div id=\"chart-temp\" class=\"chart\"></div>
      <div id=\"chart-hum\" class=\"chart\"></div>
      <div id=\"chart-co2\" class=\"chart\"></div>
    </div>
    <div id=\"stats\" style=\"display:none; padding:12px; color:#e6f0ff\"></div>
  </div>

  <script>$hc_js</script>
  <script>$sankey_js</script>
  <script>
    const HATCHER_DATA = $data_json;
    const TRANSFER_STATS = $transfer_json;
    const pop = document.getElementById('popover');
    const titleEl = document.getElementById('modal-title');
    const elTemp = document.getElementById('chart-temp');
    const elHum = document.getElementById('chart-hum');
    const elCO2 = document.getElementById('chart-co2');
    const chartsWrap = document.getElementById('charts');
    const statsWrap = document.getElementById('stats');
    const svgEl = document.querySelector('svg');

    // Nice, readable palette per metric
    const COLORS = {
      temp: '#00ddeb',   // cyan
      hum:  '#5b8def',   // blue
      co2:  '#ffa552',   // orange
      air:  '#ad7bf7',   // purple
      turn: '#7bd389',   // green
      noise:'#ff7ab6',   // pink
    };

    // Utility: clamp value into [min,max]
    const clamp=(v,min,max)=>Math.max(min, Math.min(max,v));

    // Time helpers
    const NOW = ()=>Date.now();
    const TICK = 30*60*1000; // 30 minutes

    // Low-pass filtered noise (for temp/humidity)
    function lpfNoise(len, base, jitter){
      let x = base;
      const out=[];
      for(let i=0;i<len;i++){
        x = x + (Math.random()-0.5)*jitter;
        out.push(x);
      }
      return out;
    }

    // Metric generators aiming for more realistic shapes
    function genTemp(isSetter, points=48){
      const [min,max] = isSetter ? [37.5,38.0] : [36.8,37.2];
      const target = (min+max)/2;
      const noise = lpfNoise(points, 0, 0.06);
      const out=[]; const now=NOW();
      for(let i=points-1;i>=0;i--){
        const t = now - i*TICK;
        // small drift waves + filtered noise
        const drift = Math.sin(i/8)*0.08 + Math.cos(i/17)*0.04;
        // rare short deviations (door open, load)
        const event = (Math.random()<0.04) ? (Math.random()-0.5)*0.25 : 0;
        const v = clamp(target + drift + noise[points-1-i] + event, min, max);
        out.push([t, Number(v.toFixed(2))]);
      }
      return out;
    }

    function genHum(isSetter, points=48){
      const [min,max] = isSetter ? [50,55] : [65,75];
      const base = (min+max)/2;
      const out=[]; const now=NOW();
      let level = base + (Math.random()-0.5)*2;
      for(let i=points-1;i>=0;i--){
        const t = now - i*TICK;
        // random walk with gentle pullback to base
        level += (Math.random()-0.5)*1.2 + (base - level)*0.05;
        // periodic humidifier pulses
        if(i%12===0) level += 2.0;
        const v = clamp(level, min, max);
        out.push([t, Number(v.toFixed(1))]);
      }
      return out;
    }

    function genCO2(points=48){
      // sawtooth cycles: build-up then ventilation dump
      const out=[]; const now=NOW();
      let level = 0.25; // %
      for(let i=points-1;i>=0;i--){
        const t = now - i*TICK;
        level += 0.01 + Math.random()*0.01;
        if(level>0.48){ level = 0.22 + Math.random()*0.05; }
        out.push([t, Number(level.toFixed(2))]);
      }
      return out;
    }

    function genAir(points=48){
      // step-like fan speeds with small noise
      const steps=[1.1,1.3,1.6,1.8,1.4,1.2];
      const out=[]; const now=NOW();
      for(let i=points-1;i>=0;i--){
        const t = now - i*TICK;
        const s = steps[Math.floor(i/8)%steps.length];
        const v = s + (Math.random()-0.5)*0.07;
        out.push([t, Number(v.toFixed(2))]);
      }
      return out;
    }

    function genTurn(points=48){
      // spikes at turning events per 24h (every 1–2h)
      const out=[]; const now=NOW();
      let next = Math.floor(Math.random()*3)+1; // index of next spike
      for(let i=points-1;i>=0;i--){
        const t = now - i*TICK;
        const spike = (i%next===0) ? (10 + Math.random()*20) : (Math.random()*2);
        out.push([t, Number(spike.toFixed(1))]);
        if(i%next===0){ next = Math.floor(Math.random()*4)+2; }
      }
      return out;
    }

    function genNoise(points=48){
      const out=[]; const now=NOW();
      let level = 52 + (Math.random()-0.5)*4;
      for(let i=points-1;i>=0;i--){
        const t = now - i*TICK;
        level += (Math.random()-0.5)*1.5;
        if(Math.random()<0.05) level += 6 + Math.random()*6; // occasional spike
        const v = clamp(level, 45, 68);
        out.push([t, Number(v.toFixed(1))]);
      }
      return out;
    }
    svgEl.addEventListener('click', (e) => {{
      const target = e.target.closest('.hotspot');
      if (!target) return;
      const key = target.dataset.key;
      const isSetter = key.startsWith('setter-');
      const isTransfer = key.startsWith('parent');
      const titleRole = isTransfer ? 'Transzfer' : (isSetter ? 'Előkeltető' : 'Utókeltető');
      titleEl.textContent = titleRole + ' ' + (key.split('-').pop()) + (isTransfer ? ' — batch statisztika' : ' — gép paraméterei');
      // position near click, keep on-screen
      const width = Math.min(880, window.innerWidth * 0.92);
      const height = Math.min(800, window.innerHeight * 0.86);
      pop.style.width = width + 'px';
      pop.style.maxHeight = height + 'px';
      const margin = 12;
      const px = Math.min(Math.max(e.clientX + margin, margin), window.innerWidth - width - margin);
      const py = Math.min(Math.max(e.clientY + margin, margin), window.innerHeight - height - margin);
      pop.style.left = px + 'px';
      pop.style.top = py + 'px';
      pop.style.display = 'block';
      if (isTransfer) {
        chartsWrap.style.display = 'none';
        statsWrap.style.display = 'block';
        const st = TRANSFER_STATS[key] || null;
        statsWrap.innerHTML = st ? `
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
            <div><div style=\"opacity:.7\">Szállítmányok</div><div style=\"font-size:20px\">$${st.shipments}</div></div>
            <div><div style=\"opacity:.7\">Részarány</div><div style=\"font-size:20px\">$${st.share_pct}%</div></div>
            <div><div style=\"opacity:.7\">Tojás a barnak</div><div style=\"font-size:20px\">$${Math.round(st.eggs_to_barn).toLocaleString('hu-HU')}</div></div>
            <div><div style=\"opacity:.7\">Csibe a barnak</div><div style=\"font-size:20px\">$${Math.round(st.chicks_to_barn).toLocaleString('hu-HU')}</div></div>
            <div><div style=\"opacity:.7\">Batch kora</div><div style=\"font-size:20px\">$${st.age_days ?? 'n/a'} nap</div></div>
          </div>` : '<div>Nincs adat ehhez a szülőpárhoz.</div>';
        return;
      } else { chartsWrap.style.display='grid'; statsWrap.style.display='none'; }
      // Generate metric-specific series and render with nicer colors
      const temp = genTemp(isSetter);
      const hum  = genHum(isSetter);
      const co2  = genCO2();
      const air  = genAir();
      const noise= genNoise();
      const opts = (name, data, unit, color) => ({
        chart: {
          backgroundColor: 'transparent',
          zooming: { type: 'x' }
        },
        title: { text: name, style: { color: '#e6f0ff' } },
        subtitle: { text: (document.ontouchstart === undefined ? 'Click and drag to zoom in' : 'Pinch to zoom in'), style:{ color:'#9aa4b2' } },
        xAxis: { type: 'datetime', labels: { style: { color: '#c8cfdb' } } },
        yAxis: { title: { text: unit, style:{color:'#c8cfdb'} }, gridLineColor: '#223' },
        legend: { enabled: false },
        tooltip: { backgroundColor:'#0f1a2e', borderColor:'#2a3450', style:{color:'#e6f0ff'}, pointFormat:'<b>{{point.y}}</b> '+unit },
        plotOptions: {
          area: {
            fillColor: {
              linearGradient: { x1: 0, y1: 0, x2: 0, y2: 1 },
              stops: [
                [0, Highcharts.color(color).setOpacity(0.25).get('rgba')],
                [1, Highcharts.color(color).setOpacity(0.03).get('rgba')]
              ]
            },
            marker: { radius: 2 },
            lineWidth: 2,
            threshold: null
          }
        },
        series: [{ type:'area', name, data, color }],
        credits: { enabled: false }
      });
      Highcharts.chart(elTemp, opts('Hőmérséklet', temp, '°C', COLORS.temp));
      Highcharts.chart(elHum,  opts('Relatív páratartalom', hum, '%', COLORS.hum));
      Highcharts.chart(elCO2,  opts('CO₂ koncentráció', co2, '%', COLORS.co2));
      Highcharts.chart(elAir,  opts('Légáramlás', air, 'm/s', COLORS.air));
      Highcharts.chart(elNoise,opts('Zaj / rezgésszint', noise, 'dB', COLORS.noise));
      // Show turning chart only for setter
      const elTurn = document.getElementById('chart-turn');
      elTurn.style.display = isSetter ? 'block' : 'none';
      if (isSetter) { const turn = genTurn(); Highcharts.chart(elTurn, opts('Tojás forgatás', turn, 'alkalom/nap', COLORS.turn)); }
    }});
    document.addEventListener('keydown', (ev) => { if (ev.key === 'Escape') pop.style.display = 'none'; });
  </script>
</body>
</html>
""")
    html = tpl.substitute(
        title=f"{bf.barn_id} · {bf.cutoff.date().isoformat()}",
        svg=injected_svg,
        data_json=json.dumps(chart_data, ensure_ascii=False),
        transfer_json=json.dumps(transfer_stats, default=str, ensure_ascii=False),
        hc_js=(Path('Highcharts-12/code/highcharts.js').read_text(encoding='utf-8') if Path('Highcharts-12/code/highcharts.js').exists() else ''),
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
