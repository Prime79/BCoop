#!/usr/bin/env python3
"""Build an interactive Highcharts Gantt dashboard for barn occupancy cycles.

The script consumes the simulation `flow_log.parquet` file, derives estimated
occupancy and cleaning windows per barn, and emits a standalone HTML document
styled to match the existing barnflow visuals (dark navy palette).

Usage (defaults shown):

    ./generate_barn_gantt.py \
        --flow-log flow_log.parquet \
        --out planing/barn_gantt.html

Optional arguments let you override the assumed grow-out or cleaning durations
without re-running the simulation.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import polars as pl

from simulation.config import SimulationConfig

MS_PER_SECOND = 1000
MS_PER_DAY = 86_400 * MS_PER_SECOND


@dataclass(frozen=True)
class CycleSpec:
    """Configuration for the inferred barn cycle timeline."""

    grow_days: float
    cleaning_days: float

    @property
    def grow_ms(self) -> int:
        return int(self.grow_days * MS_PER_DAY)

    @property
    def cleaning_ms(self) -> int:
        return int(self.cleaning_days * MS_PER_DAY)


@dataclass
class TaskRecord:
    """Highcharts-compatible record for an individual Gantt task."""

    id: str
    name: str
    start: int
    end: int
    y: int
    color: str
    dependency: str | None = None
    custom: Dict[str, object] | None = None

    def to_dict(self) -> Dict[str, object]:
        data: Dict[str, object] = {
            "id": self.id,
            "name": self.name,
            "start": self.start,
            "end": self.end,
            "y": self.y,
            "color": self.color,
        }
        if self.dependency:
            data["dependency"] = self.dependency
        if self.custom:
            data["custom"] = self.custom
        return data


def _loads_state(raw: str | None) -> Dict[str, float]:
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    result: Dict[str, float] = {}
    for key, value in payload.items():
        try:
            result[str(key)] = float(value or 0.0)
        except (TypeError, ValueError):
            continue
    return result


def _format_shipments(state: Dict[str, float]) -> List[Dict[str, object]]:
    if not state:
        return []
    items = sorted(state.items(), key=lambda kv: kv[1], reverse=True)
    top = items[:8]
    return [
        {
            "shipment": sid,
            "quantity": int(round(qty)),
        }
        for sid, qty in top
    ]


def load_barn_events(path: Path) -> pl.DataFrame:
    df = pl.read_parquet(
        path,
        columns=[
            "resource_id",
            "resource_type",
            "event_ts",
            "to_state",
        ],
    )
    df = df.filter(pl.col("resource_type") == "barn")
    if df.is_empty():
        raise SystemExit("No barn events found in flow log.")
    return df.with_columns(
        pl.col("event_ts").str.strptime(pl.Datetime, strict=False).alias("event_dt")
    ).sort(["resource_id", "event_dt"])


def derive_tasks(flow: pl.DataFrame, cycle: CycleSpec) -> Tuple[List[TaskRecord], List[str]]:
    tasks: List[TaskRecord] = []
    barn_ids: List[str] = []
    palette = {
        "grow": "#5b8def",      # azure blue (matches existing charts)
        "clean": "#ffa552",     # warm orange accent
    }

    grouped = flow.group_by("resource_id", maintain_order=True)
    for barn_index, (barn_id, subset) in enumerate(grouped):
        barn_ids.append(str(barn_id))
        rows = subset.sort("event_dt").iter_rows(named=True)
        for seq, row in enumerate(rows):
            event_dt: datetime | None = row.get("event_dt")
            if event_dt is None:
                continue
            start_ms = int(event_dt.timestamp() * MS_PER_SECOND)
            shipments = _loads_state(row.get("to_state"))
            total_qty = int(round(sum(shipments.values())))
            shipments_list = _format_shipments(shipments)

            next_start: datetime | None = None
            if seq + 1 < subset.height:
                next_event_ts = subset.row(seq + 1, named=True)["event_dt"]
                if isinstance(next_event_ts, datetime):
                    next_start = next_event_ts

            grow_end_dt = event_dt + timedelta(milliseconds=cycle.grow_ms)
            if next_start and grow_end_dt > next_start:
                grow_end_dt = next_start
            grow_end_ms = int(grow_end_dt.timestamp() * MS_PER_SECOND)
            if grow_end_ms <= start_ms:
                grow_end_ms = start_ms + cycle.grow_ms // 4

            grow_id = f"{barn_id}-cycle{seq}-grow"
            tasks.append(
                TaskRecord(
                    id=grow_id,
                    name="Nevelés",
                    start=start_ms,
                    end=grow_end_ms,
                    y=barn_index,
                    color=palette["grow"],
                    custom={
                        "barn": barn_id,
                        "stage": "grow",
                        "sequence": seq + 1,
                        "totalQuantity": total_qty,
                        "shipments": shipments_list,
                        "assumedDays": cycle.grow_days,
                    },
                )
            )

            cleaning_start_dt = datetime.fromtimestamp(grow_end_ms / MS_PER_SECOND)
            cleaning_end_dt = cleaning_start_dt + timedelta(milliseconds=cycle.cleaning_ms)
            if next_start and cleaning_end_dt > next_start:
                cleaning_end_dt = next_start
            cleaning_end_ms = int(cleaning_end_dt.timestamp() * MS_PER_SECOND)
            if cleaning_end_ms <= grow_end_ms:
                continue

            tasks.append(
                TaskRecord(
                    id=f"{barn_id}-cycle{seq}-clean",
                    name="Fertőtlenítés",
                    start=grow_end_ms,
                    end=cleaning_end_ms,
                    y=barn_index,
                    color=palette["clean"],
                    dependency=grow_id,
                    custom={
                        "barn": barn_id,
                        "stage": "clean",
                        "sequence": seq + 1,
                        "assumedDays": cycle.cleaning_days,
                    },
                )
            )
    return tasks, barn_ids


def build_html(tasks: Iterable[TaskRecord], barns: List[str], cycle: CycleSpec) -> str:
    gantt_js = Path("Highcharts-Gantt-12/code/highcharts-gantt.js")
    accessibility_js = Path("Highcharts-12/code/modules/accessibility.js")
    exporting_js = Path("Highcharts-12/code/modules/exporting.js")
    draggable_js = Path("Highcharts-12/code/modules/draggable-points.js")

    js_assets = {
        "highcharts": gantt_js.read_text(encoding="utf-8") if gantt_js.exists() else "",
        "accessibility": accessibility_js.read_text(encoding="utf-8") if accessibility_js.exists() else "",
        "exporting": exporting_js.read_text(encoding="utf-8") if exporting_js.exists() else "",
        "draggablePoints": draggable_js.read_text(encoding="utf-8") if draggable_js.exists() else "",
    }

    data_json = json.dumps([task.to_dict() for task in tasks], ensure_ascii=False)
    categories_json = json.dumps(barns, ensure_ascii=False)

    meta: Dict[str, object] = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "grow_days": cycle.grow_days,
        "cleaning_days": cycle.cleaning_days,
    }
    meta_json = json.dumps(meta, ensure_ascii=False)

    return f"""<!DOCTYPE html>
<html lang=\"hu\">
<head>
  <meta charset=\"utf-8\" />
  <title>Istállók terhelése · Gantt</title>
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <style>
    :root {{
      color-scheme: dark;
      --bg: #0b1220;
      --panel: #0e172a;
      --accent: #5b8def;
      --accent-clean: #ffa552;
      --text: #e6f0ff;
      --muted: #9aa4b2;
      --grid: #1b2437;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; background: var(--bg); color: var(--text); font-family: 'Inter', 'Segoe UI', system-ui, sans-serif; }}
    header {{ padding: 18px 24px 12px; border-bottom: 1px solid #223; background: rgba(11, 18, 32, 0.94); position: sticky; top: 0; z-index: 9; backdrop-filter: blur(12px); }}
    header h1 {{ margin: 0; font-size: 24px; font-weight: 600; }}
    header p {{ margin: 6px 0 0; color: var(--muted); font-size: 14px; }}
    main {{ padding: 20px clamp(16px, 4vw, 48px); display: grid; gap: 20px; }}
    .card {{ background: var(--panel); border: 1px solid #223; border-radius: 14px; padding: clamp(16px, 3vw, 28px); box-shadow: 0 20px 60px rgba(0, 0, 0, 0.35); }}
    #gantt-container {{ height: clamp(600px, 60vh, 1000px); min-height: 520px; }}
    .legend {{ display: flex; gap: 18px; flex-wrap: wrap; font-size: 14px; color: var(--muted); }}
    .legend span::before {{ content: ''; display: inline-block; width: 14px; height: 14px; margin-right: 6px; border-radius: 4px; vertical-align: middle; }}
    .legend .grow::before {{ background: var(--accent); }}
    .legend .clean::before {{ background: var(--accent-clean); }}
    footer {{ text-align: center; font-size: 13px; color: #73809a; padding: 24px 16px 32px; }}
    a {{ color: #5b8def; }}
  </style>
</head>
<body>
  <header>
  <h1>Istállók terhelése · Gantt nézet</h1>
  <p>Feltöltés → nevelés → fertőtlenítés ciklusok becslése a szimulációs napló alapján. Frissítve: {meta['generated']}</p>
  </header>
  <main>
    <section class=\"card\">
      <div class=\"legend\">
        <span class=\"grow\">Nevelés (~{cycle.grow_days:.0f} nap)</span>
        <span class=\"clean\">Fertőtlenítés (~{cycle.cleaning_days:.0f} nap)</span>
        <span>Színek illesztve a barnflow panelekhez</span>
      </div>
      <div id=\"gantt-container\"></div>
    </section>
  </main>
  <footer>Generálta: barnflow Gantt, Highcharts Gantt motorral.</footer>

  <script>{js_assets['highcharts']}</script>
  <script>{js_assets['accessibility']}</script>
  <script>{js_assets['exporting']}</script>
  <script>{js_assets['draggablePoints']}</script>
  <script>
    const GANTT_DATA = {data_json};
    const BARN_CATEGORIES = {categories_json};
    const META = {meta_json};
    const MS_PER_DAY = 86400000;

    const THEME = {{
  chart: {{ backgroundColor: 'transparent', style: {{ fontFamily: 'Inter, Segoe UI, sans-serif' }} }},
      title: {{ style: {{ color: '#e6f0ff' }} }},
      subtitle: {{ style: {{ color: '#9aa4b2' }} }},
      xAxis: {{
        labels: {{ style: {{ color: '#c8cfdb' }} }},
        gridLineColor: '#1b2437',
        lineColor: '#223'
      }},
      yAxis: {{
        gridLineColor: '#1b2437',
        labels: {{ style: {{ color: '#c8cfdb' }} }},
        lineColor: '#223'
      }},
      navigator: {{
        maskFill: 'rgba(91, 141, 239, 0.25)',
        outlineColor: '#2a3450',
        series: {{ color: '#5b8def', lineColor: '#5b8def' }},
        xAxis: {{ gridLineColor: '#223' }}
      }},
      rangeSelector: {{
        buttonTheme: {{
          fill: 'rgba(17,28,45,0.8)',
          stroke: '#2a3450',
          style: {{ color: '#c8cfdb' }},
          states: {{ select: {{ fill: '#5b8def', style: {{ color: '#0b1220' }} }} }}
        }},
        inputStyle: {{ color: '#e6f0ff', backgroundColor: '#111c2d' }},
        labelStyle: {{ color: '#9aa4b2' }}
      }},
      tooltip: {{
        backgroundColor: '#0f1a2e',
        borderColor: '#2a3450',
        style: {{ color: '#e6f0ff' }}
      }},
      scrollbar: {{ trackBorderColor: '#223', barBackgroundColor: '#2a3450' }},
      legend: {{ itemStyle: {{ color: '#e6f0ff' }}, itemHoverStyle: {{ color: '#ffffff' }} }},
      plotOptions: {{
        series: {{
          dataLabels: {{ color: '#e6f0ff', style: {{ textOutline: 'none', fontWeight: '400' }} }},
          borderWidth: 0,
        }}
      }}
    }};

    Highcharts.setOptions(THEME);

    const tooltipFormatter = function() {{
      const point = this.point;
      const custom = point.custom || {{}};
      const shipments = (custom.shipments || []).map(item => `
        <div style="display:flex; justify-content:space-between; gap:12px">
          <span style="opacity:0.75">${{item.shipment}}</span>
          <span>${{item.quantity.toLocaleString('hu-HU')}} csibe</span>
        </div>
      `).join('');
      const stage = custom.stage === 'clean' ? 'Fertőtlenítés' : 'Nevelés';
      const qtyLine = typeof custom.totalQuantity === 'number' ? `<div style="margin-top:6px">Összesen <strong>${{custom.totalQuantity.toLocaleString('hu-HU')}}</strong> csibe</div>` : '';
      const duration = ((point.end - point.start) / 86400000).toFixed(1);
      const base = `
        <div style="min-width:240px">
          <div style="font-size:16px; font-weight:600; margin-bottom:4px">${{stage}}</div>
          <div style="opacity:0.72; margin-bottom:4px">${{point.series.yAxis.categories[point.y]}} · ciklus #${{custom.sequence || '?'}} </div>
          <div style="margin-bottom:6px">Időtartam: <strong>${{duration}}</strong> nap</div>
          ${{qtyLine}}
        </div>
      `;
      if (!shipments) return base;
      return base + (shipments ? `<div style="margin-top:10px; font-size:13px; opacity:0.75">Szállítmány összetétel</div>${{shipments}}` : '');
    }};

    Highcharts.ganttChart('gantt-container', {{
      chart: {{
        backgroundColor: 'transparent',
        spacing: [12, 18, 18, 18],
        zooming: {{ type: 'x' }},
        panning: {{ enabled: true, type: 'x' }},
        panKey: 'shift'
      }},
  title: {{ text: 'Istállók ciklusai' }},
      subtitle: {{ text: `Átlagos nevelési idő: ~${{META.grow_days.toFixed(0)}} nap · Fertőtlenítés: ~${{META.cleaning_days.toFixed(0)}} nap` }},
      series: [{{
  name: 'Istállók',
        type: 'gantt',
        data: GANTT_DATA,
        animation: {{ duration: 600 }},
        dataLabels: {{
          enabled: true,
          format: '{{point.name}}',
          style: {{ fontSize: '12px', color: '#e6f0ff' }}
        }},
        dragDrop: {{
          draggableX: true,
          dragPrecisionX: MS_PER_DAY,
          dragMinX: Date.UTC(2023, 0, 1),
          dragMaxX: Date.UTC(2035, 11, 31),
          liveRedraw: false
        }},
        point: {{
          events: {{
            drop: function (e) {{
              const shiftDays = ((e.newPoint.start - e.origin.start) / MS_PER_DAY).toFixed(1);
              if (window?.console) {{
                console.info(`Istálló ${{e.newPoint.custom?.barn || '?'}} · "${{e.newPoint.name}}" mozgatva ${{shiftDays}} nappal`);
              }}
            }}
          }}
        }}
      }}],
      tooltip: {{ useHTML: true, formatter: tooltipFormatter }},
      yAxis: {{
        type: 'category',
        categories: BARN_CATEGORIES,
        gridLineColor: '#1b2437',
        gridLineWidth: 1,
        min: 0,
        scrollbar: {{ enabled: true }}
      }},
      xAxis: {{
        currentDateIndicator: {{ enabled: true, color: '#00ddeb' }},
        tickColor: '#2a3450',
        gridLineColor: '#1b2437',
        crosshair: {{ color: '#2a3450' }},
        scrollbar: {{ enabled: true }}
      }},
      navigator: {{ enabled: true }},
      rangeSelector: {{
        enabled: true,
        selected: 1,
        buttons: [
          {{ type: 'month', count: 1, text: '1hó' }},
          {{ type: 'month', count: 3, text: '3hó' }},
          {{ type: 'all', text: 'Összes' }}
        ]
      }},
      credits: {{ enabled: false }}
    }});
  </script>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a Highcharts Gantt dashboard for barn occupancy cycles.")
    parser.add_argument("--flow-log", type=Path, default=Path("flow_log.parquet"))
    parser.add_argument("--out", type=Path, default=Path("planing/barn_gantt.html"))
    parser.add_argument("--grow-days", type=float, default=None, help="Override grow-out duration (days).")
    parser.add_argument("--cleaning-days", type=float, default=None, help="Override cleaning duration (days).")
    args = parser.parse_args()

    cfg = SimulationConfig()
    default_grow_days = sum(cfg.grow_out_days_range) / 2.0
    cycle = CycleSpec(
        grow_days=args.grow_days if args.grow_days is not None else default_grow_days,
        cleaning_days=args.cleaning_days if args.cleaning_days is not None else float(cfg.cleaning_days),
    )

    flow = load_barn_events(args.flow_log)
    tasks, barns = derive_tasks(flow, cycle)

    output_html = build_html(tasks, barns, cycle)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(output_html, encoding="utf-8")
    print(f"Wrote {args.out} ({len(tasks)} tasks · {len(barns)} barns)")


if __name__ == "__main__":
    main()
