#!/usr/bin/env python
"""Generate a farm-centric Sankey diagram from the flow log."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Tuple

import polars as pl

DEFAULT_DB = Path("hatchery_events.sqlite")
DEFAULT_FLOW_LOG = Path("flow_log.parquet")
HCHARTS_DIR = Path("Highcharts-12/code")


@dataclass
class CartFlow:
    cart_id: str
    setter_id: str | None
    hatcher_id: str | None
    chicks: float
    losses: float


@dataclass
class SankeyPayload:
    nodes: list[dict[str, object]]
    links: list[dict[str, object]]
    metadata: dict[str, object]


def parse_timestamp(raw: str) -> datetime:
    try:
        return datetime.fromisoformat(raw)
    except ValueError as exc:  # pragma: no cover - defensive guard
        raise SystemExit(f"Invalid ISO timestamp: {raw}") from exc


def decode_json(value: str | None) -> dict[str, object]:
    if not value:
        return {}
    try:
        return json.loads(value)
    except json.JSONDecodeError:  # pragma: no cover - guard for corrupt rows
        return {}


def load_flow(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise SystemExit(f"flow log not found: {path}")
    frame = pl.read_parquet(path)
    return frame.with_columns(
        pl.col("event_ts").str.strptime(pl.Datetime, strict=False).alias("event_dt")
    )


def locate_parent_pair(conn: sqlite3.Connection, shipment_id: str) -> str:
    row = conn.execute(
        """
        SELECT metadata
        FROM events
        WHERE stage = 'inventory' AND status = 'arrived' AND entity_id = ?
        LIMIT 1
        """,
        (shipment_id,),
    ).fetchone()
    if not row:
        return "ismeretlen"
    meta = decode_json(row[0])
    return str(meta.get("parent_pair", "ismeretlen"))


def barn_snapshot(
    flow: pl.DataFrame, farm: str, cutoff: datetime
) -> Tuple[dict[str, dict[str, float]], dict[str, float]]:
    mask = (
        (pl.col("resource_type") == "barn")
        & pl.col("resource_id").str.starts_with(farm)
        & (pl.col("event_dt") <= cutoff)
    )
    relevant = flow.filter(mask)
    if relevant.is_empty():
        raise SystemExit(f"No barn events found for farm '{farm}' on or before {cutoff.date()}")
    latest = relevant.sort("event_dt").group_by("resource_id", maintain_order=True).tail(1)

    barns: dict[str, dict[str, float]] = {}
    shipment_totals: dict[str, float] = defaultdict(float)
    for row in latest.iter_rows(named=True):
        state = decode_json(row["to_state"])
        barn_id = str(row["resource_id"])
        shipments: dict[str, float] = {}
        for shipment_id, amount in state.items():
            qty = float(amount)
            if qty <= 0:
                continue
            shipments[shipment_id] = qty
            shipment_totals[shipment_id] += qty
        if shipments:
            barns[barn_id] = shipments
    if not barns:
        raise SystemExit(f"Farm '{farm}' has no active shipments at {cutoff.isoformat()}")
    return barns, shipment_totals


def extract_cart_flows(shipment_rows: pl.DataFrame) -> dict[str, CartFlow]:
    carts: dict[str, CartFlow] = {}

    for row in shipment_rows.filter(pl.col("resource_type") == "setter_slot").iter_rows(named=True):
        details = decode_json(row["to_state"])
        cart_id = str(details.get("cart_id")) if "cart_id" in details else None
        if not cart_id:
            continue
        entry = carts.get(cart_id)
        if entry is None:
            entry = CartFlow(cart_id, setter_id=str(row["resource_id"]), hatcher_id=None, chicks=0.0, losses=0.0)
            carts[cart_id] = entry
        else:
            entry.setter_id = str(row["resource_id"])

    for row in shipment_rows.filter(pl.col("resource_type") == "hatcher_slot").iter_rows(named=True):
        info_from = decode_json(row["from_state"])
        info_to = decode_json(row["to_state"])
        cart_id = info_from.get("cart_id") or info_to.get("cart_id")
        if not cart_id:
            continue
        cart_id = str(cart_id)
        entry = carts.get(cart_id)
        if entry is None:
            entry = CartFlow(cart_id, setter_id=None, hatcher_id=str(row["resource_id"]), chicks=0.0, losses=0.0)
            carts[cart_id] = entry
        else:
            entry.hatcher_id = str(row["resource_id"])
        entry.chicks = float(info_to.get("chicks", row["quantity"] or 0.0))
        entry.losses = float(info_to.get("losses", 0.0))

    return {cid: flow for cid, flow in carts.items() if flow.chicks > 0}


def build_sankey(
    flow: pl.DataFrame,
    conn: sqlite3.Connection,
    farm: str,
    cutoff: datetime,
) -> SankeyPayload:
    barns, farm_shipments = barn_snapshot(flow, farm, cutoff)

    links: dict[Tuple[str, str], float] = defaultdict(float)
    nodes: dict[str, dict[str, object]] = {}
    shipments_meta: list[dict[str, object]] = []

    def node_id(kind: str, identifier: str) -> str:
        return f"{kind}::{identifier}"

    def ensure_node(kind: str, identifier: str, name: str) -> str:
        key = node_id(kind, identifier)
        if key not in nodes:
            column_map = {"parent": 0, "setter": 1, "hatcher": 2, "barn": 3}
            nodes[key] = {
                "id": key,
                "name": name,
                "type": kind,
                "column": column_map.get(kind, 0),
            }
        return key

    def add_link(source: str, target: str, weight: float) -> None:
        if weight <= 0:
            return
        links[(source, target)] += weight

    for shipment_id, farm_total in farm_shipments.items():
        shipment_rows = flow.filter(pl.col("shipment_id") == shipment_id)
        if shipment_rows.is_empty():
            continue
        carts = extract_cart_flows(shipment_rows)
        if not carts:
            continue
        total_chicks = sum(cart.chicks for cart in carts.values())
        if total_chicks <= 0:
            continue
        parent_pair = locate_parent_pair(conn, shipment_id)
        parent_label = f"Szülőpár {parent_pair.split('-')[-1]}" if parent_pair != "ismeretlen" else "Szülőpár ?"
        parent_node = ensure_node("parent", parent_pair, parent_label)

        shipment_barns = {
            barn_id: shipments[shipment_id]
            for barn_id, shipments in barns.items()
            if shipment_id in shipments
        }
        if not shipment_barns:
            continue
        shipments_meta.append(
            {
                "shipment": shipment_id,
                "parent_pair": parent_pair,
                "chicks_total": total_chicks,
                "chicks_at_farm": farm_total,
                "barns": shipment_barns,
            }
        )

        for cart in carts.values():
            setter_label = f"Előkeltető {cart.setter_id}" if cart.setter_id else "Előkeltető ?"
            setter_node = ensure_node("setter", cart.setter_id or f"missing-{cart.cart_id}", setter_label)
            hatcher_label = f"Utókeltető {cart.hatcher_id}" if cart.hatcher_id else "Utókeltető ?"
            hatcher_node = ensure_node("hatcher", cart.hatcher_id or f"missing-{cart.cart_id}", hatcher_label)

            parent_share = (farm_total / total_chicks) if total_chicks else 0.0
            cart_weight = cart.chicks * parent_share
            add_link(parent_node, setter_node, cart_weight)
            add_link(setter_node, hatcher_node, cart_weight)

            for barn_id, barn_qty in shipment_barns.items():
                barn_share = barn_qty / total_chicks
                barn_weight = cart.chicks * barn_share
                barn_node = ensure_node("barn", barn_id, barn_id)
                add_link(hatcher_node, barn_node, barn_weight)

    node_list = list(nodes.values())
    link_list = [
        {"from": source, "to": target, "weight": weight}
        for (source, target), weight in sorted(links.items())
    ]
    meta = {
        "farm": farm,
        "timestamp": cutoff.isoformat(),
        "shipments": shipments_meta,
    }
    return SankeyPayload(nodes=node_list, links=link_list, metadata=meta)


def write_json(payload: SankeyPayload, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    content = {
        "nodes": payload.nodes,
        "data": payload.links,
        "metadata": payload.metadata,
    }
    output.write_text(json.dumps(content, indent=2, ensure_ascii=False))


def write_html(
    payload: SankeyPayload,
    output: Path,
    title: str | None = None,
    inline_assets: bool = True,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "nodes": payload.nodes,
        "data": payload.links,
        "metadata": payload.metadata,
    }
    chart_options = {
        "title": {"text": title or f"{payload.metadata['farm']} @ {payload.metadata['timestamp']}"},
        "tooltip": {"pointFormat": "<b>{point.weight}</b> csibe"},
        "series": [
            {
                "type": "sankey",
                "name": title or f"{payload.metadata['farm']} @ {payload.metadata['timestamp']}",
                "keys": ["from", "to", "weight"],
                "data": payload.links,
                "nodes": payload.nodes,
                "dataLabels": {
                    "nodeFormat": "{point.name}",
                    "style": {"fontSize": "12px"},
                },
            }
        ],
    }

    hc_path = HCHARTS_DIR / "highcharts.js"
    sankey_module = HCHARTS_DIR / "modules" / "sankey.js"
    if not hc_path.exists() or not sankey_module.exists():
        raise SystemExit("Highcharts sources not found. Please copy Highcharts-12/code into the project root.")

    if inline_assets:
        hc_js = hc_path.read_text(encoding="utf-8")
        sankey_js = sankey_module.read_text(encoding="utf-8")
        loader_block = f"<script>{hc_js}</script>\n  <script>{sankey_js}</script>"
    else:
        rel_hc = Path(os.path.relpath(hc_path, output.parent)).as_posix()
        rel_sankey = Path(os.path.relpath(sankey_module, output.parent)).as_posix()
        loader_block = f'<script src="{rel_hc}"></script>\n  <script src="{rel_sankey}"></script>'

    html = """<!DOCTYPE html>
<html lang=\"hu\">
<head>
  <meta charset=\"utf-8\" />
  <title>{title}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 0; padding: 0; background: #f5f5f5; }}
    #sankey-container {{ min-height: 640px; margin: 0 auto; max-width: 1280px; padding: 24px; background: #ffffff; }}
  </style>
</head>
<body>
  <div id=\"sankey-container\"></div>
  {loader}
  <script>
    const payload = {data_json};
    const options = {options_json};
    Highcharts.chart('sankey-container', options);
  </script>
</body>
</html>
""".format(
        title=chart_options["title"]["text"],
        loader=loader_block,
        data_json=json.dumps(data, ensure_ascii=False),
        options_json=json.dumps(chart_options, ensure_ascii=False),
    )
    output.write_text(html)


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--farm", required=True, help="Farm identifier, e.g. 'Kaba'")
    parser.add_argument("--timestamp", required=True, help="ISO timestamp (YYYY-MM-DD or with time)")
    parser.add_argument("--db-path", default=str(DEFAULT_DB), help="Path to hatchery_events.sqlite")
    parser.add_argument("--flow-log", default=str(DEFAULT_FLOW_LOG), help="Path to flow_log.parquet")
    parser.add_argument("--out-json", help="Optional JSON output path")
    parser.add_argument("--out-html", help="Optional HTML output path")
    parser.add_argument("--title", help="Optional chart title override")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    cutoff = parse_timestamp(args.timestamp)
    flow = load_flow(Path(args.flow_log))

    conn = sqlite3.connect(args.db_path)
    try:
        conn.row_factory = sqlite3.Row
        payload = build_sankey(flow, conn, args.farm, cutoff)
    finally:
        conn.close()

    if args.out_json:
        write_json(payload, Path(args.out_json))
    if args.out_html:
        write_html(payload, Path(args.out_html), args.title)
    if not args.out_json and not args.out_html:
        print(json.dumps({
            "nodes": payload.nodes,
            "data": payload.links,
            "metadata": payload.metadata,
        }, indent=2, ensure_ascii=False))


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
