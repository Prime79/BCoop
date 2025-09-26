#!/usr/bin/env python
"""Generate Sankey flow data for batches or barn places."""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


DEFAULT_DB = Path("hatchery_events.sqlite")


@dataclass(frozen=True)
class FlowResult:
    nodes: List[str]
    links: List[Tuple[str, str, float]]
    metadata: Dict[str, Any]


def locate_db(path: Optional[str]) -> Path:
    if path:
        candidate = Path(path)
        if not candidate.exists():
            raise FileNotFoundError(f"Database not found at {candidate}")
        return candidate
    if DEFAULT_DB.exists():
        return DEFAULT_DB
    raise FileNotFoundError(
        "Could not locate hatchery_events.sqlite; pass --db-path explicitly."
    )


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_dataframe(
    conn: sqlite3.Connection, query: str, params: Iterable[Any] = ()
) -> pd.DataFrame:
    cur = conn.execute(query, tuple(params))
    frame = pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description])
    return frame


def load_batch_flow(conn: sqlite3.Connection, batch_id: str) -> FlowResult:
    rows = fetch_dataframe(
        conn,
        """
        SELECT stage, status, quantity, metadata
        FROM events
        WHERE entity_id = ?
        ORDER BY id
        """,
        (batch_id,),
    )
    if rows.empty:
        raise ValueError(f"No records found for batch {batch_id}")

    def lookup(stage: str, status: str) -> Optional[pd.Series]:
        subset = rows[(rows.stage == stage) & (rows.status == status)]
        if subset.empty:
            return None
        return subset.iloc[0]

    nodes: List[str] = []
    links: List[Tuple[str, str, float]] = []

    def add_link(source: str, target: str, weight: float) -> None:
        if weight <= 0:
            return
        nodes.extend([source, target])
        links.append((source, target, float(weight)))

    inventory = lookup("inventory", "arrived")
    eggs = float(inventory.quantity) if inventory is not None else 0.0

    screened = lookup("xray_screen", "discarded")
    discarded = float(screened.quantity) if screened is not None else 0.0
    fertile = eggs - discarded

    hatched = lookup("hatch_room", "hatched")
    hatch_loss = lookup("hatch_room", "hatched")
    hatch_loaded = lookup("hatch_room", "loaded")
    chicks = float(hatched.quantity) if hatched is not None else 0.0
    losses = 0.0
    if hatch_loaded is not None:
        losses = float(hatch_loaded.quantity) - chicks

    grow = lookup("grow_out", "completed")
    survivors = float(grow.quantity) if grow is not None else 0.0
    farm_losses = chicks - survivors

    add_link("Inventory", "Pre-Hatch", eggs)
    add_link("Pre-Hatch", "Screened Fertile", fertile)
    add_link("Pre-Hatch", "Screened Out", discarded)
    add_link("Screened Fertile", "Hatching", fertile)
    add_link("Hatching", "Embryo Loss", losses)
    add_link("Hatching", "Chicks", chicks)
    add_link("Chicks", "Processing", chicks)
    add_link("Processing", "Transport", chicks)
    add_link("Transport", "Farm Placement", chicks)
    add_link("Farm Placement", "Farm Loss", farm_losses)
    add_link("Farm Placement", "Slaughter Ready", survivors)

    metadata: Dict[str, Any] = {
        "batch_id": batch_id,
        "inventory": inventory.metadata if inventory is not None else {},
        "pre_hatch": json.loads(lookup("pre_hatch", "loaded").metadata)
        if lookup("pre_hatch", "loaded") is not None
        else {},
        "hatch_room": json.loads(hatch_loaded.metadata) if hatch_loaded is not None else {},
        "farm_intake": json.loads(lookup("farm_intake", "placed").metadata)
        if lookup("farm_intake", "placed") is not None
        else {},
    }

    return FlowResult(nodes=nodes, links=links, metadata=metadata)


def load_barn_flow(conn: sqlite3.Connection, place_id: str) -> FlowResult:
    intake_rows = fetch_dataframe(
        conn,
        """
        SELECT entity_id, quantity, metadata
        FROM events
        WHERE stage = 'farm_intake' AND metadata LIKE ?
        """,
        (f'%"place_id": "{place_id}"%',),
    )
    if intake_rows.empty:
        raise ValueError(f"No farm intake records found for place {place_id}")

    batches = intake_rows["entity_id"].unique()
    mix = {
        row.entity_id: json.loads(row.metadata).get("occupants", {}).get(row.entity_id, row.quantity)
        for _, row in intake_rows.iterrows()
    }

    nodes: List[str] = []
    links: List[Tuple[str, str, float]] = []

    def add_link(source: str, target: str, weight: float) -> None:
        if weight <= 0:
            return
        nodes.extend([source, target])
        links.append((source, target, float(weight)))

    metadata: Dict[str, Any] = {"place_id": place_id, "batches": []}

    for batch_id in batches:
        batch_flow = load_batch_flow(conn, batch_id)
        weight = float(mix.get(batch_id, 0))
        add_link(f"Barn {place_id}", batch_id, weight)
        metadata["batches"].append({"batch_id": batch_id, "weight": weight, "metadata": batch_flow.metadata})
        for source, target, w in batch_flow.links:
            add_link(batch_id + ":" + source, batch_id + ":" + target, w)

    return FlowResult(nodes=nodes, links=links, metadata=metadata)


def to_highcharts(flow: FlowResult) -> Dict[str, Any]:
    node_index: Dict[str, int] = {}
    nodes = []
    for name in flow.nodes:
        if name not in node_index:
            node_index[name] = len(nodes)
            nodes.append({"id": name})

    links = [
        {
            "from": source,
            "to": target,
            "weight": weight,
        }
        for source, target, weight in flow.links
    ]
    return {"nodes": nodes, "data": links, "metadata": flow.metadata}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["batch", "barn"], help="Flow type to generate")
    parser.add_argument("identifier", help="Batch ID or barn place id")
    parser.add_argument("--db-path", help="Path to hatchery_events.sqlite")
    parser.add_argument("--output", help="Optional file to write JSON payload")
    args = parser.parse_args()

    conn = connect(locate_db(args.db_path))
    try:
        if args.mode == "batch":
            flow = load_batch_flow(conn, args.identifier)
        else:
            flow = load_barn_flow(conn, args.identifier)
        payload = to_highcharts(flow)
    finally:
        conn.close()

    if args.output:
        Path(args.output).write_text(json.dumps(payload, indent=2))
    else:
        print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
