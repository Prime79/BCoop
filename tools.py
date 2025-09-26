from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple

DEFAULT_DB = Path("hatchery_events.sqlite")


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _load_shipment_sources(conn: sqlite3.Connection) -> Dict[str, str]:
    sources: Dict[str, str] = {}
    for row in conn.execute(
        """
        SELECT entity_id, metadata
        FROM events
        WHERE stage = 'inventory' AND status = 'arrived'
        """
    ):
        meta = json.loads(row["metadata"]) if row["metadata"] else {}
        sources[row["entity_id"]] = meta.get("source", "unknown")
    return sources


def get_farm_mix(
    farm_name: str,
    at_date: str,
    db_path: str | Path = DEFAULT_DB,
) -> Dict[str, Dict[str, float]]:
    """Return current shipment occupancy for a farm on a given date.

    Args:
        farm_name: Name of the grow-out farm (e.g., "Nyirkercs_I").
        at_date: ISO date string (YYYY-MM-DD). The calculation includes
            all events up to the end of that day.
        db_path: Path to the simulation SQLite database.

    Returns:
        A dict with two views:
            - "shipments": remaining chicks per shipment ID
            - "egg_sources": remaining chicks aggregated by egg farm
    """

    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(db_path)

    cutoff = datetime.fromisoformat(at_date) + timedelta(days=1)
    cutoff_iso = cutoff.isoformat()

    conn = _connect(db_path)
    try:
        sources = _load_shipment_sources(conn)

        placements: Dict[str, float] = defaultdict(float)
        query = (
            "SELECT entity_id, stage, status, quantity, metadata, real_ts "
            "FROM events "
            "WHERE real_ts <= ? AND metadata LIKE ? "
            "ORDER BY real_ts, id"
        )
        pattern = f'%"farm": "{farm_name}"%'
        for row in conn.execute(query, (cutoff_iso, pattern)):
            meta = json.loads(row["metadata"]) if row["metadata"] else {}
            shipment_id = row["entity_id"]
            qty = float(row["quantity"] or 0)
            stage = row["stage"]
            status = row["status"]

            if stage == "farm_intake" and status == "placed":
                placements[shipment_id] += qty
            elif stage == "farm_place" and status in {"partial_release", "available"}:
                removed = float(meta.get("removed_amount", qty))
                placements[shipment_id] -= removed

        # Filter active shipments (allowing for minor floating point drift)
        active_shipments = {
            shipment: max(0.0, qty)
            for shipment, qty in placements.items()
            if qty > 1e-6
        }

        by_source: Dict[str, float] = defaultdict(float)
        for shipment, qty in active_shipments.items():
            source = sources.get(shipment, "unknown")
            by_source[source] += qty

        return {
            "shipments": dict(active_shipments),
            "egg_sources": dict(by_source),
        }
    finally:
        conn.close()
