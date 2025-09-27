from __future__ import annotations
"""Barn‑centric flow analysis from Parquet only.

This module reconstructs the flows that reach a given barn using only the
append‑only Parquet log written by `FlowWriter`. It aggregates:

- Shipments and their cart‑level contributions (setter/hatcher machines)
- Barn state changes and truck arrivals
- A compact timeline per shipment and a final snapshot for the barn

No SQLite dependency: parent pairs are extracted from `metadata` of
`resource_type == "inventory"` rows inside the Parquet file.
"""

import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl

sqlite3 = None  # SQLite no longer required for analysis; parent pairs read from flow log

__all__ = [
    "BarnFlowBuilder",
    "BarnFlow",
    "ShipmentFlow",
    "CartContribution",
    "ShipmentTimeline",
    "BarnStateChange",
]


@dataclass
class CartContribution:
    """Summary of a cart's path through setter and hatcher stages.

    Attributes:
        cart_id: Logical cart identifier (shipment‑scoped).
        setter_id: Setter machine slot that hosted this cart.
        hatcher_id: Hatcher machine slot that hosted this cart.
        eggs: Eggs loaded into the cart at setter.
        chicks: Chicks produced after hatching.
        losses: Hatching losses.
        barn_chicks: Chicks attributed to the target barn from this cart.
        barn_eggs: Eggs attributed to the target barn from this cart.
    """

    cart_id: str
    setter_id: Optional[str]
    hatcher_id: Optional[str]
    eggs: float
    chicks: float
    losses: float
    barn_chicks: float
    barn_eggs: float


@dataclass
class ShipmentTimeline:
    """Key timestamps for a shipment across the production process.

    Missing values indicate unavailable or not‑yet‑observed events in the
    Parquet stream.
    """

    inventory_ready: Optional[datetime] = None
    setter_load: Optional[datetime] = None
    setter_release: Optional[datetime] = None
    hatcher_load: Optional[datetime] = None
    hatcher_ready: Optional[datetime] = None
    processing_complete: Optional[datetime] = None
    loading_complete: Optional[datetime] = None
    truck_departure: Optional[datetime] = None
    truck_arrival: Optional[datetime] = None
    barn_arrival: Optional[datetime] = None


@dataclass
class ShipmentFlow:
    """Aggregated details for a shipment that delivered chicks to the barn.

    Includes cart‑level contributions scaled to the quantity actually placed in
    the target barn.
    """

    shipment_id: str
    parent_pair: str
    barn_quantity: float
    total_chicks: float
    cart_contributions: List[CartContribution]
    timeline: ShipmentTimeline
    trucks: List[str] = field(default_factory=list)


@dataclass
class BarnStateChange:
    """State change for the barn occupancy, grouped by event timestamp."""

    timestamp: datetime
    shipment_deltas: Dict[str, float]
    state_after: Dict[str, float]
    truck_id: Optional[str] = None


@dataclass
class BarnFlow:
    """Result payload describing flows and state changes for a barn."""

    barn_id: str
    cutoff: datetime
    shipments: List[ShipmentFlow]
    state_events: List[BarnStateChange]
    arrivals: Dict[str, float]
    departures: Dict[str, float]
    current_state: Dict[str, float]

    @property
    def total_arrived(self) -> float:
        return sum(self.arrivals.values())

    @property
    def total_departed(self) -> float:
        return sum(self.departures.values())

    @property
    def current_occupancy(self) -> float:
        return sum(self.current_state.values())


class BarnFlowBuilder:
    """High‑level helper that extracts barn‑specific flow information.

    Reads only the Parquet flow log, and builds a structured view for a barn
    at a cutoff time (or latest available). Parent pairs are taken from
    `metadata` on inventory rows.
    """

    def __init__(
        self,
        flow_log: Path,
        db_path: Path,
    ) -> None:
        self.flow_log = Path(flow_log)
        self.db_path = Path(db_path)

    def build(self, barn_id: str, cutoff: Optional[datetime] = None) -> BarnFlow:
        """Build a `BarnFlow` snapshot for the given barn.

        Args:
            barn_id: Target barn identifier (e.g. "Kaba-barn-01").
            cutoff: Optional ISO timestamp to limit events.
        """
        flow = _load_flow(self.flow_log)
        state_events = _compute_barn_state_changes(flow, barn_id, cutoff)
        if not state_events:
            raise ValueError(f"No barn events found for {barn_id} within the selected range")

        arrivals: Dict[str, float] = defaultdict(float)
        departures: Dict[str, float] = defaultdict(float)
        first_arrival: Dict[str, datetime] = {}
        trucks_by_shipment: Dict[str, set[str]] = defaultdict(set)

        for event in state_events:
            for shipment_id, delta in event.shipment_deltas.items():
                if delta > 0:
                    arrivals[shipment_id] += delta
                    first_arrival.setdefault(shipment_id, event.timestamp)
                    if event.truck_id:
                        trucks_by_shipment[shipment_id].add(event.truck_id)
                elif delta < 0:
                    departures[shipment_id] += abs(delta)
            if event.truck_id:
                for shipment_id in event.shipment_deltas:
                    trucks_by_shipment[shipment_id].add(event.truck_id)

        shipments_of_interest = sorted(arrivals.keys())
        if not shipments_of_interest:
            raise ValueError(f"Barn {barn_id} did not receive any shipments in the selected range")

        shipments: List[ShipmentFlow] = []
        parent_map = _extract_parent_map(flow)
        for shipment_id in shipments_of_interest:
            shipment_rows = flow.filter(pl.col("shipment_id") == shipment_id)
            carts = _extract_cart_flows(shipment_rows)
            if not carts:
                continue
            total_chicks = sum(cart.chicks for cart in carts.values())
            barn_qty = arrivals[shipment_id]
            barn_share = (barn_qty / total_chicks) if total_chicks else 0.0
            contributions = [
                CartContribution(
                    cart_id=cart.cart_id,
                    setter_id=cart.setter_id,
                    hatcher_id=cart.hatcher_id,
                    eggs=cart.eggs,
                    chicks=cart.chicks,
                    losses=cart.losses,
                    barn_chicks=cart.chicks * barn_share,
                    barn_eggs=cart.eggs * barn_share,
                )
                for cart in carts.values()
                if cart.chicks > 0
            ]
            parent_pair = parent_map.get(shipment_id, "ismeretlen")
            timeline = _summarise_shipment_timeline(
                shipment_rows,
                barn_arrival=first_arrival.get(shipment_id),
            )
            shipments.append(
                ShipmentFlow(
                    shipment_id=shipment_id,
                    parent_pair=parent_pair,
                    barn_quantity=barn_qty,
                    total_chicks=total_chicks,
                    cart_contributions=contributions,
                    timeline=timeline,
                    trucks=sorted(trucks_by_shipment.get(shipment_id, set())),
                )
            )

        cutoff_dt = cutoff or state_events[-1].timestamp
        current_state = state_events[-1].state_after if state_events else {}

        return BarnFlow(
            barn_id=barn_id,
            cutoff=cutoff_dt,
            shipments=shipments,
            state_events=state_events,
            arrivals=dict(arrivals),
            departures=dict(departures),
            current_state=current_state,
        )


def _load_flow(path: Path) -> pl.DataFrame:
    """Load the Parquet flow log and derive a `event_dt` datetime column."""
    if not path.exists():
        raise FileNotFoundError(f"flow log not found: {path}")
    frame = pl.read_parquet(path)
    return frame.with_columns(
        pl.col("event_ts").str.strptime(pl.Datetime, strict=False).alias("event_dt")
    )


def _decode_json(value: Optional[str]) -> Dict[str, object]:
    """Lenient JSON decode helper (returns empty dict on failure)."""
    if not value:
        return {}
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return {}


@dataclass
class _CartFlow:
    """Internal, minimal cart record used during extraction."""
    cart_id: str
    setter_id: Optional[str]
    hatcher_id: Optional[str]
    eggs: float
    chicks: float
    losses: float


def _extract_cart_flows(shipment_rows: pl.DataFrame) -> Dict[str, _CartFlow]:
    """Extract cart‑level setter/hatcher info and hatch outcomes for a shipment."""
    carts: Dict[str, _CartFlow] = {}

    for row in shipment_rows.filter(pl.col("resource_type") == "setter_slot").iter_rows(named=True):
        details = _decode_json(row["to_state"])
        cart_id = details.get("cart_id")
        if not cart_id:
            continue
        cart_id = str(cart_id)
        eggs = float(details.get("eggs", 0.0))
        entry = carts.get(cart_id)
        if entry is None:
            entry = _CartFlow(
                cart_id=cart_id,
                setter_id=str(row["resource_id"]),
                hatcher_id=None,
                eggs=eggs,
                chicks=0.0,
                losses=0.0,
            )
            carts[cart_id] = entry
        else:
            entry.setter_id = str(row["resource_id"])
            if eggs and not entry.eggs:
                entry.eggs = eggs

    for row in shipment_rows.filter(pl.col("resource_type") == "hatcher_slot").iter_rows(named=True):
        info_from = _decode_json(row["from_state"])
        info_to = _decode_json(row["to_state"])
        cart_id = info_from.get("cart_id") or info_to.get("cart_id")
        if not cart_id:
            continue
        cart_id = str(cart_id)
        entry = carts.get(cart_id)
        if entry is None:
            entry = _CartFlow(
                cart_id=cart_id,
                setter_id=None,
                hatcher_id=str(row["resource_id"]),
                eggs=float(info_from.get("eggs", 0.0)),
                chicks=0.0,
                losses=0.0,
            )
            carts[cart_id] = entry
        else:
            entry.hatcher_id = str(row["resource_id"])
        chicks = float(info_to.get("chicks", row.get("quantity", 0.0) or 0.0))
        losses = float(info_to.get("losses", 0.0))
        if chicks:
            entry.chicks = chicks
        if losses:
            entry.losses = losses
        if not entry.eggs:
            eggs = float(info_from.get("eggs", 0.0))
            if eggs:
                entry.eggs = eggs

    return carts


def _extract_parent_map(flow: pl.DataFrame) -> Dict[str, str]:
    """Build `shipment_id -> parent_pair` map from flow metadata (Parquet only)."""
    pairs: Dict[str, str] = {}
    subset = flow.filter(pl.col("resource_type") == "inventory").select([
        pl.col("shipment_id"),
        pl.col("metadata"),
    ])
    for row in subset.iter_rows(named=True):
        meta = _decode_json(row["metadata"]) if row["metadata"] else {}
        pp = meta.get("parent_pair")
        if pp:
            pairs[str(row["shipment_id"])] = str(pp)
    return pairs


def _summarise_shipment_timeline(
    shipment_rows: pl.DataFrame,
    barn_arrival: Optional[datetime],
) -> ShipmentTimeline:
    timeline = ShipmentTimeline(barn_arrival=barn_arrival)
    ordered = shipment_rows.sort("event_dt")

    for row in ordered.iter_rows(named=True):
        stage = row["resource_type"]
        ts = row["event_dt"]
        to_state = _decode_json(row["to_state"])

        if stage == "inventory" and timeline.inventory_ready is None:
            timeline.inventory_ready = ts
        elif stage == "setter_slot":
            if "eggs" in to_state and timeline.setter_load is None:
                timeline.setter_load = ts
            status = to_state.get("status")
            if status == "released" and timeline.setter_release is None:
                timeline.setter_release = ts
        elif stage == "hatcher_slot":
            if to_state.get("cart_id") and timeline.hatcher_load is None:
                timeline.hatcher_load = ts
            if "chicks" in to_state and timeline.hatcher_ready is None:
                timeline.hatcher_ready = ts
        elif stage == "process":
            if to_state.get("status") == "processed" and timeline.processing_complete is None:
                timeline.processing_complete = ts
        elif stage == "logistics":
            if to_state.get("trucks") and timeline.loading_complete is None:
                timeline.loading_complete = ts
        elif stage == "truck":
            status = to_state.get("status")
            if status == "in_transit" and timeline.truck_departure is None:
                timeline.truck_departure = ts
            elif status == "arrived" and timeline.truck_arrival is None:
                timeline.truck_arrival = ts

    return timeline


def _compute_barn_state_changes(
    flow: pl.DataFrame,
    barn_id: str,
    cutoff: Optional[datetime],
) -> List[BarnStateChange]:
    mask = (pl.col("resource_type") == "barn") & (pl.col("resource_id") == barn_id)
    subset = flow.filter(mask)
    if cutoff is not None:
        subset = subset.filter(pl.col("event_dt") <= cutoff)
    if subset.is_empty():
        return []

    ordered = subset.sort("event_dt")
    events: List[BarnStateChange] = []
    previous: Dict[str, float] = {}

    for row in ordered.iter_rows(named=True):
        state = {str(k): float(v) for k, v in _decode_json(row["to_state"]).items()}
        keys = set(previous) | set(state)
        deltas: Dict[str, float] = {}
        for shipment_id in keys:
            new_val = state.get(shipment_id, 0.0)
            old_val = previous.get(shipment_id, 0.0)
            delta = new_val - old_val
            if abs(delta) > 1e-6:
                deltas[shipment_id] = delta
        metadata = _decode_json(row.get("metadata"))
        truck_id = metadata.get("truck_id") if metadata else None
        if deltas:
            events.append(
                BarnStateChange(
                    timestamp=row["event_dt"],
                    shipment_deltas=deltas,
                    state_after=state,
                    truck_id=str(truck_id) if truck_id else None,
                )
            )
        previous = state

    if not events:
        # even if there were updates, the final state may still be relevant
        last_state = {str(k): float(v) for k, v in previous.items()}
        metadata = _decode_json(ordered.tail(1)["metadata"][0])
        truck_id = metadata.get("truck_id") if metadata else None
        events.append(
            BarnStateChange(
                timestamp=ordered.tail(1)["event_dt"][0],
                shipment_deltas={},
                state_after=last_state,
                truck_id=str(truck_id) if truck_id else None,
            )
        )

    return events
