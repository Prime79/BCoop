from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import polars as pl

FLOW_SCHEMA = {
    "shipment_id": pl.Utf8,
    "resource_id": pl.Utf8,
    "resource_type": pl.Utf8,
    "from_state": pl.Utf8,
    "to_state": pl.Utf8,
    "event_ts": pl.Utf8,
    "quantity": pl.Float64,
    "metadata": pl.Utf8,
}


class FlowWriter:
    """Append-only Parquet/Polars sink for resource flow events."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._buffer: list[dict[str, Any]] = []

    def log(
        self,
        shipment_id: str,
        resource_id: str,
        resource_type: str,
        from_state: Optional[Dict[str, Any]],
        to_state: Optional[Dict[str, Any]],
        event_ts: str,
        quantity: Optional[float],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._buffer.append(
            {
                "shipment_id": shipment_id,
                "resource_id": resource_id,
                "resource_type": resource_type,
                "from_state": json.dumps(from_state) if from_state else None,
                "to_state": json.dumps(to_state) if to_state else None,
                "event_ts": event_ts,
                "quantity": quantity,
                "metadata": json.dumps(metadata) if metadata else None,
            }
        )

    def close(self) -> None:
        if not self._buffer:
            return
        df = pl.DataFrame(self._buffer, schema=FLOW_SCHEMA)
        if self.path.exists():
            existing = pl.read_parquet(self.path)
            df = pl.concat([existing, df], how="vertical")
        df.write_parquet(self.path)
        self._buffer.clear()
