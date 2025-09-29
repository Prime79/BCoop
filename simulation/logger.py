import json
try:
    import sqlite3  # type: ignore
except Exception:  # pragma: no cover - optional fallback when sqlite3 is unavailable
    sqlite3 = None  # type: ignore
from pathlib import Path
from typing import Any, Dict, Optional


class EventLogger:
    """Persist simulation events to SQLite with lightweight batching.

    Graphs and analysis do not depend on SQLite. This logger is used for audit and
    debugging. If the `sqlite3` Python module is unavailable in the runtime, it
    falls back to appending JSON lines to `<db_path>.jsonl` so that runs are not
blocked by environment limitations.
"""

    def __init__(self, db_path: str) -> None:
        """Initialize the database connection; fallback to JSONL if sqlite3 unavailable."""
        self.db_path = Path(db_path)
        self._pending = 0
        self._batch_size = 1000
        self._jsonl_path = self.db_path.with_suffix('.jsonl')
        if sqlite3 is None:
            self.conn = None
        else:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sim_day REAL NOT NULL,
                    real_ts TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    status TEXT NOT NULL,
                    quantity REAL,
                    metadata TEXT
                )
                """
            )
            self.conn.commit()

    def log(
        self,
        sim_day: float,
        entity_id: str,
        stage: str,
        status: str,
        quantity: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
        real_ts: Optional[str] = None,
    ) -> None:
        """Insert an event row, committing in batches for performance."""
        payload = json.dumps(metadata or {}, ensure_ascii=True)
        if self.conn is None:
            # Fallback: append as JSONL so we still have a record stream if needed
            record = {
                "sim_day": sim_day,
                "real_ts": real_ts or "",
                "entity_id": entity_id,
                "stage": stage,
                "status": status,
                "quantity": quantity,
                "metadata": json.loads(payload),
            }
            with self._jsonl_path.open('a', encoding='utf-8') as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        else:
            self.conn.execute(
                """
                INSERT INTO events (sim_day, real_ts, entity_id, stage, status, quantity, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (sim_day, real_ts or "", entity_id, stage, status, quantity, payload),
            )
            self._pending += 1
            if self._pending >= self._batch_size:
                self.conn.commit()
                self._pending = 0

    def close(self) -> None:
        """Flush any pending inserts and close the database connection."""
        if self.conn is None:
            return
        if self._pending:
            self.conn.commit()
            self._pending = 0
        self.conn.close()


class NoOpEventLogger:
    """Drop-in replacement when audit logging is disabled."""

    def __init__(self) -> None:
        self.db_path = Path('')

    def log(
        self,
        sim_day: float,
        entity_id: str,
        stage: str,
        status: str,
        quantity: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
        real_ts: Optional[str] = None,
    ) -> None:
        return None

    def close(self) -> None:
        return None
