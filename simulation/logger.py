import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional


class EventLogger:
    """Persist simulation events to SQLite with lightweight batching."""

    def __init__(self, db_path: str) -> None:
        """Initialize the database connection and ensure the schema exists."""
        self.db_path = Path(db_path)
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
        self._pending = 0
        self._batch_size = 1000

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
        if self._pending:
            self.conn.commit()
            self._pending = 0
        self.conn.close()
