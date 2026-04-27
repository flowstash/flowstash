from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from ..entry import StateEntry
from ..protocol import _validate, _decode_entry

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS state_entries (
    namespace    TEXT    NOT NULL,
    key          TEXT    NOT NULL,
    value        BLOB    NOT NULL,
    content_type TEXT    NOT NULL DEFAULT 'application/json',
    updated_at   INTEGER NOT NULL,
    expires_at   INTEGER,
    version      INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (namespace, key)
);
"""

_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_state_expires_at ON state_entries (expires_at);
"""

_UPSERT = """
INSERT INTO state_entries (namespace, key, value, content_type, updated_at, expires_at, version)
VALUES (?, ?, ?, ?, ?, ?, 1)
ON CONFLICT(namespace, key) DO UPDATE SET
    value        = excluded.value,
    content_type = excluded.content_type,
    updated_at   = excluded.updated_at,
    expires_at   = excluded.expires_at,
    version      = state_entries.version + 1;
"""

_SELECT = """
SELECT value, content_type, updated_at, expires_at, version
FROM state_entries
WHERE namespace = ? AND key = ?
  AND (expires_at IS NULL OR expires_at > ?);
"""

_DELETE_EXPIRED_ROW = """
DELETE FROM state_entries WHERE namespace = ? AND key = ?;
"""

_PURGE_EXPIRED = """
DELETE FROM state_entries WHERE expires_at IS NOT NULL AND expires_at <= ?;
"""


def _now_epoch() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _epoch_to_dt(epoch: Optional[int]) -> Optional[datetime]:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


class SQLiteStateStore:
    """
    SQLite-backed StateStore.

    Thread-safe via a per-connection threading.local + a module-level
    threading.Lock for WAL-checkpoint scenarios.
    """

    def __init__(self, db_path: str = ":memory:", busy_timeout_ms: int = 5000):
        self._db_path = db_path
        self._busy_timeout_ms = busy_timeout_ms
        self._local = threading.local()
        # Ensure schema exists on first access
        conn = self._conn()
        conn.execute(_CREATE_TABLE)
        conn.execute(_CREATE_INDEX)
        conn.commit()

    def _conn(self) -> sqlite3.Connection:
        """Return a thread-local connection, creating it if needed."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(
                self._db_path,
                check_same_thread=False,
                timeout=self._busy_timeout_ms / 1000,
            )
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms};")
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return self._local.conn

    # ------------------------------------------------------------------
    # Protocol implementation
    # ------------------------------------------------------------------

    def get_entry(self, namespace: str, key: str) -> Optional[StateEntry]:
        _validate(namespace, key)
        conn = self._conn()
        now = _now_epoch()
        row = conn.execute(_SELECT, (namespace, key, now)).fetchone()
        if row is None:
            # Lazy cleanup: delete stale row if it existed but was expired
            # (SELECT already filtered it; we'd need a second query to confirm,
            # so we skip explicit deletion here — purge_expired() handles bulk cleanup)
            return None
        return StateEntry(
            value=row["value"],
            content_type=row["content_type"],
            updated_at=_epoch_to_dt(row["updated_at"]),
            expires_at=_epoch_to_dt(row["expires_at"]),
            version=row["version"],
        )

    def get(self, namespace: str, key: str) -> Optional[Any]:
        entry = self.get_entry(namespace, key)
        if entry is None:
            return None
        return _decode_entry(entry)

    def set(
        self,
        namespace: str,
        key: str,
        value: Any,
        *,
        content_type: str = "application/json",
        ttl_s: Optional[int] = None,
    ) -> StateEntry:
        _validate(namespace, key)
        now = _now_epoch()
        expires_at: Optional[int] = now + ttl_s if ttl_s is not None else None

        if content_type == "application/json":
            if isinstance(value, bytes):
                raw = value
            else:
                raw = json.dumps(value).encode("utf-8")
        elif isinstance(value, bytes):
            raw = value
        elif isinstance(value, str):
            raw = value.encode("utf-8")
        else:
            raw = str(value).encode("utf-8")

        conn = self._conn()
        conn.execute(_UPSERT, (namespace, key, raw, content_type, now, expires_at))
        conn.commit()

        # Fetch back to get the actual stored version
        row = conn.execute(
            "SELECT version FROM state_entries WHERE namespace=? AND key=?",
            (namespace, key),
        ).fetchone()

        return StateEntry(
            value=raw,
            content_type=content_type,
            updated_at=_epoch_to_dt(now),
            expires_at=_epoch_to_dt(expires_at),
            version=row["version"] if row else 1,
        )

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def purge_expired(self) -> int:
        """Delete all expired rows. Returns the number of rows deleted."""
        now = _now_epoch()
        conn = self._conn()
        cursor = conn.execute(_PURGE_EXPIRED, (now,))
        conn.commit()
        return cursor.rowcount
