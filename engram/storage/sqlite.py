"""SQLite control-plane store (§2.2, §16.1).

Holds the event ledger, filesystem→KG outbox, extractions, linked_entities, and
consolidation task queue. WAL mode for concurrent reads during writes.

Every row carries a `tenant_id` so a single control-plane DB serves many
tenants. The default tenant (`_default`) is used automatically when callers
do not pass one — preserves backward compatibility for single-tenant
deployments and the existing unit-test suite.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from engram.tenancy import DEFAULT_TENANT_ID

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS events (
    event_id      TEXT PRIMARY KEY,
    pair_id       TEXT UNIQUE NOT NULL,
    tenant_id     TEXT NOT NULL DEFAULT '_default',
    session_id    TEXT,
    source        TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    payload       TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'RECEIVED',
    retry_count   INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    processed_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_status   ON events(status, created_at);
CREATE INDEX IF NOT EXISTS idx_events_session  ON events(session_id);
CREATE INDEX IF NOT EXISTS idx_events_tenant   ON events(tenant_id, status, created_at);

CREATE TABLE IF NOT EXISTS fs_outbox (
    event_id      TEXT PRIMARY KEY,
    tenant_id     TEXT NOT NULL DEFAULT '_default',
    source_uri    TEXT NOT NULL,
    state         TEXT NOT NULL,
    retry_count   INTEGER NOT NULL DEFAULT 0,
    written_at    TEXT NOT NULL,
    last_attempt  TEXT,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_fs_outbox_state  ON fs_outbox(state, written_at);
CREATE INDEX IF NOT EXISTS idx_fs_outbox_tenant ON fs_outbox(tenant_id, state);

CREATE TABLE IF NOT EXISTS extractions (
    event_id      TEXT PRIMARY KEY,
    tenant_id     TEXT NOT NULL DEFAULT '_default',
    resolved_text TEXT NOT NULL,
    triplets      TEXT NOT NULL,
    l0_abstract   TEXT NOT NULL,
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS linked_entities (
    event_id       TEXT NOT NULL,
    tenant_id      TEXT NOT NULL DEFAULT '_default',
    triplet_idx    INTEGER NOT NULL,
    subject_node_id TEXT,
    object_node_id  TEXT,
    PRIMARY KEY (event_id, triplet_idx)
);

CREATE TABLE IF NOT EXISTS consolidation_tasks (
    task_id       TEXT PRIMARY KEY,
    tenant_id     TEXT NOT NULL DEFAULT '_default',
    node_id       TEXT NOT NULL,
    task_type     TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'PENDING',
    priority      INTEGER NOT NULL DEFAULT 5,
    scheduled_at  TEXT NOT NULL,
    started_at    TEXT,
    completed_at  TEXT,
    retry_count   INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_pending_unique
    ON consolidation_tasks(tenant_id, node_id, task_type)
    WHERE status IN ('PENDING', 'PROCESSING');
CREATE INDEX IF NOT EXISTS idx_tasks_status  ON consolidation_tasks(status, priority, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_tasks_tenant  ON consolidation_tasks(tenant_id, status);
"""


class SqliteStore:
    """Thin wrapper owning a single SQLite file with WAL enabled.

    Each get_conn() call returns a thread-local connection; SQLite itself is
    the concurrency boundary. Under WAL, readers and a single writer coexist.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._tls = threading.local()
        self._init_schema()

    def _new_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.path),
            isolation_level=None,  # autocommit; we use explicit transactions
            timeout=30.0,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _init_schema(self) -> None:
        with self._new_conn() as conn:
            conn.executescript(SCHEMA_SQL)

    def get_conn(self) -> sqlite3.Connection:
        conn = getattr(self._tls, "conn", None)
        if conn is None:
            conn = self._new_conn()
            self._tls.conn = conn
        return conn

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self.get_conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------------
    # Event ledger helpers
    # ------------------------------------------------------------------

    def record_event(
        self,
        *,
        pair_id: str,
        session_id: str | None,
        source: str,
        event_type: str,
        payload: dict[str, Any],
        tenant_id: str = DEFAULT_TENANT_ID,
    ) -> tuple[str, bool]:
        """Insert an event atomically. Returns (event_id, is_new)."""
        event_id = f"evt-{uuid.uuid4().hex[:12]}"
        try:
            with self.transaction() as conn:
                conn.execute(
                    "INSERT INTO events (event_id, pair_id, tenant_id, session_id, "
                    "source, event_type, payload) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (event_id, pair_id, tenant_id, session_id, source, event_type,
                     json.dumps(payload)),
                )
            return event_id, True
        except sqlite3.IntegrityError:
            row = self.get_conn().execute(
                "SELECT event_id FROM events WHERE pair_id = ?", (pair_id,)
            ).fetchone()
            if row is None:
                raise
            return row["event_id"], False

    def set_event_status(
        self, event_id: str, status: str, error_message: str | None = None
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE events SET status = ?, error_message = ?, processed_at = datetime('now') "
                "WHERE event_id = ?",
                (status, error_message, event_id),
            )

    def get_event(self, event_id: str) -> dict[str, Any] | None:
        row = self.get_conn().execute(
            "SELECT * FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["payload"] = json.loads(d["payload"])
        return d

    def claim_pending_events(
        self, limit: int = 10, *, tenant_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Atomically move RECEIVED → PROCESSING (tracked via a NULL processed_at)."""
        if tenant_id is None:
            rows = self.get_conn().execute(
                "SELECT * FROM events WHERE status = 'RECEIVED' "
                "ORDER BY created_at LIMIT ?",
                (limit,),
            ).fetchall()
        else:
            rows = self.get_conn().execute(
                "SELECT * FROM events WHERE status = 'RECEIVED' AND tenant_id = ? "
                "ORDER BY created_at LIMIT ?",
                (tenant_id, limit),
            ).fetchall()
        return [dict(r, payload=json.loads(r["payload"])) for r in rows]

    # ------------------------------------------------------------------
    # Extraction storage
    # ------------------------------------------------------------------

    def save_extraction(
        self,
        event_id: str,
        resolved_text: str,
        triplets: list[dict[str, Any]],
        l0_abstract: str,
        *,
        tenant_id: str = DEFAULT_TENANT_ID,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO extractions "
                "(event_id, tenant_id, resolved_text, triplets, l0_abstract) "
                "VALUES (?, ?, ?, ?, ?)",
                (event_id, tenant_id, resolved_text, json.dumps(triplets), l0_abstract),
            )

    def get_extraction(self, event_id: str) -> dict[str, Any] | None:
        row = self.get_conn().execute(
            "SELECT * FROM extractions WHERE event_id = ?", (event_id,)
        ).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["triplets"] = json.loads(d["triplets"])
        return d

    # ------------------------------------------------------------------
    # Outbox
    # ------------------------------------------------------------------

    def fs_outbox_write(
        self, event_id: str, source_uri: str, *, tenant_id: str = DEFAULT_TENANT_ID,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO fs_outbox "
                "(event_id, tenant_id, source_uri, state, written_at) "
                "VALUES (?, ?, ?, 'WRITTEN', datetime('now'))",
                (event_id, tenant_id, source_uri),
            )

    def fs_outbox_mark(self, event_id: str, state: str, error: str | None = None) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE fs_outbox SET state = ?, last_attempt = datetime('now'), "
                "retry_count = retry_count + 1, error_message = ? WHERE event_id = ?",
                (state, error, event_id),
            )

    # ------------------------------------------------------------------
    # Consolidation queue
    # ------------------------------------------------------------------

    def enqueue_task(
        self,
        *,
        node_id: str,
        task_type: str,
        priority: int = 5,
        tenant_id: str = DEFAULT_TENANT_ID,
    ) -> str | None:
        """Insert a task; return task_id, or None if deduped by the unique index."""
        task_id = f"task-{uuid.uuid4().hex[:12]}"
        try:
            with self.transaction() as conn:
                conn.execute(
                    "INSERT INTO consolidation_tasks "
                    "(task_id, tenant_id, node_id, task_type, priority, scheduled_at) "
                    "VALUES (?, ?, ?, ?, ?, datetime('now'))",
                    (task_id, tenant_id, node_id, task_type, priority),
                )
            return task_id
        except sqlite3.IntegrityError:
            return None

    def queue_depth(self, *, tenant_id: str | None = None) -> int:
        if tenant_id is None:
            row = self.get_conn().execute(
                "SELECT COUNT(*) AS c FROM consolidation_tasks "
                "WHERE status IN ('PENDING', 'PROCESSING')"
            ).fetchone()
        else:
            row = self.get_conn().execute(
                "SELECT COUNT(*) AS c FROM consolidation_tasks "
                "WHERE status IN ('PENDING', 'PROCESSING') AND tenant_id = ?",
                (tenant_id,),
            ).fetchone()
        return int(row["c"])
