"""SQLite control-plane store (§2.2, §16.1).

Holds the event ledger, durable ingest stages/outputs, artifact-level
filesystem→KG readiness, extractions, linked entities, and consolidation
queue. WAL mode permits concurrent reads during writes.

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
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from engram.tenancy import DEFAULT_TENANT_ID

_INGEST_STAGE_RANK = {
    "RECEIVED": 0,
    "GATED": 1,
    "EXTRACTED": 2,
    "LINKED": 3,
    "FILESYSTEM_COMMITTED": 4,
    "KG_COMMITTED": 5,
    "CONSOLIDATION_COMMITTED": 6,
    "COMPLETE": 7,
    "GATED_SKIP": 7,
}

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS events (
    event_id      TEXT PRIMARY KEY,
    pair_id       TEXT NOT NULL,
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
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_tenant_pair ON events(tenant_id, pair_id);

CREATE TABLE IF NOT EXISTS event_stage_state (
    event_id       TEXT PRIMARY KEY,
    tenant_id      TEXT NOT NULL DEFAULT '_default',
    completed_stage TEXT NOT NULL DEFAULT 'RECEIVED',
    gate_output    TEXT,
    link_output    TEXT,
    updated_at     TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (event_id) REFERENCES events(event_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_event_stage_tenant
    ON event_stage_state(tenant_id, completed_stage, updated_at);

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

CREATE TABLE IF NOT EXISTS ingest_artifacts (
    event_id        TEXT NOT NULL,
    tenant_id       TEXT NOT NULL DEFAULT '_default',
    artifact_type   TEXT NOT NULL,
    source_uri      TEXT NOT NULL,
    artifact_id     TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    source_session_id TEXT,
    source_turn_ids TEXT NOT NULL DEFAULT '[]',
    confidence      REAL,
    extractor_version TEXT,
    required        INTEGER NOT NULL DEFAULT 1,
    filesystem_state TEXT NOT NULL DEFAULT 'PENDING',
    kg_state        TEXT NOT NULL DEFAULT 'PENDING',
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    error_message   TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (event_id, source_uri),
    FOREIGN KEY (event_id) REFERENCES events(event_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_ingest_artifacts_event
    ON ingest_artifacts(tenant_id, event_id, required, filesystem_state, kg_state);
CREATE INDEX IF NOT EXISTS idx_ingest_artifacts_state
    ON ingest_artifacts(tenant_id, filesystem_state, kg_state, updated_at);

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
    not_before    TEXT,
    generation    INTEGER NOT NULL DEFAULT 1,
    claimed_generation INTEGER,
    child_signature TEXT,
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

CREATE TABLE IF NOT EXISTS bulk_jobs (
    job_id          TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL DEFAULT '_default',
    source          TEXT NOT NULL,
    filename        TEXT,
    dry_run         INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL,
    total_count     INTEGER NOT NULL DEFAULT 0,
    accepted_count  INTEGER NOT NULL DEFAULT 0,
    rejected_count  INTEGER NOT NULL DEFAULT 0,
    rejected_rows   TEXT NOT NULL DEFAULT '[]',
    event_ids       TEXT NOT NULL DEFAULT '[]',
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at    TEXT
);
CREATE INDEX IF NOT EXISTS idx_bulk_jobs_tenant_created
    ON bulk_jobs(tenant_id, created_at);
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
        created_at = datetime.now(timezone.utc).isoformat()
        try:
            with self.transaction() as conn:
                conn.execute(
                    "INSERT INTO events (event_id, pair_id, tenant_id, session_id, "
                    "source, event_type, payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (event_id, pair_id, tenant_id, session_id, source, event_type,
                     json.dumps(payload), created_at),
                )
                conn.execute(
                    "INSERT INTO event_stage_state "
                    "(event_id, tenant_id, completed_stage) VALUES (?, ?, 'RECEIVED')",
                    (event_id, tenant_id),
                )
            return event_id, True
        except sqlite3.IntegrityError:
            row = self.get_conn().execute(
                "SELECT event_id FROM events WHERE tenant_id = ? AND pair_id = ?",
                (tenant_id, pair_id),
            ).fetchone()
            if row is None:
                raise
            return row["event_id"], False

    def set_event_status(
        self,
        event_id: str,
        status: str,
        error_message: str | None = None,
        *,
        tenant_id: str | None = None,
    ) -> None:
        tenant_clause = "" if tenant_id is None else " AND tenant_id = ?"
        params: tuple[Any, ...]
        if tenant_id is None:
            params = (status, error_message, event_id)
        else:
            params = (status, error_message, event_id, tenant_id)
        with self.transaction() as conn:
            conn.execute(
                "UPDATE events SET status = ?, error_message = ?, processed_at = datetime('now') "
                f"WHERE event_id = ?{tenant_clause}",
                params,
            )

    def get_event(
        self, event_id: str, *, tenant_id: str | None = None,
    ) -> dict[str, Any] | None:
        if tenant_id is None:
            row = self.get_conn().execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
        else:
            row = self.get_conn().execute(
                "SELECT * FROM events WHERE event_id = ? AND tenant_id = ?",
                (event_id, tenant_id),
            ).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["payload"] = json.loads(d["payload"])
        return d

    def get_event_readiness(
        self,
        event_ids: list[str],
        *,
        tenant_id: str,
    ) -> list[dict[str, Any]]:
        """Return tenant-scoped event plus outbox state for exact drain checks."""
        if not event_ids:
            return []
        placeholders = ",".join("?" for _ in event_ids)
        rows = self.get_conn().execute(
            "SELECT e.event_id, e.pair_id, e.status, e.error_message, "
            "e.created_at, e.processed_at, o.state AS outbox_state, "
            "o.source_uri AS source_uri, s.completed_stage, "
            "COALESCE(a.artifact_count, 0) AS artifact_count, "
            "COALESCE(a.filesystem_ready_count, 0) AS filesystem_ready_count, "
            "COALESCE(a.kg_ready_count, 0) AS kg_ready_count, "
            "COALESCE(a.artifact_error_count, 0) AS artifact_error_count "
            "FROM events e LEFT JOIN fs_outbox o ON o.event_id = e.event_id "
            "LEFT JOIN event_stage_state s ON s.event_id = e.event_id "
            "LEFT JOIN ("
            "  SELECT event_id, "
            "  SUM(CASE WHEN required = 1 THEN 1 ELSE 0 END) AS artifact_count, "
            "  SUM(CASE WHEN required = 1 AND filesystem_state = 'COMMITTED' THEN 1 ELSE 0 END) "
            "      AS filesystem_ready_count, "
            "  SUM(CASE WHEN required = 1 AND kg_state = 'COMMITTED' THEN 1 ELSE 0 END) "
            "      AS kg_ready_count, "
            "  SUM(CASE WHEN required = 1 AND (filesystem_state = 'FAILED' OR kg_state = 'FAILED') "
            "      THEN 1 ELSE 0 END) AS artifact_error_count "
            "  FROM ingest_artifacts GROUP BY event_id"
            ") a ON a.event_id = e.event_id "
            f"WHERE e.tenant_id = ? AND e.event_id IN ({placeholders})",
            (tenant_id, *event_ids),
        ).fetchall()
        by_id = {row["event_id"]: dict(row) for row in rows}
        return [by_id[event_id] for event_id in event_ids if event_id in by_id]

    def list_event_ids(
        self,
        *,
        tenant_id: str,
        source: str | None = None,
        limit: int = 500,
    ) -> tuple[int, list[str]]:
        """Return a bounded tenant event set and its unbounded total count."""
        where = "tenant_id = ?"
        params: list[Any] = [tenant_id]
        if source is not None:
            where += " AND source = ?"
            params.append(source)
        total_row = self.get_conn().execute(
            f"SELECT COUNT(*) AS count FROM events WHERE {where}",
            tuple(params),
        ).fetchone()
        rows = self.get_conn().execute(
            f"SELECT event_id FROM events WHERE {where} ORDER BY created_at, rowid LIMIT ?",
            (*params, limit),
        ).fetchall()
        return int(total_row["count"] if total_row else 0), [
            str(row["event_id"]) for row in rows
        ]

    def get_event_stage(self, event_id: str, *, tenant_id: str) -> dict[str, Any]:
        """Return the durable ingest stage, creating a legacy-compatible row."""
        row = self.get_conn().execute(
            "SELECT * FROM event_stage_state WHERE event_id = ? AND tenant_id = ?",
            (event_id, tenant_id),
        ).fetchone()
        if row is None:
            stage = self._infer_legacy_stage(event_id, tenant_id=tenant_id)
            with self.transaction() as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO event_stage_state "
                    "(event_id, tenant_id, completed_stage) VALUES (?, ?, ?)",
                    (event_id, tenant_id, stage),
                )
            row = self.get_conn().execute(
                "SELECT * FROM event_stage_state WHERE event_id = ? AND tenant_id = ?",
                (event_id, tenant_id),
            ).fetchone()
        if row is None:
            raise RuntimeError(f"unable to initialise ingest stage for {event_id}")
        state = dict(row)
        for key in ("gate_output", "link_output"):
            state[key] = json.loads(state[key]) if state.get(key) else None
        return state

    def advance_event_stage(
        self,
        event_id: str,
        stage: str,
        *,
        tenant_id: str,
        gate_output: dict[str, Any] | None = None,
        link_output: list[dict[str, Any]] | None = None,
    ) -> None:
        """Persist a successfully committed stage and nondeterministic output."""
        with self.transaction() as conn:
            current = conn.execute(
                "SELECT completed_stage FROM event_stage_state WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if stage not in _INGEST_STAGE_RANK:
                raise ValueError(f"unknown ingest stage: {stage}")
            if current is not None:
                current_stage = str(current["completed_stage"])
                if _INGEST_STAGE_RANK.get(current_stage, -1) > _INGEST_STAGE_RANK[stage]:
                    raise RuntimeError(
                        f"refusing ingest stage regression {current_stage} -> {stage}"
                    )
            conn.execute(
                "INSERT INTO event_stage_state "
                "(event_id, tenant_id, completed_stage, gate_output, link_output, updated_at) "
                "VALUES (?, ?, ?, ?, ?, datetime('now')) "
                "ON CONFLICT(event_id) DO UPDATE SET "
                "tenant_id = excluded.tenant_id, completed_stage = excluded.completed_stage, "
                "gate_output = COALESCE(excluded.gate_output, event_stage_state.gate_output), "
                "link_output = COALESCE(excluded.link_output, event_stage_state.link_output), "
                "updated_at = datetime('now')",
                (
                    event_id,
                    tenant_id,
                    stage,
                    json.dumps(gate_output) if gate_output is not None else None,
                    json.dumps(link_output) if link_output is not None else None,
                ),
            )

    def _infer_legacy_stage(self, event_id: str, *, tenant_id: str) -> str:
        event = self.get_event(event_id, tenant_id=tenant_id)
        if event is None:
            raise RuntimeError(f"unknown event_id: {event_id}")
        if event["status"] in {"COMPLETE", "GATED_SKIP"}:
            return str(event["status"])
        outbox = self.get_fs_outbox(event_id)
        if event["status"] == "INDEXED" or (outbox and outbox["state"] == "INDEXED"):
            return "KG_COMMITTED"
        if outbox and outbox["state"] in {"WRITTEN", "INDEX_FAILED"}:
            return "FILESYSTEM_COMMITTED"
        linked = self.get_conn().execute(
            "SELECT 1 FROM linked_entities WHERE event_id = ? AND tenant_id = ? LIMIT 1",
            (event_id, tenant_id),
        ).fetchone()
        if linked is not None:
            return "LINKED"
        if self.get_extraction(event_id) is not None:
            return "EXTRACTED"
        if event["status"] == "GATED_STORE":
            return "GATED"
        return "RECEIVED"

    def request_maintenance_kg_replay(self, event_id: str, *, tenant_id: str) -> None:
        """Deliberately invalidate only KG+consolidation commits for maintenance.

        Unlike crash recovery, this is an explicit, named stage regression. Gate,
        extraction, entity-link, and filesystem outputs remain immutable.
        """
        state = self.get_event_stage(event_id, tenant_id=tenant_id)
        current = str(state["completed_stage"])
        if _INGEST_STAGE_RANK.get(current, -1) < _INGEST_STAGE_RANK["FILESYSTEM_COMMITTED"]:
            raise RuntimeError(
                f"cannot request KG replay before filesystem commit: {event_id} ({current})"
            )
        with self.transaction() as conn:
            conn.execute(
                "UPDATE event_stage_state SET completed_stage = 'FILESYSTEM_COMMITTED', "
                "updated_at = datetime('now') WHERE event_id = ? AND tenant_id = ?",
                (event_id, tenant_id),
            )
            conn.execute(
                "UPDATE ingest_artifacts SET kg_state = 'PENDING', error_message = NULL, "
                "updated_at = datetime('now') WHERE event_id = ? AND tenant_id = ?",
                (event_id, tenant_id),
            )
            conn.execute(
                "UPDATE fs_outbox SET state = 'WRITTEN', error_message = NULL, "
                "last_attempt = datetime('now') WHERE event_id = ? AND tenant_id = ?",
                (event_id, tenant_id),
            )
            conn.execute(
                "UPDATE events SET status = 'RECEIVED', error_message = NULL, processed_at = NULL "
                "WHERE event_id = ? AND tenant_id = ?",
                (event_id, tenant_id),
            )

    def claim_pending_events(
        self, limit: int = 10, *, tenant_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Atomically claim RECEIVED events for a durable worker.

        The claim is a shared SQLite state transition, so independent API
        workers/replicas cannot pick the same event before processing starts.
        Reconciliation treats stale PROCESSING rows as replayable.
        """
        if limit <= 0:
            return []
        with self.transaction() as conn:
            if tenant_id is None:
                selected = conn.execute(
                    "SELECT event_id FROM events WHERE status = 'RECEIVED' "
                    "ORDER BY created_at LIMIT ?",
                    (limit,),
                ).fetchall()
            else:
                selected = conn.execute(
                    "SELECT event_id FROM events WHERE status = 'RECEIVED' AND tenant_id = ? "
                    "ORDER BY created_at LIMIT ?",
                    (tenant_id, limit),
                ).fetchall()
            event_ids = [r["event_id"] for r in selected]
            if not event_ids:
                return []
            placeholders = ",".join("?" for _ in event_ids)
            conn.execute(
                f"UPDATE events SET status = 'PROCESSING', processed_at = datetime('now'), "
                f"error_message = NULL WHERE status = 'RECEIVED' "
                f"AND event_id IN ({placeholders})",
                tuple(event_ids),
            )
            rows = conn.execute(
                f"SELECT * FROM events WHERE event_id IN ({placeholders}) "
                "ORDER BY created_at",
                tuple(event_ids),
            ).fetchall()
        return [dict(r, payload=json.loads(r["payload"])) for r in rows]

    def release_event_claim(self, event_id: str) -> bool:
        """Return an interrupted in-flight event to the durable queue.

        This is a lease release, not a retry: committed stage state remains
        authoritative and retry telemetry must not count orderly shutdown.
        """
        with self.transaction() as conn:
            cursor = conn.execute(
                "UPDATE events SET status = 'RECEIVED', processed_at = NULL "
                "WHERE event_id = ? AND status IN ('PROCESSING', 'GATED_STORE', 'INDEXED')",
                (event_id,),
            )
        return cursor.rowcount > 0

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

    def get_fs_outbox(self, event_id: str) -> dict[str, Any] | None:
        row = self.get_conn().execute(
            "SELECT * FROM fs_outbox WHERE event_id = ?", (event_id,)
        ).fetchone()
        return dict(row) if row is not None else None

    def upsert_ingest_artifact(
        self,
        *,
        event_id: str,
        tenant_id: str,
        artifact_type: str,
        source_uri: str,
        artifact_id: str,
        content_hash: str,
        source_session_id: str | None = None,
        source_turn_ids: list[str] | None = None,
        confidence: float | None = None,
        extractor_version: str | None = None,
        filesystem_state: str = "COMMITTED",
        required: bool = True,
    ) -> None:
        """Record one immutable filesystem artifact without replacing its identity."""
        existing = self.get_conn().execute(
            "SELECT artifact_id, content_hash FROM ingest_artifacts "
            "WHERE event_id = ? AND source_uri = ?",
            (event_id, source_uri),
        ).fetchone()
        if existing is not None and (
            existing["artifact_id"] != artifact_id or existing["content_hash"] != content_hash
        ):
            raise RuntimeError(f"artifact identity changed on replay: {source_uri}")
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO ingest_artifacts "
                "(event_id, tenant_id, artifact_type, source_uri, artifact_id, content_hash, "
                "source_session_id, source_turn_ids, confidence, extractor_version, "
                "required, filesystem_state, kg_state, attempt_count, error_message) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 1, NULL) "
                "ON CONFLICT(event_id, source_uri) DO UPDATE SET "
                "filesystem_state = excluded.filesystem_state, required = excluded.required, "
                "source_session_id = excluded.source_session_id, "
                "source_turn_ids = excluded.source_turn_ids, "
                "confidence = excluded.confidence, extractor_version = excluded.extractor_version, "
                "attempt_count = ingest_artifacts.attempt_count + 1, error_message = NULL, "
                "updated_at = datetime('now')",
                (
                    event_id,
                    tenant_id,
                    artifact_type,
                    source_uri,
                    artifact_id,
                    content_hash,
                    source_session_id,
                    json.dumps(source_turn_ids or []),
                    confidence,
                    extractor_version,
                    1 if required else 0,
                    filesystem_state,
                ),
            )

    def mark_event_artifacts_kg(
        self,
        event_id: str,
        state: str,
        *,
        error: str | None = None,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE ingest_artifacts SET kg_state = ?, error_message = ?, "
                "attempt_count = attempt_count + 1, updated_at = datetime('now') "
                "WHERE event_id = ? AND required = 1",
                (state, error, event_id),
            )

    def list_ingest_artifacts(
        self, event_id: str, *, tenant_id: str
    ) -> list[dict[str, Any]]:
        rows = self.get_conn().execute(
            "SELECT * FROM ingest_artifacts WHERE event_id = ? AND tenant_id = ? "
            "ORDER BY artifact_type, source_uri",
            (event_id, tenant_id),
        ).fetchall()
        return [dict(row) for row in rows]

    def get_linked_entity_uris(self, event_id: str, *, tenant_id: str) -> list[str]:
        rows = self.get_conn().execute(
            "SELECT subject_node_id, object_node_id FROM linked_entities "
            "WHERE event_id = ? AND tenant_id = ? ORDER BY triplet_idx",
            (event_id, tenant_id),
        ).fetchall()
        values: list[str] = []
        for row in rows:
            for key in ("subject_node_id", "object_node_id"):
                value = row[key]
                if value and value not in values:
                    values.append(str(value))
        return values

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

    def enqueue_directory_refresh(
        self,
        *,
        node_id: str,
        tenant_id: str = DEFAULT_TENANT_ID,
        debounce_seconds: int = 0,
        child_signature: str | None = None,
        priority: int = 5,
    ) -> str | None:
        """Coalesce a directory dirty signal into one generation-aware task."""
        modifier = f"+{max(0, debounce_seconds)} seconds"
        with self.transaction() as conn:
            active = conn.execute(
                "SELECT task_id, status, generation, child_signature "
                "FROM consolidation_tasks WHERE tenant_id = ? AND node_id = ? "
                "AND task_type = 'REFRESH_DIRECTORY' "
                "AND status IN ('PENDING', 'PROCESSING') LIMIT 1",
                (tenant_id, node_id),
            ).fetchone()
            if active is not None:
                changed = child_signature is None or active["child_signature"] != child_signature
                conn.execute(
                    "UPDATE consolidation_tasks SET "
                    "generation = generation + ?, child_signature = ?, "
                    "scheduled_at = datetime('now', ?), not_before = datetime('now', ?), "
                    "priority = MIN(priority, ?) WHERE task_id = ?",
                    (
                        1 if changed else 0,
                        child_signature,
                        modifier,
                        modifier,
                        priority,
                        active["task_id"],
                    ),
                )
                return str(active["task_id"])

            previous = conn.execute(
                "SELECT child_signature FROM consolidation_tasks "
                "WHERE tenant_id = ? AND node_id = ? AND task_type = 'REFRESH_DIRECTORY' "
                "AND status = 'COMPLETE' ORDER BY completed_at DESC LIMIT 1",
                (tenant_id, node_id),
            ).fetchone()
            if (
                child_signature is not None
                and previous is not None
                and previous["child_signature"] == child_signature
            ):
                return None

            task_id = f"task-{uuid.uuid4().hex[:12]}"
            conn.execute(
                "INSERT INTO consolidation_tasks "
                "(task_id, tenant_id, node_id, task_type, priority, scheduled_at, "
                "not_before, generation, child_signature) "
                "VALUES (?, ?, ?, 'REFRESH_DIRECTORY', ?, datetime('now', ?), "
                "datetime('now', ?), 1, ?)",
                (
                    task_id,
                    tenant_id,
                    node_id,
                    priority,
                    modifier,
                    modifier,
                    child_signature,
                ),
            )
            return task_id

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

    def count_events_by_status(
        self, status: str, *, tenant_id: str | None = None
    ) -> int:
        params: tuple[str, ...]
        if tenant_id is None:
            query = "SELECT COUNT(*) AS c FROM events WHERE status = ?"
            params = (status,)
        else:
            query = (
                "SELECT COUNT(*) AS c FROM events "
                "WHERE status = ? AND tenant_id = ?"
            )
            params = (status, tenant_id)
        row = self.get_conn().execute(query, params).fetchone()
        return int(row["c"]) if row else 0

    def count_outbox_pending(self) -> int:
        row = self.get_conn().execute(
            "SELECT COUNT(*) AS c FROM fs_outbox WHERE state = 'PENDING'"
        ).fetchone()
        return int(row["c"]) if row else 0

    # ------------------------------------------------------------------
    # Bulk ingest jobs
    # ------------------------------------------------------------------

    def save_bulk_job(
        self,
        *,
        job_id: str,
        tenant_id: str,
        source: str,
        filename: str | None,
        dry_run: bool,
        status: str,
        total_count: int,
        accepted_count: int,
        rejected_count: int,
        rejected_rows: list[dict[str, Any]],
        event_ids: list[str],
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO bulk_jobs "
                "(job_id, tenant_id, source, filename, dry_run, status, total_count, "
                "accepted_count, rejected_count, rejected_rows, event_ids, completed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "CASE WHEN ? THEN datetime('now') ELSE NULL END)",
                (
                    job_id,
                    tenant_id,
                    source,
                    filename,
                    1 if dry_run else 0,
                    status,
                    total_count,
                    accepted_count,
                    rejected_count,
                    json.dumps(rejected_rows),
                    json.dumps(event_ids),
                    status != "QUEUED",
                ),
            )

    def set_bulk_job_status(
        self,
        job_id: str,
        status: str,
        *,
        tenant_id: str | None = None,
    ) -> None:
        tenant_clause = "" if tenant_id is None else " AND tenant_id = ?"
        params: tuple[Any, ...] = (
            (status, job_id) if tenant_id is None else (status, job_id, tenant_id)
        )
        with self.transaction() as conn:
            conn.execute(
                "UPDATE bulk_jobs SET status = ?, "
                "completed_at = COALESCE(completed_at, datetime('now')) "
                f"WHERE job_id = ?{tenant_clause}",
                params,
            )

    def get_bulk_job(
        self, job_id: str, *, tenant_id: str | None = None,
    ) -> dict[str, Any] | None:
        if tenant_id is None:
            row = self.get_conn().execute(
                "SELECT * FROM bulk_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        else:
            row = self.get_conn().execute(
                "SELECT * FROM bulk_jobs WHERE job_id = ? AND tenant_id = ?",
                (job_id, tenant_id),
            ).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["dry_run"] = bool(d.get("dry_run"))
        d["rejected_rows"] = json.loads(d.get("rejected_rows") or "[]")
        d["event_ids"] = json.loads(d.get("event_ids") or "[]")
        return d
