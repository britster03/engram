"""PostgreSQL implementation of Engram's durable control plane."""

from __future__ import annotations

import json
import re
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from engram.storage.chat_runs import CHAT_RUNS_SQL
from engram.storage.control_plane import ControlPlaneQueries
from engram.storage.idempotency import IdempotencyConflictError, payload_fingerprint
from engram.storage.session_search import SESSION_SEARCH_INDEXES, SESSION_STATE_SQL
from engram.tenancy import DEFAULT_TENANT_ID

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool
except ImportError:  # pragma: no cover - exercised only without production deps
    psycopg = None  # type: ignore[assignment]
    ConnectionPool = None  # type: ignore[assignment,misc]
    dict_row = None  # type: ignore[assignment]


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY, pair_id TEXT NOT NULL, tenant_id TEXT NOT NULL DEFAULT '_default',
    session_id TEXT, source TEXT NOT NULL, event_type TEXT NOT NULL, payload TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'RECEIVED', retry_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_tenant_pair ON events(tenant_id, pair_id);
CREATE INDEX IF NOT EXISTS idx_events_status ON events(status, created_at);
CREATE INDEX IF NOT EXISTS idx_events_tenant ON events(tenant_id, status, created_at);
CREATE TABLE IF NOT EXISTS fs_outbox (
    event_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL DEFAULT '_default', source_uri TEXT NOT NULL,
    state TEXT NOT NULL, retry_count INTEGER NOT NULL DEFAULT 0,
    written_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, last_attempt TIMESTAMPTZ,
    error_message TEXT
);
CREATE TABLE IF NOT EXISTS extractions (
    event_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL DEFAULT '_default', resolved_text TEXT NOT NULL,
    triplets TEXT NOT NULL, l0_abstract TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS linked_entities (
    event_id TEXT NOT NULL, tenant_id TEXT NOT NULL DEFAULT '_default', triplet_idx INTEGER NOT NULL,
    subject_node_id TEXT, object_node_id TEXT, PRIMARY KEY(event_id, triplet_idx)
);
CREATE TABLE IF NOT EXISTS consolidation_tasks (
    task_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL DEFAULT '_default', node_id TEXT NOT NULL,
    task_type TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING', priority INTEGER NOT NULL DEFAULT 5,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ, retry_count INTEGER NOT NULL DEFAULT 0, error_message TEXT,
    requested_generation BIGINT NOT NULL DEFAULT 1, processing_generation BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_pending_unique
    ON consolidation_tasks(tenant_id, node_id, task_type) WHERE status IN ('PENDING', 'PROCESSING');
CREATE INDEX IF NOT EXISTS idx_tasks_status ON consolidation_tasks(status, priority, scheduled_at);
CREATE TABLE IF NOT EXISTS bulk_jobs (
    job_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL DEFAULT '_default', source TEXT NOT NULL,
    filename TEXT, dry_run BOOLEAN NOT NULL DEFAULT FALSE, status TEXT NOT NULL,
    total_count INTEGER NOT NULL DEFAULT 0, accepted_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0, rejected_rows TEXT NOT NULL DEFAULT '[]',
    event_ids TEXT NOT NULL DEFAULT '[]', created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS bulk_code_items (
    job_id TEXT NOT NULL, tenant_id TEXT NOT NULL DEFAULT '_default', row_number INTEGER NOT NULL,
    path TEXT NOT NULL, status TEXT NOT NULL, node_count INTEGER NOT NULL DEFAULT 0,
    relationship_count INTEGER NOT NULL DEFAULT 0, error_message TEXT, project_uri TEXT,
    PRIMARY KEY(job_id, row_number)
);
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id TEXT PRIMARY KEY, payload TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS api_keys (
    key_hash TEXT PRIMARY KEY, tenant_id TEXT NOT NULL REFERENCES tenants(tenant_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, last_used_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys(tenant_id);
CREATE TABLE IF NOT EXISTS audit_log (
    audit_id TEXT PRIMARY KEY, ts TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tenant_id TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL, target TEXT,
    request_id TEXT, details TEXT, remote_addr TEXT
);
CREATE INDEX IF NOT EXISTS idx_audit_tenant_ts ON audit_log(tenant_id, ts);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY, value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS workflow_dispatches (
    dispatch_id TEXT PRIMARY KEY, workflow_type TEXT NOT NULL, aggregate_id TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 1, tenant_id TEXT NOT NULL DEFAULT '_default',
    task_queue TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING', workflow_id TEXT NOT NULL,
    workflow_run_id TEXT, attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb, aggregate_revision BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    dispatched_at TIMESTAMPTZ, available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    claim_token TEXT, lease_owner TEXT, claimed_until TIMESTAMPTZ,
    failure_count INTEGER NOT NULL DEFAULT 0, failed_at TIMESTAMPTZ,
    failure_class TEXT, completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(workflow_type, aggregate_id, generation)
);
CREATE INDEX IF NOT EXISTS idx_workflow_dispatches_pending
    ON workflow_dispatches(status, available_at, created_at) WHERE status = 'PENDING';
"""
SCHEMA_SQL += SESSION_STATE_SQL + ";" + ";\n".join(SESSION_SEARCH_INDEXES[:2]) + ";"
SCHEMA_SQL += CHAT_RUNS_SQL


def _sql(sql: str) -> str:
    """Normalize internal portable SQL to psycopg parameter syntax."""
    sql = re.sub(r"\?", "%s", sql)
    return sql


class _Cursor:
    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()


class _Connection:
    def __init__(self, conn: Any) -> None:
        self.raw = conn

    def execute(self, sql: str, params: Any = None) -> _Cursor:
        cur = self.raw.cursor()
        cur.execute(_sql(sql), params or ())
        return _Cursor(cur)

    def executemany(self, sql: str, params: Any) -> _Cursor:
        cur = self.raw.cursor()
        cur.executemany(_sql(sql), params)
        return _Cursor(cur)


class _BufferedCursor:
    """Materialize an autocommit result before returning its pool connection."""

    def __init__(self, cursor: Any) -> None:
        self.rowcount = cursor.rowcount
        self._rows = list(cursor.fetchall()) if cursor.description else []
        self._index = 0

    def fetchone(self):
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row

    def fetchall(self):
        rows = self._rows[self._index :]
        self._index = len(self._rows)
        return rows


class _PooledConnection:
    def __init__(self, store: Any, tenant_id: str | None = None) -> None:
        self.store = store
        self.tenant_id = tenant_id

    def _execute(self, statement: str, params: Any, many: bool):
        with self.store.transaction() as connection:
            if self.tenant_id is not None:
                connection.execute("SELECT set_config('app.tenant_id', ?, true)", (self.tenant_id,))
            with connection.raw.cursor() as cursor:
                if many:
                    cursor.executemany(_sql(statement), params)
                else:
                    cursor.execute(_sql(statement), params or ())
                return _BufferedCursor(cursor)

    def execute(self, statement: str, params: Any = None):
        return self._execute(statement, params, False)

    def executemany(self, statement: str, params: Any):
        return self._execute(statement, params, True)


class PostgresStore(ControlPlaneQueries):
    """Production control plane backed exclusively by PostgreSQL."""

    def __init__(
        self,
        dsn: str,
        *,
        initialize_schema: bool = True,
        ingest_task_queue: str = "engram-ingest",
        code_ingest_task_queue: str = "engram-code-ingest",
        projection_task_queue: str = "engram-projection",
        consolidation_task_queue: str = "engram-consolidation",
        overview_debounce_seconds: int = 30,
    ) -> None:
        if ConnectionPool is None:
            raise RuntimeError("Postgres control plane requires psycopg[binary,pool]")
        self.dsn = dsn
        self.ingest_task_queue = ingest_task_queue
        self.code_ingest_task_queue = code_ingest_task_queue
        self.projection_task_queue = projection_task_queue
        self.consolidation_task_queue = consolidation_task_queue
        self.overview_debounce_seconds = max(0, int(overview_debounce_seconds))
        self._tls = threading.local()
        self._pool = ConnectionPool(
            conninfo=dsn,
            min_size=1,
            max_size=12,
            timeout=5.0,
            kwargs={"autocommit": True, "row_factory": dict_row},
            open=True,
        )
        if initialize_schema:
            with self._pool.connection() as raw, raw.cursor() as cur:
                cur.execute(SCHEMA_SQL)

    def close(self) -> None:
        self._pool.close()

    def get_conn(self) -> _Connection | _PooledConnection:  # type: ignore[override]
        raw = getattr(self._tls, "conn", None)
        return _Connection(raw) if raw is not None else _PooledConnection(self)

    def tenant_connection(self, tenant_id: str) -> _PooledConnection:
        return _PooledConnection(self, tenant_id)

    @contextmanager
    def transaction(self) -> Iterator[_Connection]:  # type: ignore[override]
        raw = getattr(self._tls, "conn", None)
        if raw is not None:
            with raw.transaction():
                yield _Connection(raw)
            return
        with self._pool.connection() as raw:
            self._tls.conn = raw
            try:
                with raw.transaction():
                    yield _Connection(raw)
            finally:
                self._tls.conn = None

    def record_event(
        self,
        *,
        pair_id: str,
        session_id: str | None,
        source: str,
        event_type: str,
        payload: dict[str, Any],
        tenant_id: str = DEFAULT_TENANT_ID,
        initial_status: str = "RECEIVED",
        require_matching_payload: bool = False,
    ) -> tuple[str, bool]:
        """Accept an event atomically, validating explicit request retries.

        Internal session replays retain their existing turn-pair identity and
        may omit previously supplied context. Explicit requests must opt into
        full payload validation instead of silently accepting changed input.
        """
        event_id = f"evt-{uuid.uuid4().hex}"
        normalized_status = str(initial_status).upper()
        if normalized_status not in {"RECEIVED", "COMPLETE"}:
            raise ValueError(f"unsupported initial event status: {initial_status!r}")
        processed_at = datetime.now(timezone.utc) if normalized_status == "COMPLETE" else None
        with self.transaction() as conn:
            row = conn.execute(
                "INSERT INTO events (event_id, pair_id, tenant_id, session_id, source, "
                "event_type, payload, status, processed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (tenant_id, pair_id) DO NOTHING RETURNING event_id",
                (
                    event_id,
                    pair_id,
                    tenant_id,
                    session_id,
                    source,
                    event_type,
                    json.dumps(payload),
                    normalized_status,
                    processed_at,
                ),
            ).fetchone()
            if row:
                # CODE_INGEST uploads create their own dispatch keyed by the
                # durable bulk job. They must not also enter conversation
                # ingestion through this generic event helper.
                if str(event_type).upper() == "INGEST":
                    self._enqueue_dispatch_in_tx(conn, "INGEST", event_id, tenant_id, generation=1)
                return event_id, True
            existing = conn.execute(
                "SELECT event_id, session_id, source, event_type, payload FROM events "
                "WHERE tenant_id = ? AND pair_id = ?",
                (tenant_id, pair_id),
            ).fetchone()
            if require_matching_payload and (
                existing["session_id"] != session_id
                or existing["source"] != source
                or existing["event_type"] != event_type
                or payload_fingerprint(json.loads(existing["payload"])) != payload_fingerprint(payload)
            ):
                raise IdempotencyConflictError("idempotency_key was already used for different input")
            return str(existing["event_id"]), False

    def retry_event(self, event_id: str, *, tenant_id: str) -> dict[str, Any] | None:
        """Retry the original workflow and aggregate, retaining its durable envelope."""
        with self.transaction() as conn:
            event = conn.execute(
                "SELECT * FROM events WHERE event_id = ? AND tenant_id = ? "
                "AND status = 'FAILED' FOR UPDATE", (event_id, tenant_id),
            ).fetchone()
            if event is None:
                return None
            generation_row = None
            if conn.execute("SELECT to_regclass('ingest_reprocessings') AS name").fetchone()["name"]:
                conn.execute("SELECT set_config('app.tenant_id', ?, true)", (tenant_id,))
                generation_row = conn.execute(
                    "SELECT * FROM ingest_reprocessings WHERE tenant_id = ? AND event_id = ? FOR UPDATE", (tenant_id, event_id),
                ).fetchone()
                if generation_row:
                    newer = conn.execute(
                        "SELECT 1 FROM ingest_reprocessings WHERE tenant_id = ? AND root_event_id = ? "
                        "AND event_id <> ? AND (state = 'PENDING' OR created_at > ?)",
                        (tenant_id, generation_row["root_event_id"], event_id, generation_row["created_at"]),
                    ).fetchone()
                    if newer or generation_row["state"] not in {"FAILED", "PENDING"}:
                        return None
            workflow_type = str(event["event_type"])
            if workflow_type not in {"INGEST", "CODE_INGEST"}:
                raise ValueError("this event type cannot be retried")
            event_payload = json.loads(event["payload"])
            aggregate_id = (
                str(event_payload.get("job_id") or "") if workflow_type == "CODE_INGEST" else event_id
            )
            if not aggregate_id:
                raise ValueError("code ingest event has no durable job identity")
            previous = conn.execute(
                "SELECT * FROM workflow_dispatches WHERE tenant_id = ? "
                "AND workflow_type = ? AND aggregate_id = ? ORDER BY generation DESC LIMIT 1 FOR UPDATE",
                (tenant_id, workflow_type, aggregate_id),
            ).fetchone()
            if previous and previous["status"] in {"DISPATCHING", "STARTED"}:
                return None  # terminal bookkeeping/reconciliation must finish first
            if workflow_type == "CODE_INGEST" and (
                previous is None or previous["payload"].get("event_id") != event_id
            ):
                raise ValueError("code ingest dispatch does not match the failed event")
            if previous and previous["status"] == "PENDING":
                self.mark_dispatch_dead(str(previous["dispatch_id"]), "superseded by explicit retry")
            conn.execute(
                "UPDATE events SET status = 'RECEIVED', error_message = NULL, "
                "retry_count = retry_count + 1, processed_at = NULL "
                "WHERE event_id = ? AND tenant_id = ? AND status = 'FAILED' "
                "RETURNING retry_count",
                (event_id, tenant_id),
            )
            generation = self._next_dispatch_generation_in_tx(conn, workflow_type, aggregate_id)
            self._enqueue_dispatch_in_tx(
                conn, workflow_type, aggregate_id, tenant_id, generation=generation,
                task_queue=previous["task_queue"] if previous else None,
                payload=dict(previous["payload"]) if previous else None,
                aggregate_revision=previous.get("aggregate_revision") if previous else None,
            )
            if workflow_type == "CODE_INGEST":
                self.requeue_bulk_job(aggregate_id, tenant_id=tenant_id)
            if generation_row:
                conn.execute("UPDATE ingest_reprocessings SET state = 'PENDING', updated_at = CURRENT_TIMESTAMP WHERE tenant_id = ? AND event_id = ?", (tenant_id, event_id))
        return self.get_event(event_id, tenant_id=tenant_id)

    def retry_recovered_provider_events(
        self, *, tenant_id: str, healthy_at: datetime, limit: int = 200
    ) -> int:
        """One audited generation after a fresh provider success, tenant scoped.

        Unknown/permanent errors remain terminal. The durable dispatch marker
        prevents a second automatic recovery even after another failed run or
        after audit-log retention. Status, dispatch and audit commit together.
        """
        now = datetime.now(timezone.utc)
        if healthy_at.tzinfo is None or not 0 <= (now - healthy_at).total_seconds() <= 300:
            return 0
        if limit <= 0:
            return 0
        policy = "provider-recovery-v1"
        # Temporal includes the original exception class at the beginning of
        # ApplicationError text. Never infer retryability from a random '503'
        # inside a validation message or source content.
        transient = r"^(APITimeoutError|APIConnectionError|RateLimitError|InternalServerError|CircuitOpenError):"
        with self.transaction() as conn:
            rows = conn.execute(
                "SELECT e.event_id, e.error_message FROM events e "
                "WHERE e.tenant_id = ? AND e.status = 'FAILED' AND e.event_type = 'INGEST' "
                "AND e.processed_at <= ? AND e.processed_at <= CURRENT_TIMESTAMP - INTERVAL '30 seconds' "
                "AND e.error_message ~ ? "
                "AND NOT EXISTS (SELECT 1 FROM workflow_dispatches d WHERE d.tenant_id=e.tenant_id "
                "AND d.aggregate_id=e.event_id AND d.workflow_type='INGEST' "
                "AND (d.status NOT IN ('DEAD','COMPLETE') OR d.payload->>'recovery_policy' = ?)) "
                "ORDER BY e.processed_at, e.event_id FOR UPDATE OF e SKIP LOCKED LIMIT ?",
                (tenant_id, healthy_at, transient, policy, min(limit, 200)),
            ).fetchall()
            for row in rows:
                event_id = str(row["event_id"])
                generation = self._next_dispatch_generation_in_tx(conn, "INGEST", event_id)
                details = {
                    "recovery_policy": policy,
                    "generation": generation,
                    "provider_success_at": healthy_at.isoformat(),
                    "previous_error_type": str(row["error_message"]).split(":", 1)[0],
                }
                conn.execute(
                    "UPDATE events SET status='RECEIVED', error_message=NULL, processed_at=NULL, "
                    "retry_count=retry_count+1 WHERE event_id=? AND tenant_id=? AND status='FAILED'",
                    (event_id, tenant_id),
                )
                self._enqueue_dispatch_in_tx(
                    conn, "INGEST", event_id, tenant_id, generation=generation, payload=details
                )
                conn.execute(
                    "INSERT INTO audit_log (audit_id,tenant_id,actor,action,target,details) "
                    "VALUES (?,?,?,?,?,?)",
                    (f"aud-{uuid.uuid4().hex}", tenant_id, "provider-reconciliation",
                     "ingest.provider_recovery", event_id, json.dumps(details)),
                )
            return len(rows)

    def requeue_event(
        self,
        event_id: str,
        *,
        increment_retry: bool = True,
    ) -> bool:
        """Requeue an event and its Temporal workflow atomically."""
        retry_sql = "retry_count = retry_count + 1, " if increment_retry else ""
        with self.transaction() as conn:
            row = conn.execute(
                f"UPDATE events SET status = 'RECEIVED', {retry_sql}"
                "error_message = NULL, processed_at = NULL WHERE event_id = ? "
                "RETURNING tenant_id",
                (event_id,),
            ).fetchone()
            if row is None:
                return False
            generation = self._next_dispatch_generation_in_tx(conn, "INGEST", event_id)
            self._enqueue_dispatch_in_tx(
                conn,
                "INGEST",
                event_id,
                str(row["tenant_id"]),
                generation=generation,
            )
        return True

    @staticmethod
    def _next_dispatch_generation_in_tx(
        conn: _Connection,
        workflow_type: str,
        aggregate_id: str,
    ) -> int:
        row = conn.execute(
            "SELECT COALESCE(MAX(generation), 0) + 1 AS generation "
            "FROM workflow_dispatches WHERE workflow_type = ? AND aggregate_id = ?",
            (workflow_type, aggregate_id),
        ).fetchone()
        return int(row["generation"])

    def _enqueue_dispatch_in_tx(
        self,
        conn: _Connection,
        workflow_type: str,
        aggregate_id: str,
        tenant_id: str,
        *,
        generation: int = 1,
        task_queue: str | None = None,
        available_at: datetime | None = None,
        payload: dict[str, Any] | None = None,
        aggregate_revision: int | None = None,
    ) -> str:
        dispatch_id = f"dsp-{uuid.uuid4().hex[:16]}"
        prefixes = {
            "INGEST": "ingest",
            "CODE_INGEST": "code-ingest",
            "PROJECTION": "projection",
            "CONSOLIDATION": "consolidation",
        }
        queues = {
            "INGEST": getattr(self, "ingest_task_queue", "engram-ingest"),
            "CODE_INGEST": getattr(self, "code_ingest_task_queue", "engram-code-ingest"),
            "PROJECTION": getattr(self, "projection_task_queue", "engram-projection"),
            "CONSOLIDATION": getattr(self, "consolidation_task_queue", "engram-consolidation"),
        }
        prefix = prefixes.get(workflow_type)
        if prefix is None:
            raise ValueError(f"unsupported workflow type: {workflow_type}")
        default_queue = queues[workflow_type]
        queue = task_queue or default_queue
        workflow_id = (
            f"projection:{aggregate_id}"
            if workflow_type == "PROJECTION"
            else f"{prefix}:{aggregate_id}:{generation}"
        )
        row = conn.execute(
            "INSERT INTO workflow_dispatches (dispatch_id, workflow_type, aggregate_id, generation, "
            "tenant_id, task_queue, workflow_id, payload, aggregate_revision, available_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (workflow_type, aggregate_id, generation) DO NOTHING RETURNING dispatch_id",
            (
                dispatch_id,
                workflow_type,
                aggregate_id,
                generation,
                tenant_id,
                queue,
                workflow_id,
                json.dumps(payload or {}),
                aggregate_revision,
                available_at or datetime.now(timezone.utc),
            ),
        ).fetchone()
        return str(row["dispatch_id"]) if row else ""

    def enqueue_projection_dispatch(
        self,
        *,
        canonical_mutation_id: str | None = None,
        tenant_id: str,
        available_at: datetime | None = None,
        task_queue: str | None = None,
        payload: dict[str, Any] | None = None,
        aggregate_revision: int | None = None,
    ) -> str | None:
        """Enqueue one deterministic Temporal projection workflow.

        ``workflow_dispatches`` is the sole transactional outbox.  The aggregate
        ID should be the canonical mutation ID; the stable memory/claim IDs and
        any compact projection envelope travel in ``payload``.  The method is
        also useful to adapters that have already committed the canonical
        mutation, but canonical writers should call ``_enqueue_dispatch_in_tx``
        in their transaction.
        """
        if not canonical_mutation_id:
            raise ValueError("canonical_mutation_id is required")
        with self.transaction() as conn:
            dispatch_id = self._enqueue_dispatch_in_tx(
                conn,
                "PROJECTION",
                canonical_mutation_id,
                tenant_id,
                task_queue=task_queue,
                available_at=available_at,
                payload=payload,
                aggregate_revision=aggregate_revision,
            )
        return dispatch_id or None

    def enqueue_code_ingest_dispatch(
        self,
        *,
        job_id: str,
        tenant_id: str,
        available_at: datetime | None = None,
        task_queue: str | None = None,
        payload: dict[str, Any] | None = None,
        aggregate_revision: int | None = None,
    ) -> str | None:
        """Enqueue an opaque code-ingest job on the dedicated Temporal queue."""
        if not job_id:
            raise ValueError("job_id is required")
        with self.transaction() as conn:
            dispatch_id = self._enqueue_dispatch_in_tx(
                conn,
                "CODE_INGEST",
                job_id,
                tenant_id,
                task_queue=task_queue,
                available_at=available_at,
                payload=payload,
                aggregate_revision=aggregate_revision,
            )
        return dispatch_id or None

    def get_dispatch(
        self,
        dispatch_id: str,
        *,
        tenant_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Read one dispatch envelope for a Temporal Activity.

        Projection Activities use this envelope to obtain the canonical
        mutation ID, stable IDs/revision, and compact payload persisted by the
        same PostgreSQL transaction that changed canonical state.
        """
        if not dispatch_id:
            return None
        sql = "SELECT * FROM workflow_dispatches WHERE dispatch_id = ?"
        params: list[Any] = [dispatch_id]
        if tenant_id is not None:
            sql += " AND tenant_id = ?"
            params.append(tenant_id)
        return self.get_conn().execute(sql, tuple(params)).fetchone()

    def get_dispatch_for_aggregate(
        self,
        workflow_type: str,
        aggregate_id: str,
        *,
        tenant_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Return the newest dispatch for an aggregate as a compatibility lookup."""
        sql = "SELECT * FROM workflow_dispatches WHERE workflow_type = ? AND aggregate_id = ?"
        params: list[Any] = [workflow_type, aggregate_id]
        if tenant_id is not None:
            sql += " AND tenant_id = ?"
            params.append(tenant_id)
        sql += " ORDER BY generation DESC LIMIT 1"
        return self.get_conn().execute(sql, tuple(params)).fetchone()

    def enqueue_task(
        self,
        *,
        node_id: str,
        task_type: str,
        priority: int = 5,
        tenant_id: str = DEFAULT_TENANT_ID,
        mark_dirty: bool = False,
    ) -> str | None:
        task_id = f"task-{uuid.uuid4().hex[:12]}"
        with self.transaction() as conn:
            row = conn.execute(
                "INSERT INTO consolidation_tasks "
                "(task_id, tenant_id, node_id, task_type, priority, scheduled_at) "
                "VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP) "
                "ON CONFLICT DO NOTHING RETURNING task_id",
                (task_id, tenant_id, node_id, task_type, priority),
            ).fetchone()
            if not row:
                if mark_dirty:
                    conn.execute(
                        "UPDATE consolidation_tasks SET requested_generation = requested_generation + 1 "
                        "WHERE tenant_id = ? AND node_id = ? AND task_type = ? "
                        "AND status IN ('PENDING', 'PROCESSING')",
                        (tenant_id, node_id, task_type),
                    )
                return None
            self._enqueue_dispatch_in_tx(conn, "CONSOLIDATION", task_id, tenant_id)
            return task_id

    def finish_consolidation_task(self, task_id: str, processed_generation: int) -> str | None:
        """Complete a generation and durably hand off changes arriving during its run."""
        with self.transaction() as conn:
            task = conn.execute(
                "SELECT * FROM consolidation_tasks WHERE task_id = ? FOR UPDATE", (task_id,)
            ).fetchone()
            if task is None or task["status"] == "COMPLETE":
                return None
            if task["status"] != "PROCESSING":
                raise ValueError("only a processing consolidation task can complete")
            conn.execute(
                "UPDATE consolidation_tasks SET status = 'COMPLETE', "
                "completed_at = CURRENT_TIMESTAMP, error_message = NULL WHERE task_id = ?",
                (task_id,),
            )
            self.mark_aggregate_dispatches_complete("CONSOLIDATION", task_id)
            if task["requested_generation"] > processed_generation:
                return self.enqueue_task(
                    node_id=task["node_id"],
                    task_type=task["task_type"],
                    priority=task["priority"],
                    tenant_id=task["tenant_id"],
                )
        return None

    def claim_dispatches(
        self,
        *,
        limit: int = 32,
        lease_seconds: int = 300,
        lease_owner: str | None = None,
    ) -> list[dict[str, Any]]:
        """Claim due work with fenced leases so dispatcher replicas cannot double-start it."""
        if limit < 1:
            return []
        lease_seconds = max(1, int(lease_seconds))
        owner = lease_owner or f"dispatcher-{uuid.uuid4().hex}"
        with self.transaction() as conn:
            # A dispatcher can die after claiming a row but before it contacts
            # Temporal.  Expired claims are made available again, while an
            # active lease remains fenced from other dispatcher replicas.
            conn.execute(
                "UPDATE workflow_dispatches SET status = 'PENDING', claim_token = NULL, "
                "lease_owner = NULL, claimed_until = NULL, updated_at = CURRENT_TIMESTAMP "
                "WHERE status = 'DISPATCHING' AND ("
                "(claimed_until IS NOT NULL AND claimed_until <= CURRENT_TIMESTAMP) OR "
                "(claimed_until IS NULL AND updated_at < CURRENT_TIMESTAMP - INTERVAL '5 minutes')"
                ")"
            )
            picked = conn.execute(
                "SELECT dispatch_id FROM workflow_dispatches "
                "WHERE status = 'PENDING' AND available_at <= CURRENT_TIMESTAMP "
                "ORDER BY available_at, created_at LIMIT ? FOR UPDATE SKIP LOCKED",
                (limit,),
            ).fetchall()
            rows: list[dict[str, Any]] = []
            for picked_row in picked:
                token = uuid.uuid4().hex
                row = conn.execute(
                    "UPDATE workflow_dispatches SET status = 'DISPATCHING', "
                    "attempts = attempts + 1, claim_token = ?, lease_owner = ?, "
                    "claimed_until = CURRENT_TIMESTAMP + (? * INTERVAL '1 second'), "
                    "updated_at = CURRENT_TIMESTAMP WHERE dispatch_id = ? AND status = 'PENDING' "
                    "RETURNING *",
                    (token, owner, lease_seconds, picked_row["dispatch_id"]),
                ).fetchone()
                if row:
                    rows.append(dict(row))
        return rows

    def mark_dispatch_started(
        self,
        dispatch_id: str,
        run_id: str | None = None,
        *,
        claim_token: str,
    ) -> bool:
        if not claim_token:
            raise ValueError("claim_token is required to mark a dispatch started")
        where = "dispatch_id = ? AND status = 'DISPATCHING'"
        params: list[Any] = [run_id, dispatch_id]
        where += " AND claim_token = ?"
        params.append(claim_token)
        with self.transaction() as conn:
            result = conn.execute(
                "UPDATE workflow_dispatches SET status = 'STARTED', workflow_run_id = ?, "
                "dispatched_at = CURRENT_TIMESTAMP, claimed_until = NULL, "
                "claim_token = NULL, lease_owner = NULL, updated_at = CURRENT_TIMESTAMP "
                f"WHERE {where}",
                tuple(params),
            )
        return result.rowcount > 0

    def mark_aggregate_dispatches_complete(
        self,
        workflow_type: str,
        aggregate_id: str,
    ) -> None:
        """Close active outbox rows after their durable aggregate finishes."""
        with self.transaction() as conn:
            conn.execute(
                "UPDATE workflow_dispatches SET status = 'COMPLETE', "
                "completed_at = CURRENT_TIMESTAMP, claim_token = NULL, lease_owner = NULL, "
                "claimed_until = NULL, updated_at = CURRENT_TIMESTAMP WHERE workflow_type = ? "
                "AND aggregate_id = ? "
                "AND status IN ('PENDING', 'DISPATCHING', 'STARTED')",
                (workflow_type, aggregate_id),
            )

    def mark_aggregate_dispatches_dead(
        self,
        workflow_type: str,
        aggregate_id: str,
        error: str,
        *,
        failure_class: str = "WORKFLOW_TERMINAL",
    ) -> None:
        """Mark active dispatches terminal when their workflow has failed."""

        with self.transaction() as conn:
            conn.execute(
                "UPDATE workflow_dispatches SET status = 'DEAD', last_error = ?, "
                "failure_class = ?, failure_count = failure_count + 1, "
                "failed_at = CURRENT_TIMESTAMP, claim_token = NULL, lease_owner = NULL, "
                "claimed_until = NULL, updated_at = CURRENT_TIMESTAMP "
                "WHERE workflow_type = ? AND aggregate_id = ? "
                "AND status IN ('PENDING', 'DISPATCHING', 'STARTED')",
                (error[:1000], failure_class[:100], workflow_type, aggregate_id),
            )

    def complete_dispatch(self, dispatch_id: str, *, claim_token: str | None = None) -> bool:
        """Mark one dispatch complete, fenced to its current claim when supplied."""
        where = "dispatch_id = ? AND status IN ('DISPATCHING', 'STARTED')"
        params: list[Any] = [dispatch_id]
        if claim_token:
            where += " AND claim_token = ?"
            params.append(claim_token)
        with self.transaction() as conn:
            result = conn.execute(
                "UPDATE workflow_dispatches SET status = 'COMPLETE', "
                "completed_at = CURRENT_TIMESTAMP, claim_token = NULL, lease_owner = NULL, "
                "claimed_until = NULL, updated_at = CURRENT_TIMESTAMP "
                f"WHERE {where}",
                tuple(params),
            )
        return result.rowcount > 0

    def release_dispatch(
        self,
        dispatch_id: str,
        error: str,
        *,
        claim_token: str,
        delay_seconds: int | None = None,
    ) -> bool:
        """Release only a claim still owned by the dispatcher.

        A workflow can finish between ``start_workflow`` reaching Temporal
        and the client receiving its response.  In that race the activity
        marks the row COMPLETE first; an unconditional update here would
        incorrectly reopen completed work.
        """
        if not claim_token:
            raise ValueError("claim_token is required to release a dispatch")
        where = "dispatch_id = ? AND status = 'DISPATCHING'"
        params: list[Any] = [error[:1000]]
        if delay_seconds is None:
            available_sql = (
                "CURRENT_TIMESTAMP + LEAST(POWER(2, LEAST(failure_count, 10)) "
                "* INTERVAL '1 second', INTERVAL '5 minutes')"
            )
        else:
            available_sql = "CURRENT_TIMESTAMP + (? * INTERVAL '1 second')"
            params.append(max(0, int(delay_seconds)))
        params.append(dispatch_id)
        where += " AND claim_token = ?"
        params.append(claim_token)
        with self.transaction() as conn:
            result = conn.execute(
                "UPDATE workflow_dispatches SET status = 'PENDING', last_error = ?, "
                "failure_class = 'DISPATCH_TRANSIENT', failure_count = failure_count + 1, "
                "available_at = "
                f"{available_sql}, claim_token = NULL, lease_owner = NULL, "
                "claimed_until = NULL, updated_at = CURRENT_TIMESTAMP "
                f"WHERE {where}",
                tuple(params),
            )
        return result.rowcount > 0

    def mark_dispatch_failed(
        self,
        dispatch_id: str,
        error: str,
        *,
        claim_token: str,
        failure_class: str = "DISPATCH_TERMINAL",
    ) -> bool:
        """Quarantine a poison outbox row instead of retrying it forever."""
        if not claim_token:
            raise ValueError("claim_token is required to fail a dispatch")
        where = "dispatch_id = ? AND status = 'DISPATCHING'"
        params: list[Any] = [error[:1000], failure_class[:100], dispatch_id]
        where += " AND claim_token = ?"
        params.append(claim_token)
        with self.transaction() as conn:
            result = conn.execute(
                "UPDATE workflow_dispatches SET status = 'DEAD', "
                "last_error = ?, failure_class = ?, failure_count = failure_count + 1, "
                "failed_at = CURRENT_TIMESTAMP, claim_token = NULL, lease_owner = NULL, "
                "claimed_until = NULL, updated_at = CURRENT_TIMESTAMP "
                f"WHERE {where}",
                tuple(params),
            )
        return result.rowcount > 0

    def mark_dispatch_dead(
        self,
        dispatch_id: str,
        error: str,
        *,
        failure_class: str = "WORKFLOW_TERMINAL",
    ) -> bool:
        """Record a terminal Temporal failure after the lease was acknowledged.

        Dispatcher-owned transitions require the claim token.  Once Temporal
        has accepted a workflow, the dispatcher clears that token; the
        workflow's terminal bookkeeping therefore uses this separate method and
        only touches the same dispatch row in an active state.
        """
        with self.transaction() as conn:
            result = conn.execute(
                "UPDATE workflow_dispatches SET status = 'DEAD', "
                "last_error = ?, failure_class = ?, failure_count = failure_count + 1, "
                "failed_at = CURRENT_TIMESTAMP, claim_token = NULL, lease_owner = NULL, "
                "claimed_until = NULL, updated_at = CURRENT_TIMESTAMP "
                "WHERE dispatch_id = ? AND status IN ('PENDING', 'DISPATCHING', 'STARTED')",
                (error[:1000], failure_class[:100], dispatch_id),
            )
        return result.rowcount > 0

    def repair_dispatches(self, *, limit: int = 200) -> dict[str, int]:
        """Create missing workflow outbox rows without duplicating active work.

        Temporal owns retries for workflows that already have an active
        dispatch. This repair pass only addresses control-plane rows for which
        no PENDING, DISPATCHING, or STARTED dispatch exists.
        """
        counts = {"ingest": 0, "consolidation": 0, "projection": 0}
        with self.transaction() as conn:
            events = conn.execute(
                "SELECT e.event_id, e.tenant_id FROM events e "
                "WHERE e.event_type = 'INGEST' "
                "AND e.status IN ('RECEIVED', 'PROCESSING', 'GATED_STORE', 'INDEXED') "
                "AND NOT EXISTS (SELECT 1 FROM workflow_dispatches d "
                "WHERE d.workflow_type = 'INGEST' AND d.aggregate_id = e.event_id "
                "AND d.status IN ('PENDING', 'DISPATCHING', 'STARTED')) "
                "ORDER BY e.created_at LIMIT ? FOR UPDATE SKIP LOCKED",
                (limit,),
            ).fetchall()
            for event in events:
                event_id = str(event["event_id"])
                generation = self._next_dispatch_generation_in_tx(conn, "INGEST", event_id)
                if self._enqueue_dispatch_in_tx(
                    conn,
                    "INGEST",
                    event_id,
                    str(event["tenant_id"]),
                    generation=generation,
                ):
                    counts["ingest"] += 1

            tasks = conn.execute(
                "SELECT t.task_id, t.tenant_id FROM consolidation_tasks t "
                "WHERE t.status IN ('PENDING', 'PROCESSING') "
                "AND NOT EXISTS (SELECT 1 FROM workflow_dispatches d "
                "WHERE d.workflow_type = 'CONSOLIDATION' "
                "AND d.aggregate_id = t.task_id "
                "AND d.status IN ('PENDING', 'DISPATCHING', 'STARTED')) "
                "ORDER BY t.created_at LIMIT ? FOR UPDATE SKIP LOCKED",
                (limit,),
            ).fetchall()
            for task in tasks:
                task_id = str(task["task_id"])
                generation = self._next_dispatch_generation_in_tx(conn, "CONSOLIDATION", task_id)
                if self._enqueue_dispatch_in_tx(
                    conn,
                    "CONSOLIDATION",
                    task_id,
                    str(task["tenant_id"]),
                    generation=generation,
                ):
                    counts["consolidation"] += 1

            # Projection dependencies and Neo4j deadlocks are transient. A
            # workflow can still exhaust its bounded activity retry budget
            # during a large ingest burst, so reconciliation creates a new
            # generation after the failed execution has closed. The stable
            # projection workflow ID is safe to reuse after a failed run.
            projections = conn.execute(
                "SELECT d.* FROM workflow_dispatches d "
                "WHERE d.workflow_type = 'PROJECTION' AND d.status = 'DEAD' "
                "AND d.failure_class = 'PROJECTION_TRANSIENT' "
                "AND d.updated_at < CURRENT_TIMESTAMP - INTERVAL '10 seconds' "
                "AND d.generation = (SELECT MAX(newer.generation) "
                "FROM workflow_dispatches newer "
                "WHERE newer.workflow_type = d.workflow_type "
                "AND newer.aggregate_id = d.aggregate_id) "
                "ORDER BY d.updated_at LIMIT ? FOR UPDATE SKIP LOCKED",
                (limit,),
            ).fetchall()
            for projection in projections:
                raw_payload = projection.get("payload") or {}
                if isinstance(raw_payload, str):
                    try:
                        raw_payload = json.loads(raw_payload)
                    except json.JSONDecodeError:
                        raw_payload = {}
                payload = raw_payload if isinstance(raw_payload, dict) else {}
                generation = self._next_dispatch_generation_in_tx(
                    conn,
                    "PROJECTION",
                    str(projection["aggregate_id"]),
                )
                if self._enqueue_dispatch_in_tx(
                    conn,
                    "PROJECTION",
                    str(projection["aggregate_id"]),
                    str(projection["tenant_id"]),
                    generation=generation,
                    task_queue=str(projection["task_queue"]),
                    payload=payload,
                    aggregate_revision=projection.get("aggregate_revision"),
                ):
                    counts["projection"] += 1
        return counts

    def started_dispatches(self, *, limit: int = 64) -> list[dict[str, Any]]:
        """Rotate through acknowledged executions, including long-lived workflows."""
        return list(self.get_conn().execute(
            "SELECT * FROM workflow_dispatches WHERE status = 'STARTED' "
            "AND updated_at < CURRENT_TIMESTAMP - INTERVAL '30 seconds' "
            "ORDER BY updated_at, dispatch_id LIMIT ?", (limit,),
        ).fetchall())

    def touch_dispatch_inspected(self, dispatch_id: str, run_id: str | None) -> None:
        """Rotate unavailable/expired visibility records without inventing a status."""
        with self.transaction() as conn:
            conn.execute(
                "UPDATE workflow_dispatches SET updated_at = CURRENT_TIMESTAMP "
                "WHERE dispatch_id = ? AND status = 'STARTED' AND workflow_run_id IS NOT DISTINCT FROM ?",
                (dispatch_id, run_id),
            )

    def fail_ingest_execution(
        self, workflow_type: str, aggregate_id: str, error: str, *,
        workflow_id: str | None = None, run_id: str | None = None,
    ) -> bool:
        """Record event and dispatch failure together without undoing committed work."""
        snapshot = self.get_dispatch_for_aggregate(workflow_type, aggregate_id)
        if snapshot is None:
            return False
        event_id = aggregate_id if workflow_type == "INGEST" else snapshot["payload"].get("event_id")
        tenant = snapshot["tenant_id"]
        with self.transaction() as conn:
            event = conn.execute(
                "SELECT * FROM events WHERE event_id = ? AND tenant_id = ? FOR UPDATE", (event_id, tenant),
            ).fetchone()
            row = conn.execute(
                "SELECT * FROM workflow_dispatches WHERE workflow_type = ? AND aggregate_id = ? "
                "ORDER BY generation DESC LIMIT 1 FOR UPDATE", (workflow_type, aggregate_id),
            ).fetchone()
            if row is None or (workflow_id and row["workflow_id"] != workflow_id) or (
                run_id and row["workflow_run_id"] and row["workflow_run_id"] != run_id
            ):
                return False
            if event and event["status"] in {"COMPLETE", "GATED_SKIP"}:
                self.mark_aggregate_dispatches_complete(workflow_type, aggregate_id)
                return False
            if row["status"] not in {"PENDING", "DISPATCHING", "STARTED", "DEAD"}:
                return False
            if event:
                conn.execute(
                    "UPDATE events SET status = 'FAILED', error_message = ?, processed_at = CURRENT_TIMESTAMP "
                    "WHERE event_id = ? AND tenant_id = ?", (error[:1000], event_id, tenant),
                )
                self._fail_reprocessing_in_tx(conn, tenant, event_id)
            self.mark_dispatch_dead(row["dispatch_id"], error)
            return True

    @staticmethod
    def _fail_reprocessing_in_tx(conn: Any, tenant: str, event_id: str) -> None:
        if conn.execute("SELECT to_regclass('ingest_reprocessings') AS name").fetchone()["name"]:
            conn.execute("SELECT set_config('app.tenant_id', ?, true)", (tenant,))
            conn.execute(
                "UPDATE ingest_reprocessings SET state = 'FAILED', updated_at = CURRENT_TIMESTAMP "
                "WHERE tenant_id = ? AND event_id = ? AND state = 'PENDING'", (tenant, event_id),
            )

    def reconcile_workflow_execution(
        self, dispatch_id: str, *, run_id: str | None, workflow_status: str,
    ) -> bool:
        """Apply a verified execution status, fenced to this dispatch/run/generation.

        Network lookups happen before this short transaction. Unknown visibility
        (including retention expiry) is never evidence of workflow failure.
        """
        snapshot = self.get_dispatch(dispatch_id)
        if snapshot is None:
            return False
        tenant = snapshot["tenant_id"]
        kind = snapshot["workflow_type"]
        aggregate = snapshot["aggregate_id"]
        payload = snapshot["payload"]
        event_id = aggregate if kind == "INGEST" else payload.get("event_id") if kind == "CODE_INGEST" else None
        terminal_failures = {"FAILED", "CANCELED", "TERMINATED", "TIMED_OUT"}
        if workflow_status not in terminal_failures | {"RUNNING", "COMPLETED"}:
            return False
        with self.transaction() as conn:
            # Use the same aggregate-before-dispatch lock order as explicit retry.
            event = conn.execute(
                "SELECT * FROM events WHERE tenant_id = ? AND event_id = ? FOR UPDATE",
                (tenant, event_id),
            ).fetchone() if event_id else None
            task = conn.execute(
                "SELECT * FROM consolidation_tasks WHERE tenant_id = ? AND task_id = ? FOR UPDATE",
                (tenant, aggregate),
            ).fetchone() if kind == "CONSOLIDATION" else None
            row = conn.execute(
                "SELECT * FROM workflow_dispatches WHERE dispatch_id = ? "
                "AND status = 'STARTED' AND workflow_run_id IS NOT DISTINCT FROM ? FOR UPDATE",
                (dispatch_id, run_id),
            ).fetchone()
            if row is None:
                return False
            if workflow_status == "RUNNING":
                conn.execute("UPDATE workflow_dispatches SET updated_at = CURRENT_TIMESTAMP WHERE dispatch_id = ?", (dispatch_id,))
                return False
            newer = conn.execute(
                "SELECT 1 FROM workflow_dispatches WHERE workflow_type = ? AND aggregate_id = ? AND generation > ?",
                (kind, aggregate, row["generation"]),
            ).fetchone()
            durable_success = bool(
                (event and event["status"] in {"COMPLETE", "GATED_SKIP"})
                or (task and task["status"] == "COMPLETE")
                or (kind == "PROJECTION" and workflow_status == "COMPLETED")
            )
            if durable_success:
                self.complete_dispatch(dispatch_id)
                return True
            error = f"Temporal execution {workflow_status}; terminal state reconciled from workflow visibility"
            if workflow_status == "COMPLETED":
                error = "Temporal execution completed without durable aggregate completion"
            self.mark_dispatch_dead(dispatch_id, error, failure_class="WORKFLOW_" + workflow_status)
            if not newer:
                if event:
                    conn.execute(
                        "UPDATE events SET status = 'FAILED', error_message = ?, processed_at = CURRENT_TIMESTAMP "
                        "WHERE tenant_id = ? AND event_id = ? AND status NOT IN ('COMPLETE', 'GATED_SKIP')",
                        (error, tenant, event_id),
                    )
                    self._fail_reprocessing_in_tx(conn, tenant, event_id)
                if task:
                    conn.execute(
                        "UPDATE consolidation_tasks SET status = 'FAILED', error_message = ?, completed_at = CURRENT_TIMESTAMP "
                        "WHERE tenant_id = ? AND task_id = ? AND status <> 'COMPLETE'", (error, tenant, aggregate),
                    )
            return True

    def rebuild_dispatches(self) -> dict[str, int]:
        """Recreate dispatch rows after a stopped legacy-worker cutover."""
        counts = {"ingest": 0, "consolidation": 0}
        with self.transaction() as conn:
            conn.execute("UPDATE events SET status = 'RECEIVED' WHERE status = 'PROCESSING'")
            conn.execute(
                "UPDATE consolidation_tasks SET status = 'PENDING', started_at = NULL "
                "WHERE status = 'PROCESSING'"
            )
            events = conn.execute(
                "SELECT event_id, tenant_id FROM events "
                "WHERE event_type = 'INGEST' "
                "AND status IN ('RECEIVED', 'GATED_STORE', 'INDEXED')"
            ).fetchall()
            for event in events:
                if self._enqueue_dispatch_in_tx(
                    conn,
                    "INGEST",
                    str(event["event_id"]),
                    str(event["tenant_id"]),
                    generation=self._next_dispatch_generation_in_tx(
                        conn, "INGEST", str(event["event_id"])
                    ),
                ):
                    counts["ingest"] += 1
            tasks = conn.execute(
                "SELECT task_id, tenant_id FROM consolidation_tasks WHERE status = 'PENDING'"
            ).fetchall()
            for task in tasks:
                if self._enqueue_dispatch_in_tx(
                    conn,
                    "CONSOLIDATION",
                    str(task["task_id"]),
                    str(task["tenant_id"]),
                    generation=self._next_dispatch_generation_in_tx(
                        conn, "CONSOLIDATION", str(task["task_id"])
                    ),
                ):
                    counts["consolidation"] += 1
        return counts

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
                "INSERT INTO extractions (event_id, tenant_id, resolved_text, triplets, l0_abstract) "
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT (event_id) DO UPDATE SET "
                "tenant_id=EXCLUDED.tenant_id, resolved_text=EXCLUDED.resolved_text, "
                "triplets=EXCLUDED.triplets, l0_abstract=EXCLUDED.l0_abstract",
                (event_id, tenant_id, resolved_text, json.dumps(triplets), l0_abstract),
            )

    def fs_outbox_write(
        self, event_id: str, source_uri: str, *, tenant_id: str = DEFAULT_TENANT_ID
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO fs_outbox (event_id, tenant_id, source_uri, state, written_at) "
                "VALUES (?, ?, ?, 'WRITTEN', CURRENT_TIMESTAMP) ON CONFLICT (event_id) DO UPDATE SET "
                "tenant_id=EXCLUDED.tenant_id, source_uri=EXCLUDED.source_uri, state='WRITTEN', "
                "written_at=CURRENT_TIMESTAMP",
                (event_id, tenant_id, source_uri),
            )

    def save_linked_entities(
        self,
        rows: list[tuple[str, str, int, str | None, str | None]],
    ) -> None:
        """Persist resolved endpoints using PostgreSQL UPSERT syntax."""
        if not rows:
            return
        with self.transaction() as conn:
            conn.executemany(
                "INSERT INTO linked_entities "
                "(event_id, tenant_id, triplet_idx, subject_node_id, object_node_id) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT (event_id, triplet_idx) DO UPDATE SET "
                "tenant_id = EXCLUDED.tenant_id, "
                "subject_node_id = EXCLUDED.subject_node_id, "
                "object_node_id = EXCLUDED.object_node_id",
                rows,
            )

    def save_bulk_job(self, **kwargs: Any) -> None:
        # Preserve the existing API's replace semantics with a portable UPSERT.
        status = kwargs["status"]
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO bulk_jobs (job_id, tenant_id, source, filename, dry_run, status, total_count, "
                "accepted_count, rejected_count, rejected_rows, event_ids, completed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END) "
                "ON CONFLICT (job_id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id, source=EXCLUDED.source, "
                "filename=EXCLUDED.filename, dry_run=EXCLUDED.dry_run, status=EXCLUDED.status, "
                "total_count=EXCLUDED.total_count, accepted_count=EXCLUDED.accepted_count, "
                "rejected_count=EXCLUDED.rejected_count, rejected_rows=EXCLUDED.rejected_rows, "
                "event_ids=EXCLUDED.event_ids, completed_at=EXCLUDED.completed_at",
                (
                    kwargs["job_id"],
                    kwargs["tenant_id"],
                    kwargs["source"],
                    kwargs["filename"],
                    kwargs["dry_run"],
                    status,
                    kwargs["total_count"],
                    kwargs["accepted_count"],
                    kwargs["rejected_count"],
                    json.dumps(kwargs["rejected_rows"]),
                    json.dumps(kwargs["event_ids"]),
                    status != "QUEUED",
                ),
            )
