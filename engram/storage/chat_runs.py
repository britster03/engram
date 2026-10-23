"""Durable user acceptance and independent response runs, scoped by tenant."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

CHAT_RUNS_SQL = """
CREATE TABLE IF NOT EXISTS chat_runs (
    tenant_id TEXT NOT NULL, run_id TEXT NOT NULL, session_id TEXT NOT NULL,
    client_message_id TEXT NOT NULL, input_event_id TEXT NOT NULL REFERENCES events(event_id),
    input_hash TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'RUNNING'
      CHECK (status IN ('RUNNING','COMPLETED','FAILED','CANCELLED')),
    response JSONB, error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deadline TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP + INTERVAL '5 minutes',
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, run_id), UNIQUE (tenant_id, client_message_id),
    FOREIGN KEY (tenant_id, session_id) REFERENCES session_states(tenant_id, session_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_runs_active_session
    ON chat_runs(tenant_id, session_id) WHERE status = 'RUNNING';
CREATE INDEX IF NOT EXISTS idx_chat_runs_deadline ON chat_runs(deadline) WHERE status = 'RUNNING';
"""


class ChatConflictError(ValueError):
    def __init__(self, message: str, *, run: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.run = dict(run) if run else None


def find_session_run(
    store: Any, tenant: str, session_id: str, *, client_message_id: str | None = None
) -> dict[str, Any] | None:
    """Discover an accepted submission even when its SSE acknowledgement was lost."""
    with store.transaction() as conn:
        conn.execute(
            "UPDATE chat_runs SET status = 'FAILED', error_code = 'deadline_exceeded', "
            "completed_at = CURRENT_TIMESTAMP WHERE tenant_id = ? AND session_id = ? "
            "AND status = 'RUNNING' AND deadline <= CURRENT_TIMESTAMP",
            (tenant, session_id),
        )
        return conn.execute(
            "SELECT * FROM chat_runs WHERE tenant_id = ? AND session_id = ? "
            + ("AND client_message_id = ? " if client_message_id else "")
            + "ORDER BY created_at DESC, run_id DESC LIMIT 1",
            (tenant, session_id, client_message_id) if client_message_id else (tenant, session_id),
        ).fetchone()


def find_run(
    store: Any, tenant: str, *, client_message_id: str | None = None, run_id: str | None = None
) -> dict[str, Any] | None:
    if not client_message_id and not run_id:
        return None
    field, value = (
        ("client_message_id", client_message_id) if client_message_id else ("run_id", run_id)
    )
    with store.transaction() as conn:
        conn.execute(
            "UPDATE chat_runs SET status = 'FAILED', error_code = 'deadline_exceeded', "
            "completed_at = CURRENT_TIMESTAMP WHERE tenant_id = ? AND " + field + " = ? "
            "AND status = 'RUNNING' AND deadline <= CURRENT_TIMESTAMP",
            (tenant, value),
        )
        return conn.execute(
            "SELECT * FROM chat_runs WHERE tenant_id = ? AND " + field + " = ?", (tenant, value)
        ).fetchone()


def accept_message(
    manager: Any,
    session_id: str,
    content: str,
    *,
    tenant: str,
    client_message_id: str,
    input_hash: str,
    automatic_memory: bool,
) -> dict[str, Any]:
    from engram.session.manager import SessionState, Turn

    store = manager.control_plane
    with store.transaction() as conn:
        # Same idempotency key cannot race across different/new sessions.
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(?, 0))",
            (f"chat-id:{tenant}:{client_message_id}",),
        )
        existing = find_run(store, tenant, client_message_id=client_message_id)
        if existing:
            if existing["input_hash"] != input_hash or existing["session_id"] != session_id:
                raise ChatConflictError("client_message_id was already used for different input")
            raise ChatConflictError(
                "this message already has a response run: " + existing["run_id"], run=existing
            )
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(?, 0))",
            (f"session:{tenant}:{session_id}",),
        )
        conn.execute(
            "UPDATE chat_runs SET status = 'FAILED', error_code = 'deadline_exceeded', "
            "completed_at = CURRENT_TIMESTAMP WHERE tenant_id = ? AND session_id = ? "
            "AND status = 'RUNNING' AND deadline <= CURRENT_TIMESTAMP",
            (tenant, session_id),
        )
        active = conn.execute(
            "SELECT * FROM chat_runs WHERE tenant_id = ? AND session_id = ? AND status = 'RUNNING'",
            (tenant, session_id),
        ).fetchone()
        if active:
            raise ChatConflictError("a response is already running for this session", run=active)
        payload = store.get_session_state(session_id, tenant_id=tenant)
        if payload is None:
            raise ChatConflictError("session does not exist")
        session = SessionState.from_payload(payload)
        if session.status not in {"ACTIVE", "WINDOWED"}:
            raise ChatConflictError("session is closed")
        index = session.turns[-1].turn_idx + 1 if session.turns else 0
        now = datetime.now(timezone.utc).isoformat()
        session.turns.append(Turn("user", content, now, index, ingestible=False))
        event_id, _ = store.record_event(
            pair_id="chat-user:" + client_message_id,
            session_id=session_id,
            tenant_id=tenant,
            source="chat_user",
            event_type="INGEST" if automatic_memory else "QUERY",
            initial_status="RECEIVED" if automatic_memory else "COMPLETE",
            payload={
                "schema": "chat-user-v1",
                "source_policy": "user-only-v1",
                "session_id": session_id,
                "client_message_id": client_message_id,
                "turn_pair": {
                    "user": {"content": content, "turn_idx": index},
                    "assistant": {"content": ""},
                },
            },
        )
        manager.schedule_compaction(session)
        store.save_session_state(session_id, session.to_payload(), tenant_id=tenant)
        run = conn.execute(
            "INSERT INTO chat_runs(tenant_id, run_id, session_id, client_message_id, input_event_id, input_hash) "
            "VALUES (?, ?, ?, ?, ?, ?) RETURNING *",
            (
                tenant,
                "run-" + uuid.uuid4().hex,
                session_id,
                client_message_id,
                event_id,
                input_hash,
            ),
        ).fetchone()
    manager.cache.delete(session_id)  # PostgreSQL remains authoritative after acceptance.
    return dict(run)


def finish_response(manager: Any, run_id: str, response: dict[str, Any], *, tenant: str) -> None:
    from engram.session.manager import SessionState, Turn

    store = manager.control_plane
    with store.transaction() as conn:
        reference = conn.execute(
            "SELECT session_id FROM chat_runs WHERE tenant_id = ? AND run_id = ?", (tenant, run_id)
        ).fetchone()
        if reference is None:
            raise ChatConflictError("response run not found")
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(?, 0))",
            (f"session:{tenant}:{reference['session_id']}",),
        )
        run = conn.execute(
            "SELECT * FROM chat_runs WHERE tenant_id = ? AND run_id = ? FOR UPDATE",
            (tenant, run_id),
        ).fetchone()
        if not run or run["status"] != "RUNNING":
            raise ChatConflictError("response run is no longer active")
        if run["deadline"] <= datetime.now(timezone.utc):
            raise ChatConflictError("response deadline exceeded")
        session_id = run["session_id"]
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(?, 0))",
            (f"session:{tenant}:{session_id}",),
        )
        session = SessionState.from_payload(store.get_session_state(session_id, tenant_id=tenant))
        index = session.turns[-1].turn_idx + 1 if session.turns else 0
        # A response accepted before close may finish, but no new user message may enter.
        session.turns.append(
            Turn(
                "assistant",
                response["answer"],
                datetime.now(timezone.utc).isoformat(),
                index,
                ingestible=False,
            )
        )
        manager.schedule_compaction(session)
        store.save_session_state(session_id, session.to_payload(), tenant_id=tenant)
        store.record_event(
            pair_id="chat-answer:" + run_id,
            session_id=session_id,
            tenant_id=tenant,
            source="retrieval_session",
            event_type="QUERY",
            initial_status="COMPLETE",
            payload={
                "session_id": session_id,
                "derived_response": True,
                "run_id": run_id,
                "input_event_id": run["input_event_id"],
                "turn_pair": {
                    "user": {"content": ""},
                    "assistant": {"content": response["answer"], "turn_idx": index},
                },
            },
        )
        conn.execute(
            "UPDATE chat_runs SET status = 'COMPLETED', response = ?, completed_at = CURRENT_TIMESTAMP "
            "WHERE tenant_id = ? AND run_id = ?",
            (json.dumps(response), tenant, run_id),
        )
    manager.cache.delete(session_id)


def terminate_run(
    store: Any,
    run_id: str,
    *,
    tenant: str,
    cancelled: bool = False,
    error_code: str = "generation_failed",
) -> None:
    with store.transaction() as conn:
        conn.execute(
            "UPDATE chat_runs SET status = ?, error_code = ?, completed_at = CURRENT_TIMESTAMP "
            "WHERE tenant_id = ? AND run_id = ? AND status = 'RUNNING'",
            ("CANCELLED" if cancelled else "FAILED", error_code, tenant, run_id),
        )
