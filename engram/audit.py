"""Append-only audit log.

Captures every privileged action: tenant creation, key rotation, memory
retire/unmerge, consolidation triggers. The trail is tenant-scoped and
immutable once written; operators can prove what happened, when, by whom.

Rows live in the control-plane SQLite (or Postgres when configured).
Retention is driven by `audit.retention_days` in config.yaml (default 365).
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS audit_log (
    audit_id     TEXT PRIMARY KEY,
    ts           TEXT NOT NULL DEFAULT (datetime('now')),
    tenant_id    TEXT NOT NULL,
    actor        TEXT NOT NULL,                 -- api-key-hash | admin-key | system | cli
    action       TEXT NOT NULL,                 -- e.g. tenant.create, memory.retire
    target       TEXT,                          -- e.g. tenant_id or mem:// URI
    request_id   TEXT,
    details      TEXT,                          -- JSON blob, may be empty
    remote_addr  TEXT
);
CREATE INDEX IF NOT EXISTS idx_audit_tenant_ts ON audit_log(tenant_id, ts);
CREATE INDEX IF NOT EXISTS idx_audit_action_ts ON audit_log(action, ts);
"""


class AuditLog:
    """Thread-safe append-only audit writer.

    Co-located with the SQLite control-plane DB for simplicity. When the
    Postgres backend is in use, operators should shadow-ship the audit
    table to a dedicated append-only store (S3, BigQuery) via a CDC feed
    for compliance retention.
    """

    def __init__(self, db_path: str | Path) -> None:
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._tls = threading.local()
        self._init()

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._tls, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                str(self._path), isolation_level=None, check_same_thread=False,
                timeout=30.0,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._tls.conn = conn
        return conn

    def _init(self) -> None:
        self._conn().executescript(_SCHEMA)

    def record(
        self,
        *,
        tenant_id: str,
        actor: str,
        action: str,
        target: str | None = None,
        details: dict[str, Any] | None = None,
        request_id: str | None = None,
        remote_addr: str | None = None,
    ) -> str:
        audit_id = f"aud-{uuid.uuid4().hex[:16]}"
        try:
            self._conn().execute(
                "INSERT INTO audit_log "
                "(audit_id, tenant_id, actor, action, target, request_id, details, remote_addr) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    audit_id, tenant_id, actor, action, target, request_id,
                    json.dumps(details or {}, ensure_ascii=False),
                    remote_addr,
                ),
            )
        except Exception:
            # Audit should never break the calling request. Log + continue.
            log.exception("audit write failed: tenant=%s action=%s", tenant_id, action)
        return audit_id

    def tail(self, tenant_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = self._conn().execute(
            "SELECT * FROM audit_log WHERE tenant_id = ? ORDER BY ts DESC LIMIT ?",
            (tenant_id, limit),
        ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            with suppress(Exception):
                d["details"] = json.loads(d.get("details") or "{}")
            out.append(d)
        return out

    def prune(self, *, retention_days: int) -> int:
        """Delete rows older than `retention_days`. Returns rows affected."""
        cursor = self._conn().execute(
            "DELETE FROM audit_log "
            "WHERE julianday('now') - julianday(ts) > ?",
            (retention_days,),
        )
        return cursor.rowcount or 0


# Process-wide singleton
_audit: AuditLog | None = None
_audit_lock = threading.Lock()


def get_audit_log(db_path: str | Path) -> AuditLog:
    global _audit
    with _audit_lock:
        if _audit is None:
            _audit = AuditLog(db_path)
        return _audit
