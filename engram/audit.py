"""Append-only audit log.

Captures every privileged action: tenant creation, key rotation, memory
retire/unmerge, consolidation triggers. The trail is tenant-scoped and
immutable once written; operators can prove what happened, when, by whom.

Rows live in the PostgreSQL control plane.
Retention is driven by `audit.retention_days` in config.yaml (default 365).
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from contextlib import suppress
from typing import Any

log = logging.getLogger(__name__)


class AuditLog:
    """Append-only PostgreSQL audit writer."""

    def __init__(self, store: Any) -> None:
        if not hasattr(store, "get_conn"):
            raise TypeError("AuditLog requires a PostgreSQL control-plane store")
        self._store = store

    def _conn(self) -> Any:
        return self._store.get_conn()

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
                    audit_id,
                    tenant_id,
                    actor,
                    action,
                    target,
                    request_id,
                    json.dumps(details or {}, ensure_ascii=False),
                    remote_addr,
                ),
            )
        except Exception:
            # Audit should never break the calling request. Log + continue.
            log.exception("audit write failed: tenant=%s action=%s", tenant_id, action)
        return audit_id

    def tail(self, tenant_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = (
            self._conn()
            .execute(
                "SELECT * FROM audit_log WHERE tenant_id = ? ORDER BY ts DESC LIMIT ?",
                (tenant_id, limit),
            )
            .fetchall()
        )
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
            "DELETE FROM audit_log WHERE ts < CURRENT_TIMESTAMP - (? * INTERVAL '1 day')",
            (retention_days,),
        )
        return cursor.rowcount or 0


# Process-wide singleton
_audit: AuditLog | None = None
_audit_lock = threading.Lock()


def get_audit_log(store: Any) -> AuditLog:
    global _audit
    with _audit_lock:
        if _audit is None:
            _audit = AuditLog(store)
        return _audit
