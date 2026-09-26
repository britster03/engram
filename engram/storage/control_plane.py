"""Backend-neutral control-plane query helpers.

The application has one durable backend: PostgreSQL.  These helpers operate
through the PostgreSQL connection adapter and keep repetitive CRUD operations
out of :mod:`engram.storage.postgres`.
"""

from __future__ import annotations

import json
from typing import Any

from engram.tenancy import DEFAULT_TENANT_ID


class ControlPlaneQueries:
    """Shared PostgreSQL CRUD methods supplied by ``PostgresStore``."""

    def get_conn(self) -> Any:
        raise NotImplementedError

    def transaction(self) -> Any:
        raise NotImplementedError

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
                "UPDATE events SET status = ?, error_message = ?, "
                "processed_at = CURRENT_TIMESTAMP "
                f"WHERE event_id = ?{tenant_clause}",
                params,
            )

    def get_event(
        self,
        event_id: str,
        *,
        tenant_id: str | None = None,
    ) -> dict[str, Any] | None:
        if tenant_id is None:
            row = (
                self.get_conn()
                .execute("SELECT * FROM events WHERE event_id = ?", (event_id,))
                .fetchone()
            )
        else:
            row = (
                self.get_conn()
                .execute(
                    "SELECT * FROM events WHERE event_id = ? AND tenant_id = ?",
                    (event_id, tenant_id),
                )
                .fetchone()
            )
        if row is None:
            return None
        event = dict(row)
        event["payload"] = json.loads(event["payload"])
        return event

    def list_session_events(
        self,
        session_id: str,
        *,
        tenant_id: str = DEFAULT_TENANT_ID,
    ) -> list[dict[str, Any]]:
        rows = (
            self.get_conn()
            .execute(
                "SELECT event_id, pair_id, payload, created_at FROM events "
                "WHERE tenant_id = ? AND session_id = ? "
                "AND event_type IN ('INGEST', 'QUERY') "
                "ORDER BY created_at ASC, event_id ASC",
                (tenant_id, session_id),
            )
            .fetchall()
        )
        events: list[dict[str, Any]] = []
        for row in rows:
            event = dict(row)
            event["payload"] = json.loads(event["payload"])
            events.append(event)
        return events

    def list_session_rollups(
        self,
        *,
        tenant_id: str = DEFAULT_TENANT_ID,
    ) -> list[dict[str, Any]]:
        rows = (
            self.get_conn()
            .execute(
                "SELECT session_id, COUNT(*) AS pair_count, MIN(created_at) AS created_at, "
                "MAX(created_at) AS last_active FROM events "
                "WHERE tenant_id = ? AND session_id IS NOT NULL AND session_id != '' "
                "AND event_type IN ('INGEST', 'QUERY') GROUP BY session_id "
                "ORDER BY last_active DESC, session_id ASC",
                (tenant_id,),
            )
            .fetchall()
        )
        return [dict(row) for row in rows]

    def claim_pending_events(
        self,
        limit: int = 10,
        *,
        tenant_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Claim pending rows for the non-Temporal compatibility worker."""
        if limit <= 0:
            return []
        with self.transaction() as conn:
            tenant_filter = "" if tenant_id is None else " AND tenant_id = ?"
            params: tuple[Any, ...] = (limit,) if tenant_id is None else (tenant_id, limit)
            rows = conn.execute(
                "WITH picked AS ("
                "SELECT event_id FROM events WHERE status = 'RECEIVED' "
                "AND event_type = 'INGEST'"
                f"{tenant_filter} ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT ?"
                ") UPDATE events e SET status = 'PROCESSING', "
                "processed_at = CURRENT_TIMESTAMP, error_message = NULL "
                "FROM picked WHERE e.event_id = picked.event_id RETURNING e.*",
                params,
            ).fetchall()
        return [dict(row, payload=json.loads(row["payload"])) for row in rows]

    def get_extraction(self, event_id: str) -> dict[str, Any] | None:
        row = (
            self.get_conn()
            .execute("SELECT * FROM extractions WHERE event_id = ?", (event_id,))
            .fetchone()
        )
        if row is None:
            return None
        extraction = dict(row)
        extraction["triplets"] = json.loads(extraction["triplets"])
        return extraction

    def fs_outbox_mark(
        self,
        event_id: str,
        state: str,
        error: str | None = None,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE fs_outbox SET state = ?, last_attempt = CURRENT_TIMESTAMP, "
                "retry_count = retry_count + 1, error_message = ? WHERE event_id = ?",
                (state, error, event_id),
            )

    def get_fs_outbox(
        self,
        event_id: str,
        *,
        tenant_id: str | None = None,
    ) -> dict[str, Any] | None:
        if tenant_id is None:
            row = (
                self.get_conn()
                .execute("SELECT * FROM fs_outbox WHERE event_id = ?", (event_id,))
                .fetchone()
            )
        else:
            row = (
                self.get_conn()
                .execute(
                    "SELECT * FROM fs_outbox WHERE event_id = ? AND tenant_id = ?",
                    (event_id, tenant_id),
                )
                .fetchone()
            )
        return dict(row) if row is not None else None

    def queue_depth(self, *, tenant_id: str | None = None) -> int:
        if tenant_id is None:
            row = (
                self.get_conn()
                .execute(
                    "SELECT COUNT(*) AS c FROM consolidation_tasks "
                    "WHERE status IN ('PENDING', 'PROCESSING')"
                )
                .fetchone()
            )
        else:
            row = (
                self.get_conn()
                .execute(
                    "SELECT COUNT(*) AS c FROM consolidation_tasks "
                    "WHERE status IN ('PENDING', 'PROCESSING') AND tenant_id = ?",
                    (tenant_id,),
                )
                .fetchone()
            )
        return int(row["c"])

    def count_events_by_status(self, status: str) -> int:
        row = (
            self.get_conn()
            .execute("SELECT COUNT(*) AS c FROM events WHERE status = ?", (status,))
            .fetchone()
        )
        return int(row["c"]) if row else 0

    def count_outbox_pending(self) -> int:
        row = (
            self.get_conn()
            .execute("SELECT COUNT(*) AS c FROM fs_outbox WHERE state = 'PENDING'")
            .fetchone()
        )
        return int(row["c"]) if row else 0

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
                "completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP) "
                f"WHERE job_id = ?{tenant_clause}",
                params,
            )

    def mark_bulk_job_processing(self, job_id: str, *, tenant_id: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE bulk_jobs SET status = 'PROCESSING' "
                "WHERE job_id = ? AND tenant_id = ? AND status = 'QUEUED'",
                (job_id, tenant_id),
            )

    def requeue_bulk_job(self, job_id: str, *, tenant_id: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE bulk_jobs SET status = 'QUEUED', completed_at = NULL "
                "WHERE job_id = ? AND tenant_id = ?",
                (job_id, tenant_id),
            )

    def get_bulk_job(
        self,
        job_id: str,
        *,
        tenant_id: str | None = None,
    ) -> dict[str, Any] | None:
        if tenant_id is None:
            row = (
                self.get_conn()
                .execute("SELECT * FROM bulk_jobs WHERE job_id = ?", (job_id,))
                .fetchone()
            )
        else:
            row = (
                self.get_conn()
                .execute(
                    "SELECT * FROM bulk_jobs WHERE job_id = ? AND tenant_id = ?",
                    (job_id, tenant_id),
                )
                .fetchone()
            )
        if row is None:
            return None
        job = dict(row)
        job["dry_run"] = bool(job.get("dry_run"))
        job["rejected_rows"] = json.loads(job.get("rejected_rows") or "[]")
        job["event_ids"] = json.loads(job.get("event_ids") or "[]")
        return job

    def save_bulk_code_items(
        self,
        *,
        job_id: str,
        tenant_id: str,
        project_uri: str | None,
        items: list[dict[str, Any]],
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                "DELETE FROM bulk_code_items WHERE job_id = ? AND tenant_id = ?",
                (job_id, tenant_id),
            )
            conn.executemany(
                "INSERT INTO bulk_code_items "
                "(job_id, tenant_id, row_number, path, status, node_count, "
                "relationship_count, error_message, project_uri) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        job_id,
                        tenant_id,
                        int(item["row_number"]),
                        str(item["path"]),
                        str(item["status"]),
                        int(item.get("node_count") or 0),
                        int(item.get("relationship_count") or 0),
                        item.get("error_message"),
                        project_uri,
                    )
                    for item in items
                ],
            )

    def get_bulk_code_items(
        self,
        job_id: str,
        *,
        tenant_id: str | None = None,
    ) -> list[dict[str, Any]]:
        if tenant_id is None:
            rows = (
                self.get_conn()
                .execute(
                    "SELECT * FROM bulk_code_items WHERE job_id = ? ORDER BY row_number",
                    (job_id,),
                )
                .fetchall()
            )
        else:
            rows = (
                self.get_conn()
                .execute(
                    "SELECT * FROM bulk_code_items WHERE job_id = ? AND tenant_id = ? "
                    "ORDER BY row_number",
                    (job_id, tenant_id),
                )
                .fetchall()
            )
        return [dict(row) for row in rows]
