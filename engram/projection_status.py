"""Canonical projection progress derived from PostgreSQL, never Neo4j."""

from __future__ import annotations

from typing import Any, Literal

ProjectionStatus = Literal["PENDING", "PROJECTING", "INDEXED", "FAILED"]


def projection_status_for_event(
    state: Any,
    event_id: str,
    *,
    tenant_id: str,
) -> ProjectionStatus | None:
    """Return the aggregate state of projection dispatches for an ingest event."""

    cfg = getattr(state, "cfg", None)
    canonical = bool(cfg is not None and cfg.canonical_memory.enabled)
    if not canonical:
        outbox = state.control_plane.get_fs_outbox(event_id, tenant_id=tenant_id)
        if outbox is None:
            return None
        legacy = str(outbox.get("state") or "").upper()
        if legacy == "INDEXED":
            return "INDEXED"
        if legacy in {"INDEX_FAILED", "FAILED"}:
            return "FAILED"
        return "PROJECTING" if legacy == "WRITTEN" else "PENDING"

    with state.memory_repository.transaction(tenant_id=tenant_id) as conn:
        rows = conn.execute(
            "WITH event_dispatches AS ("
            "SELECT d.dispatch_id, d.status FROM canonical_mutations m "
            "JOIN workflow_dispatches d ON d.tenant_id = m.tenant_id "
            "AND d.workflow_type = 'PROJECTION' "
            "AND d.aggregate_id = CAST(m.id AS TEXT) "
            "WHERE m.tenant_id = ? AND m.source_event_id = ? "
            "UNION "
            "SELECT d.dispatch_id, d.status FROM canonical_mutations m "
            "CROSS JOIN LATERAL jsonb_array_elements_text("
            "COALESCE(m.result->'dispatch_ids', '[]'::jsonb)) AS ids(dispatch_id) "
            "JOIN workflow_dispatches d ON d.tenant_id = m.tenant_id "
            "AND d.dispatch_id = ids.dispatch_id "
            "WHERE m.tenant_id = ? AND m.source_event_id = ? "
            "UNION "
            "SELECT d.dispatch_id, d.status FROM workflow_dispatches d "
            "WHERE d.tenant_id = ? AND d.workflow_type = 'PROJECTION' AND ("
            "d.payload->>'memory_id' IN (SELECT CAST(id AS TEXT) FROM memory_nodes "
            "WHERE tenant_id = ? AND origin_event_id = ?) OR "
            "d.payload->>'claim_id' IN (SELECT CAST(id AS TEXT) FROM memory_claims "
            "WHERE tenant_id = ? AND source_event_id = ?))) "
            "SELECT status FROM event_dispatches",
            (
                tenant_id,
                event_id,
                tenant_id,
                event_id,
                tenant_id,
                tenant_id,
                event_id,
                tenant_id,
                event_id,
            ),
        ).fetchall()
    statuses = {str(row["status"]).upper() for row in rows}
    if not statuses:
        return "PENDING"
    if statuses & {"DEAD", "FAILED"}:
        return "FAILED"
    if statuses <= {"COMPLETE"}:
        return "INDEXED"
    if statuses & {"DISPATCHING", "STARTED"}:
        return "PROJECTING"
    return "PENDING"


__all__ = ["ProjectionStatus", "projection_status_for_event"]
