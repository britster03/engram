"""Explicit source reprocessing with atomic promotion of canonical results."""

from __future__ import annotations

import json
import uuid
from typing import Any

from engram.ingest.policy import processing_plan, processing_policy
from engram.storage.idempotency import IdempotencyConflictError, request_pair_id


def request_reprocessing(
    state: Any,
    event_id: str,
    *,
    tenant_id: str,
    request_key: str,
    reason: str,
) -> dict[str, Any]:
    store, repository = state.control_plane, state.memory_repository
    requested = store.get_event(event_id, tenant_id=tenant_id)
    if requested is None:
        raise LookupError("event not found")
    if requested["event_type"] != "INGEST":
        raise ValueError("only conversational ingestion supports extraction reprocessing")
    root_id = (requested["payload"].get("_reprocess") or {}).get("root_event_id") or event_id
    policy = processing_policy(state.cfg)["fingerprint"]
    with repository.transaction(tenant_id=tenant_id) as conn:
        root = conn.execute(
            "SELECT * FROM events WHERE tenant_id = ? AND event_id = ? FOR UPDATE",
            (tenant_id, root_id),
        ).fetchone()
        if root is None:
            raise LookupError("original source not found")
        previous_request = conn.execute(
            "SELECT * FROM ingest_reprocessings WHERE tenant_id = ? AND root_event_id = ? AND request_key = ?",
            (tenant_id, root_id, request_key),
        ).fetchone()
        if previous_request:
            if previous_request["policy"] != policy or previous_request["reason"] != reason:
                raise IdempotencyConflictError(
                    "reprocessing key was already used for different input or policy"
                )
            return store.get_event(previous_request["event_id"], tenant_id=tenant_id)
        if root["status"] not in {"COMPLETE", "GATED_SKIP", "FAILED"}:
            raise ValueError("original ingestion must finish before reprocessing")
        conn.execute(
            "UPDATE ingest_reprocessings r SET state = 'FAILED', updated_at = CURRENT_TIMESTAMP "
            "FROM events e WHERE r.tenant_id = ? AND r.root_event_id = ? AND r.state = 'PENDING' "
            "AND e.tenant_id = r.tenant_id AND e.event_id = r.event_id AND e.status = 'FAILED'",
            (tenant_id, root_id),
        )
        pending = conn.execute(
            "SELECT 1 FROM ingest_reprocessings WHERE tenant_id = ? AND root_event_id = ? AND state = 'PENDING'",
            (tenant_id, root_id),
        ).fetchone()
        if pending:
            raise ValueError("another reprocessing request is still running")
        running = conn.execute(
            "SELECT 1 FROM workflow_dispatches d WHERE d.tenant_id = ? AND d.workflow_type = 'INGEST' "
            "AND d.status IN ('PENDING', 'DISPATCHING', 'STARTED') AND (d.aggregate_id = ? OR EXISTS "
            "(SELECT 1 FROM ingest_reprocessings r WHERE r.tenant_id = d.tenant_id AND r.event_id = d.aggregate_id AND r.root_event_id = ?))",
            (tenant_id, root_id, root_id),
        ).fetchone()
        if running:
            raise ValueError(
                "previous ingestion execution must finish terminal bookkeeping before reprocessing"
            )
        active = conn.execute(
            "SELECT event_id FROM ingest_reprocessings WHERE tenant_id = ? AND root_event_id = ? AND state = 'ACTIVE'",
            (tenant_id, root_id),
        ).fetchone()
        if active is None:
            conn.execute(
                "INSERT INTO ingest_reprocessings (tenant_id, root_event_id, event_id, policy, reason, state) "
                "VALUES (?, ?, ?, 'original', 'Original source generation', 'ACTIVE')",
                (tenant_id, root_id, root_id),
            )
        previous_id = active["event_id"] if active else root_id
        payload = json.loads(root["payload"])
        payload["_reprocess"] = {
            "root_event_id": root_id,
            "previous_event_id": previous_id,
            "source_asserted_at": root["created_at"].isoformat(),
        }
        child_id, _ = store.record_event(
            pair_id=request_pair_id(f"{root_id}:{request_key}", namespace="reprocess"),
            tenant_id=tenant_id,
            session_id=root["session_id"],
            source=root["source"],
            event_type="INGEST",
            payload=payload,
            require_matching_payload=True,
        )
        conn.execute(
            "INSERT INTO ingest_reprocessings (tenant_id, root_event_id, event_id, previous_event_id, request_key, policy, reason, state) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING')",
            (tenant_id, root_id, child_id, previous_id, request_key, policy, reason),
        )
        child = store.get_event(child_id, tenant_id=tenant_id)
        processing_plan(repository, child, tenant_id, state.cfg)
        conn.execute(
            "INSERT INTO audit_log (audit_id, tenant_id, actor, action, target, details) VALUES (?, ?, 'api', 'INGEST_REPROCESS_REQUESTED', ?, ?)",
            (
                "aud-" + uuid.uuid4().hex,
                tenant_id,
                root_id,
                json.dumps(
                    {
                        "event_id": child_id,
                        "previous_event_id": previous_id,
                        "policy": policy,
                        "reason": reason,
                    }
                ),
            ),
        )
        return child


def promote_reprocessing(
    repository: Any, conn: Any, *, tenant: str, event_id: str, payload: dict
) -> list[str]:
    """Retire only superseded-source contributions inside the canonical commit.

    Evidence and versions remain immutable. Independently supported claims remain
    active. All retirement projections share the commit's transaction.
    """
    lineage = payload.get("_reprocess")
    if not lineage:
        return []
    generation = conn.execute(
        "SELECT * FROM ingest_reprocessings WHERE tenant_id = ? AND event_id = ? FOR UPDATE",
        (tenant, event_id),
    ).fetchone()
    if generation is None or generation["state"] != "PENDING":
        raise ValueError("reprocessing generation is no longer pending")
    previous = conn.execute(
        "UPDATE ingest_reprocessings SET state = 'SUPERSEDED', updated_at = CURRENT_TIMESTAMP "
        "WHERE tenant_id = ? AND event_id = ? AND state = 'ACTIVE' RETURNING event_id",
        (tenant, generation["previous_event_id"]),
    ).fetchone()
    if previous is None:
        raise ValueError("reprocessing no longer targets the active source generation")
    conn.execute(
        "UPDATE ingest_reprocessings SET state = 'ACTIVE', updated_at = CURRENT_TIMESTAMP WHERE tenant_id = ? AND event_id = ?",
        (tenant, event_id),
    )
    previous_id = previous["event_id"]
    retired = conn.execute(
        "UPDATE memory_claims c SET status = 'RETRACTED', updated_at = CURRENT_TIMESTAMP "
        "WHERE c.tenant_id = ? AND c.status NOT IN ('RETRACTED', 'SUPERSEDED', 'HISTORICAL') "
        "AND (c.source_event_id = ? OR EXISTS (SELECT 1 FROM memory_evidence e WHERE e.tenant_id = c.tenant_id AND e.claim_id = c.id AND e.source_event_id = ?)) "
        "AND (c.source_event_id IS NULL OR c.source_event_id = ? OR EXISTS (SELECT 1 FROM ingest_reprocessings g WHERE g.tenant_id = c.tenant_id AND g.event_id = c.source_event_id AND g.state = 'SUPERSEDED')) "
        "AND NOT EXISTS (SELECT 1 FROM memory_evidence e WHERE e.tenant_id = c.tenant_id AND e.claim_id = c.id "
        "AND e.source_event_id <> ? AND NOT EXISTS (SELECT 1 FROM ingest_reprocessings g WHERE g.tenant_id = e.tenant_id AND g.event_id = e.source_event_id AND g.state = 'SUPERSEDED')) "
        "RETURNING c.*",
        (tenant, previous_id, previous_id, previous_id, previous_id),
    ).fetchall()
    dispatches = []
    affected = set()
    for claim in retired:
        subject_id = uuid.UUID(str(claim["subject_id"]))
        affected.add(subject_id)
        revision = repository._bump_node_revision(conn, tenant, subject_id)
        dispatch = repository._enqueue_dispatch_in_tx(
            conn,
            tenant_id=tenant,
            aggregate_type="CLAIM",
            aggregate_id=str(claim["id"]),
            operation="UPSERT_CLAIM",
            revision=revision,
            payload={
                "memory_id": str(subject_id),
                "claim_id": str(claim["id"]),
                "reprocessing_event_id": event_id,
            },
        )
        dispatches.append(dispatch.dispatch_id)
    historical = conn.execute(
        "UPDATE memory_nodes n SET status = 'HISTORICAL', revision = revision + 1, updated_at = CURRENT_TIMESTAMP "
        "WHERE n.tenant_id = ? AND n.status = 'ACTIVE' AND ((n.memory_type = 'EPISODE' AND n.origin_event_id = ?) "
        "OR (n.memory_type = 'ENTITY' AND n.metadata->>'source_root' = ? AND n.metadata->>'identity_key' IS NULL "
        "AND NOT EXISTS (SELECT 1 FROM memory_claims c WHERE c.tenant_id = n.tenant_id AND (c.subject_id = n.id OR c.object_entity_id = n.id) AND c.status IN ('ACTIVE', 'CONFLICTING')))) RETURNING n.id",
        (tenant, previous_id, generation["root_event_id"]),
    ).fetchall()
    affected.update(uuid.UUID(str(n["id"])) for n in historical)
    for identity in sorted(affected, key=str):
        node = repository._require_node(conn, tenant, identity)
        dispatch = repository._enqueue_dispatch_in_tx(
            conn,
            tenant_id=tenant,
            aggregate_type="MEMORY",
            aggregate_id=str(identity),
            operation="UPSERT_NODE",
            revision=node.revision,
            payload={"memory_id": str(identity), "reprocessing_event_id": event_id},
        )
        dispatches.append(dispatch.dispatch_id)
    return dispatches
