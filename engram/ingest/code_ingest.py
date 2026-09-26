"""Durable PostgreSQL-canonical code archive ingestion."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from engram.codegraph import CodeSourceFile, analyze_project, attach_embeddings
from engram.storage.memory_repository import (
    MemoryRepository,
    TypedClaim,
    _BoundTransactionStore,
)
from engram.tenancy import Tenant, TenantQuotas, set_current_tenant


def enqueue_code_archive(
    state: Any,
    *,
    job_id: str,
    tenant_id: str,
    project_name: str,
    filename: str,
    content: bytes,
) -> str:
    """Persist the upload and enqueue CODE_INGEST in one transaction."""

    event_id = f"evt-code-{hashlib.sha256(f'{tenant_id}:{job_id}'.encode()).hexdigest()[:16]}"
    pair_id = hashlib.sha256(f"code:{tenant_id}:{job_id}".encode()).hexdigest()
    artifact_key = filename or "project.zip"
    payload = {
        "job_id": job_id,
        "project_name": project_name,
        "artifact_key": artifact_key,
    }
    expires_at = datetime.now(timezone.utc) + timedelta(
        hours=state.cfg.canonical_memory.artifact_retention_hours
    )
    with state.control_plane.transaction() as conn:
        conn.execute(
            "INSERT INTO events "
            "(event_id, pair_id, tenant_id, session_id, source, event_type, payload, status) "
            "VALUES (?, ?, ?, NULL, 'bulk_zip', 'CODE_INGEST', ?, 'RECEIVED') "
            "ON CONFLICT (tenant_id, pair_id) DO NOTHING",
            (event_id, pair_id, tenant_id, json.dumps(payload)),
        )
        bound = MemoryRepository(
            _BoundTransactionStore(
                conn,
                projection_task_queue=state.memory_repository.projection_task_queue,
            ),
            default_tenant_id=tenant_id,
        )
        bound.record_ingest_artifact(
            event_id=event_id,
            artifact_type="UPLOAD_ZIP",
            artifact_key=artifact_key,
            content=content,
            media_type="application/zip",
            expires_at=expires_at,
            tenant_id=tenant_id,
            metadata={"job_id": job_id, "project_name": project_name},
        )
        state.control_plane._enqueue_dispatch_in_tx(
            conn,
            "CODE_INGEST",
            job_id,
            tenant_id,
            payload={"event_id": event_id, "artifact_key": artifact_key},
        )
    return event_id


def process_code_ingest_job(state: Any, job_id: str) -> str:
    """Build canonical code nodes/claims from a durable ZIP artifact."""

    dispatch = state.control_plane.get_dispatch_for_aggregate("CODE_INGEST", job_id)
    if dispatch is None:
        raise ValueError(f"code ingest dispatch not found: {job_id}")
    tenant_id = str(dispatch["tenant_id"])
    payload = _mapping(dispatch.get("payload"))
    event_id = str(payload.get("event_id") or "")
    artifact_key = str(payload.get("artifact_key") or "project.zip")
    if not event_id:
        raise ValueError("code ingest dispatch is missing event_id")
    event = state.control_plane.get_event(event_id, tenant_id=tenant_id)
    if event is None:
        raise ValueError(f"code ingest event not found: {event_id}")
    event_payload = _mapping(event.get("payload"))
    project_name = str(event_payload.get("project_name") or "").strip()
    if not project_name:
        raise ValueError("code ingest event is missing project_name")
    artifact = state.memory_repository.get_ingest_artifact(
        event_id,
        "UPLOAD_ZIP",
        artifact_key,
        tenant_id=tenant_id,
    )
    if artifact is None:
        raise ValueError(f"code ingest artifact not found: {event_id}:{artifact_key}")

    set_current_tenant(
        Tenant(
            tenant_id=tenant_id,
            display_name=tenant_id,
            api_key_hashes=[],
            quotas=TenantQuotas(),
            status="ACTIVE",
        )
    )
    try:
        sources = _code_sources(artifact.content)
        graph = analyze_project(project_name, sources, core=state.core)
        if not graph.ok:
            _save_results(state, job_id, tenant_id, graph, status="FAILED")
            state.control_plane.set_event_status(
                event_id,
                "FAILED",
                error_message="one or more code files failed deterministic analysis",
                tenant_id=tenant_id,
            )
            raise ValueError("one or more code files failed deterministic analysis")
        attach_embeddings(graph, state.embed)
        source_by_path = {item.path: item.content for item in sources}
        descriptors: list[dict[str, Any]] = []
        graph_uri_to_index: dict[str, int] = {graph.project_uri: 0}
        for node in graph.nodes:
            if node.node_type in {"PROJECT", "DIRECTORY"}:
                continue
            relative_path = str(node.properties.get("relative_path") or node.source_uri)
            identity_path = _identity_path(node.source_uri, graph.project_uri, relative_path)
            graph_uri_to_index[node.source_uri] = len(descriptors) + 1
            descriptors.append(
                {
                    "path": identity_path,
                    "memory_type": node.node_type,
                    "canonical_name": node.display_name,
                    "body": source_by_path.get(relative_path),
                    "metadata": dict(node.properties),
                }
            )
        # Project, symbols, hierarchy, relationships, evidence, projection
        # dispatches, and the event terminal state commit together.  The
        # bound repository keeps every nested repository operation on this
        # single PostgreSQL transaction.
        with state.memory_repository.transaction(tenant_id=tenant_id) as conn:
            canonical = MemoryRepository(
                _BoundTransactionStore(
                    conn,
                    projection_task_queue=state.memory_repository.projection_task_queue,
                ),
                default_tenant_id=tenant_id,
            )
            committed = canonical.commit_code_project(
                event_id=event_id,
                project_name=project_name,
                archive_bytes=artifact.content,
                filename=artifact_key,
                files=descriptors,
                tenant_id=tenant_id,
                mutation_key=f"{event_id}:code-project",
            )
            by_graph_uri = {
                graph_uri: committed.nodes[index]
                for graph_uri, index in graph_uri_to_index.items()
                if index < len(committed.nodes)
            }
            for edge_index, edge in enumerate(graph.edges):
                subject = by_graph_uri.get(edge.subject_uri)
                object_node = by_graph_uri.get(edge.object_uri)
                if subject is None or object_node is None:
                    continue
                claim = canonical.add_claim(
                    TypedClaim(
                        subject_id=subject.id,
                        predicate=edge.edge_type,
                        object_type="ENTITY",
                        object_entity_id=object_node.id,
                        confidence=float(edge.properties.get("confidence", 1.0)),
                        source_event_id=event_id,
                        source_triplet_index=edge_index,
                        metadata={
                            "relation_label": edge.relation_label,
                            **dict(edge.properties),
                        },
                    ),
                    tenant_id=tenant_id,
                    mutation_key=f"{event_id}:code-edge:{edge_index}",
                )
                canonical.record_evidence(
                    memory_id=subject.id,
                    claim_id=claim.id,
                    source_event_id=event_id,
                    extractor="deterministic-codegraph",
                    extractor_version="v2",
                    confidence=claim.confidence,
                    metadata={"artifact_id": str(artifact.id)},
                    idempotency_key=f"{event_id}:edge:{edge_index}",
                    tenant_id=tenant_id,
                )
            conn.execute(
                "UPDATE events SET status = 'COMPLETE', processed_at = CURRENT_TIMESTAMP, "
                "error_message = NULL WHERE tenant_id = ? AND event_id = ?",
                (tenant_id, event_id),
            )
        _save_results(state, job_id, tenant_id, graph, status="INDEXED")
        return "COMPLETE"
    finally:
        set_current_tenant(None)


def _code_sources(raw: bytes) -> list[CodeSourceFile]:
    from engram.api.routes.bulk_ingest import _parse_zip

    _, rejected, sources = _parse_zip(
        raw,
        job_id="code-ingest",
        default_session_id=None,
        source="bulk_zip",
    )
    code_rejections = [
        row for row in rejected if row.preview and not row.reason.startswith("unsupported")
    ]
    if code_rejections:
        raise ValueError(code_rejections[0].reason)
    if not sources:
        raise ValueError("ZIP archive contains no supported source files")
    return sources


def _identity_path(source_uri: str, project_uri: str, relative_path: str) -> str:
    suffix = source_uri.removeprefix(project_uri).strip("/")
    return suffix or relative_path


def _save_results(state: Any, job_id: str, tenant_id: str, graph: Any, *, status: str) -> None:
    items = [
        {
            "row_number": index,
            "path": result.path,
            "status": status if result.status != "FAILED" else "FAILED",
            "node_count": result.node_count,
            "relationship_count": result.relationship_count,
            "error_message": result.error_message,
        }
        for index, result in enumerate(graph.file_results, start=1)
    ]
    state.control_plane.save_bulk_code_items(
        job_id=job_id,
        tenant_id=tenant_id,
        project_uri=graph.project_uri,
        items=items,
    )


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    return {}


__all__ = ["enqueue_code_archive", "process_code_ingest_job"]
