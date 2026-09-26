"""Memory-node endpoints (§11.1)."""

from __future__ import annotations

import logging
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from typing import Any, cast
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from engram import frontmatter
from engram.api.auth import AuthDep
from engram.deps import get_state
from engram.frontmatter import FrontmatterError
from engram.tenancy import current_tenant_id
from engram.uri import is_legacy_file_uri

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/memories", tags=["memories"], dependencies=[AuthDep])


class MemoryResponse(BaseModel):
    source_uri: str
    frontmatter: dict[str, Any] = Field(default_factory=dict)
    body: str = ""
    edges: list[dict[str, Any]] | None = None
    memory_id: str | None = None
    canonical_uri: str | None = None
    revision: int | None = None
    memory_type: str | None = None
    status: str | None = None
    current_version: dict[str, Any] | None = None
    claims: list[dict[str, Any]] = Field(default_factory=list)
    evidence: list[dict[str, Any]] = Field(default_factory=list)


class MemoryListItem(BaseModel):
    source_uri: str
    node_type: str | None = None
    l0_abstract: str | None = None
    status: str | None = None


class MemoryListResponse(BaseModel):
    items: list[MemoryListItem]
    next_cursor: str | None = None


@router.get("", response_model=MemoryListResponse)
def list_memories(
    prefix: str = Query(default="mem://", description="URI prefix to list under"),
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = Query(default=None),
) -> MemoryListResponse:
    state = get_state()
    tenant_id = current_tenant_id()
    if state.cfg.canonical_memory.enabled:
        repository = _require_canonical_repository(state)
        page = repository.list_memories(
            tenant_id=tenant_id,
            prefix=prefix,
            limit=limit,
            cursor=cursor,
        )
        return MemoryListResponse(
            items=[
                MemoryListItem(
                    source_uri=node.canonical_uri,
                    node_type=node.memory_type,
                    status=node.status,
                )
                for node in page.nodes
            ],
            next_cursor=page.next_cursor,
        )
    # Prefer Neo4j for listing if possible; fall back to filesystem walk for empty KG.
    try:
        rows = state.neo4j.run_template(
            "MATCH (n:Node) WHERE n.source_uri STARTS WITH $prefix "
            "AND n.tenant_id = $tenant_id "
            "AND n.status = 'ACTIVE' "
            "AND ($cursor IS NULL OR n.source_uri > $cursor) "
            "RETURN n.source_uri AS source_uri, n.node_type AS node_type, "
            "n.l0_abstract AS l0_abstract, n.status AS status "
            "ORDER BY n.source_uri LIMIT $limit",
            {"prefix": prefix, "cursor": cursor, "limit": limit + 1, "tenant_id": tenant_id},
            timeout_s=5,
        )
    except Exception:
        rows = []
    if not rows:
        # Fallback: walk the filesystem.
        rows = _walk_fs(state, prefix, limit + 1, cursor)
    has_more = len(rows) > limit
    items = rows[:limit]
    next_cursor = items[-1]["source_uri"] if has_more and items else None
    return MemoryListResponse(
        items=[MemoryListItem(**r) for r in items],
        next_cursor=next_cursor,
    )


@router.get("/{source_uri:path}", response_model=MemoryResponse)
def get_memory(source_uri: str) -> MemoryResponse:
    # FastAPI lower-cases the scheme otherwise; normalise:
    if not source_uri.startswith("mem://"):
        source_uri = f"mem://{source_uri.lstrip('/')}"
    state = get_state()
    tenant_id = current_tenant_id()
    if state.cfg.canonical_memory.enabled:
        repository = _require_canonical_repository(state)
        node = repository.resolve_memory_ref(
            source_uri,
            tenant_id=tenant_id,
            include_historical=True,
        )
        if node is None:
            status_code = (
                state.cfg.canonical_memory.legacy_uri_status_code
                if is_legacy_file_uri(source_uri)
                else 404
            )
            detail = (
                "legacy filesystem memory references are retired"
                if status_code == 410
                else f"memory not found: {source_uri}"
            )
            raise HTTPException(status_code=status_code, detail=detail)
        memory = repository.get_current_state(
            node.id,
            tenant_id=tenant_id,
            include_historical_claims=True,
        )
        if memory is None:
            raise HTTPException(status_code=404, detail=f"memory not found: {source_uri}")
        return MemoryResponse(
            source_uri=memory.node.canonical_uri,
            memory_id=str(memory.node.id),
            canonical_uri=memory.node.canonical_uri,
            revision=memory.node.revision,
            memory_type=memory.node.memory_type,
            status=memory.node.status,
            body=memory.current_version.body if memory.current_version else "",
            current_version=_jsonable(memory.current_version),
            claims=[_jsonable(item) for item in memory.claims],
            evidence=[_jsonable(item) for item in memory.evidence],
            edges=[_jsonable(item) for item in memory.claims],
        )
    fs = state.fs
    if fs is None:
        raise HTTPException(status_code=503, detail="legacy filesystem store is unavailable")
    if not fs.exists(source_uri):
        raise HTTPException(status_code=404, detail=f"memory not found: {source_uri}")
    try:
        raw = fs.read(source_uri)
        mf = frontmatter.parse(raw)
    except FrontmatterError as err:
        raise HTTPException(status_code=500, detail=f"frontmatter error: {err}") from err
    # Pull outgoing RELATES_TO edges for quick navigation
    try:
        edges = state.neo4j.run_template(
            "MATCH (n:Node {tenant_id: $tenant_id, source_uri: $uri})"
            "-[r:RELATES_TO]->(m:Node {tenant_id: $tenant_id}) "
            "WHERE r.tenant_id = $tenant_id AND r.status = 'ACTIVE' "
            "RETURN r.relation_label AS relation, m.source_uri AS object_uri LIMIT 25",
            {"uri": source_uri, "tenant_id": tenant_id},
            timeout_s=5,
        )
    except Exception:
        edges = None
    return MemoryResponse(
        source_uri=source_uri,
        frontmatter=mf.frontmatter,
        body=mf.body,
        edges=edges,
    )


@router.post("/{source_uri:path}/unmerge", status_code=202)
def unmerge_memory(source_uri: str, request: Request) -> dict[str, Any]:
    """Enqueue an async unmerge (§8.6).

    The LLM-driven split runs on the consolidation worker, not on the
    request thread, so a slow Core Model call never blocks the API.
    Clients poll GET /api/v1/memories/{id} + /history to observe progress.
    """
    from engram.logging_setup import get_request_id
    from engram.tenancy import current_tenant_id

    if not source_uri.startswith("mem://"):
        source_uri = f"mem://{source_uri.lstrip('/')}"
    state = get_state()
    if state.cfg.canonical_memory.enabled:
        repository = _require_canonical_repository(state)
        tid = current_tenant_id()
        node = repository.resolve_memory_ref(
            source_uri,
            tenant_id=tid,
            include_historical=True,
        )
        if node is None:
            _raise_missing_reference(state, source_uri)
        assert node is not None
        task_id = state.control_plane.enqueue_task(
            node_id=str(node.id),
            task_type="UNMERGE_CANONICAL",
            priority=4,
            tenant_id=tid,
        )
        return {
            "source_uri": node.canonical_uri,
            "memory_id": str(node.id),
            "task_id": task_id,
            "status": "PENDING" if task_id else "ALREADY_QUEUED",
        }
    fs = state.fs
    if fs is None:
        raise HTTPException(status_code=503, detail="legacy filesystem store is unavailable")
    if not fs.exists(source_uri):
        raise HTTPException(status_code=404, detail="memory not found")
    tid = current_tenant_id()
    task_id = state.control_plane.enqueue_task(
        node_id=source_uri,
        task_type="UNMERGE",
        priority=4,
        tenant_id=tid,
    )
    state.audit.record(
        tenant_id=tid,
        actor=_bearer_actor(request),
        action="memory.unmerge.request",
        target=source_uri,
        details={"task_id": task_id},
        request_id=get_request_id(),
        remote_addr=(request.client.host if request.client else None),
    )
    return {
        "source_uri": source_uri,
        "task_id": task_id,
        "status": "PENDING" if task_id else "ALREADY_QUEUED",
    }


@router.post("/{source_uri:path}/retire")
def retire_memory(source_uri: str, request: Request) -> dict[str, str]:
    from engram.logging_setup import get_request_id
    from engram.tenancy import current_tenant_id

    if not source_uri.startswith("mem://"):
        source_uri = f"mem://{source_uri.lstrip('/')}"
    state = get_state()
    if state.cfg.canonical_memory.enabled:
        repository = _require_canonical_repository(state)
        tid = current_tenant_id()
        node = repository.resolve_memory_ref(
            source_uri,
            tenant_id=tid,
            include_historical=True,
        )
        if node is None:
            _raise_missing_reference(state, source_uri)
        assert node is not None
        retired = repository.retire_memory(
            node.id,
            tenant_id=tid,
            mutation_key=f"retire:{node.id}:{get_request_id()}",
        )
        state.audit.record(
            tenant_id=tid,
            actor=_bearer_actor(request),
            action="memory.retire",
            target=retired.canonical_uri,
            request_id=get_request_id(),
            remote_addr=(request.client.host if request.client else None),
        )
        return {
            "source_uri": retired.canonical_uri,
            "memory_id": str(retired.id),
            "status": retired.status,
        }
    fs = state.fs
    if fs is None:
        raise HTTPException(status_code=503, detail="legacy filesystem store is unavailable")
    if not fs.exists(source_uri):
        raise HTTPException(status_code=404, detail="memory not found")
    raw = fs.read(source_uri)
    mf = frontmatter.parse(raw)
    mf.frontmatter["status"] = "HISTORICAL"
    fs.write_atomic(source_uri, mf.serialize())
    try:
        state.neo4j.run_template(
            "MATCH (n:Node {tenant_id: $tenant_id, source_uri: $uri}) SET n.status = 'HISTORICAL'",
            {"uri": source_uri},
        )
    except Exception:
        log.exception("failed to retire node in Neo4j: %s", source_uri)
    tid = current_tenant_id()
    state.audit.record(
        tenant_id=tid,
        actor=_bearer_actor(request),
        action="memory.retire",
        target=source_uri,
        request_id=get_request_id(),
        remote_addr=(request.client.host if request.client else None),
    )
    return {"source_uri": source_uri, "status": "HISTORICAL"}


def _bearer_actor(request) -> str:
    import hashlib

    authz = (request.headers.get("authorization") or "").split(" ", 1)[-1]
    if not authz:
        return "anonymous"
    return f"key:{hashlib.sha256(authz.encode()).hexdigest()[:12]}"


@router.get("/{source_uri:path}/history")
def history(source_uri: str) -> dict[str, Any]:
    if not source_uri.startswith("mem://"):
        source_uri = f"mem://{source_uri.lstrip('/')}"
    state = get_state()
    tenant_id = current_tenant_id()
    if state.cfg.canonical_memory.enabled:
        repository = _require_canonical_repository(state)
        node = repository.resolve_memory_ref(
            source_uri,
            tenant_id=tenant_id,
            include_historical=True,
        )
        if node is None:
            _raise_missing_reference(state, source_uri)
        assert node is not None
        result = repository.get_history(node.id, tenant_id=tenant_id)
        return {
            "source_uri": node.canonical_uri,
            "memory_id": str(node.id),
            "history": _jsonable(result),
        }
    try:
        rows = state.neo4j.run_template(
            "MATCH (latest:Node {tenant_id: $tenant_id, source_uri: $uri}) "
            "MATCH path = (latest)-[:SUPERSEDES*0..]->(n:Node) "
            "WHERE n.tenant_id = $tenant_id "
            "AND all(rel IN relationships(path) WHERE rel.tenant_id = $tenant_id) "
            "RETURN n.source_uri AS source_uri, n.status AS status, "
            "n.created_at AS created_at, length(path) AS distance "
            "ORDER BY distance LIMIT 50",
            {"uri": source_uri, "tenant_id": tenant_id},
            timeout_s=5,
        )
    except Exception:
        rows = []
    return {"source_uri": source_uri, "history": rows}


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _require_canonical_repository(state: Any):
    repository = state.memory_repository
    if repository is None:
        raise HTTPException(status_code=503, detail="canonical memory repository is unavailable")
    return repository


def _walk_fs(state, prefix: str, limit: int, cursor: str | None) -> list[dict[str, Any]]:
    """Filesystem fallback when Neo4j is unavailable (e.g. early boot)."""
    from engram import uri as uri_mod

    fs = state.fs
    if fs is None:
        return []
    try:
        tenant_root = fs.tenant_scope_path()
        start_path = tenant_root if prefix == "mem://" else fs.path_for(prefix)
    except Exception:
        return []
    if not start_path.exists():
        return []
    items: list[dict[str, Any]] = []
    for path in sorted(start_path.rglob("*.md")):
        u = uri_mod.path_to_uri(path, tenant_root)
        if cursor and u <= cursor:
            continue
        try:
            mf = frontmatter.parse(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        items.append(
            {
                "source_uri": u,
                "node_type": mf.frontmatter.get("node_type"),
                "l0_abstract": mf.body.splitlines()[0][:200] if mf.body.strip() else None,
                "status": mf.frontmatter.get("status"),
            }
        )
        if len(items) >= limit:
            break
    return items


def _raise_missing_reference(state: Any, reference: str) -> None:
    status_code = (
        state.cfg.canonical_memory.legacy_uri_status_code if is_legacy_file_uri(reference) else 404
    )
    detail = (
        "legacy filesystem memory references are retired"
        if status_code == 410
        else "memory not found"
    )
    raise HTTPException(status_code=status_code, detail=detail)


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if is_dataclass(value):
        return _jsonable(asdict(cast(Any, value)))
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (UUID, datetime, date)):
        return value.isoformat() if not isinstance(value, UUID) else str(value)
    return value
