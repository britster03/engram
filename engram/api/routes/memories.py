"""Memory-node endpoints (§11.1)."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from engram.api.auth import AuthDep
from engram import frontmatter
from engram.frontmatter import FrontmatterError
from engram.deps import get_state

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/memories", tags=["memories"], dependencies=[AuthDep])


class MemoryResponse(BaseModel):
    source_uri: str
    frontmatter: dict[str, Any]
    body: str
    edges: list[dict[str, Any]] | None = None


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
    # Prefer Neo4j for listing if possible; fall back to filesystem walk for empty KG.
    try:
        rows = state.neo4j.run_template(
            "MATCH (n:Node) WHERE n.source_uri STARTS WITH $prefix "
            "AND n.status = 'ACTIVE' "
            "AND ($cursor IS NULL OR n.source_uri > $cursor) "
            "RETURN n.source_uri AS source_uri, n.node_type AS node_type, "
            "n.l0_abstract AS l0_abstract, n.status AS status "
            "ORDER BY n.source_uri LIMIT $limit",
            {"prefix": prefix, "cursor": cursor, "limit": limit + 1},
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
    if not state.fs.exists(source_uri):
        raise HTTPException(status_code=404, detail=f"memory not found: {source_uri}")
    try:
        raw = state.fs.read(source_uri)
        mf = frontmatter.parse(raw)
    except FrontmatterError as err:
        raise HTTPException(status_code=500, detail=f"frontmatter error: {err}") from err
    # Pull outgoing RELATES_TO edges for quick navigation
    try:
        edges = state.neo4j.run_template(
            "MATCH (n:Node {source_uri: $uri})-[r:RELATES_TO]->(m:Node) "
            "WHERE r.status = 'ACTIVE' "
            "RETURN r.relation_label AS relation, m.source_uri AS object_uri LIMIT 25",
            {"uri": source_uri},
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
    if not state.fs.exists(source_uri):
        raise HTTPException(status_code=404, detail="memory not found")
    tid = current_tenant_id()
    task_id = state.sqlite.enqueue_task(
        node_id=source_uri, task_type="UNMERGE", priority=4, tenant_id=tid,
    )
    state.audit.record(
        tenant_id=tid, actor=_bearer_actor(request), action="memory.unmerge.request",
        target=source_uri, details={"task_id": task_id},
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
    if not state.fs.exists(source_uri):
        raise HTTPException(status_code=404, detail="memory not found")
    raw = state.fs.read(source_uri)
    mf = frontmatter.parse(raw)
    mf.frontmatter["status"] = "HISTORICAL"
    state.fs.write_atomic(source_uri, mf.serialize())
    try:
        state.neo4j.run_template(
            "MATCH (n:Node {tenant_id: $tenant_id, source_uri: $uri}) "
            "SET n.status = 'HISTORICAL'",
            {"uri": source_uri},
        )
    except Exception:
        log.exception("failed to retire node in Neo4j: %s", source_uri)
    tid = current_tenant_id()
    state.audit.record(
        tenant_id=tid, actor=_bearer_actor(request), action="memory.retire",
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
    try:
        rows = state.neo4j.run_template(
            "MATCH (latest:Node {source_uri: $uri}) "
            "MATCH path = (latest)-[:SUPERSEDES*0..]->(n:Node) "
            "RETURN n.source_uri AS source_uri, n.status AS status, "
            "n.created_at AS created_at, length(path) AS distance "
            "ORDER BY distance LIMIT 50",
            {"uri": source_uri},
            timeout_s=5,
        )
    except Exception:
        rows = []
    return {"source_uri": source_uri, "history": rows}


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _walk_fs(state, prefix: str, limit: int, cursor: str | None) -> list[dict[str, Any]]:
    """Filesystem fallback when Neo4j is unavailable (e.g. early boot)."""
    from engram import uri as uri_mod

    try:
        start_path = state.fs.path_for(prefix)
    except Exception:
        return []
    if not start_path.exists():
        return []
    items: list[dict[str, Any]] = []
    for path in sorted(start_path.rglob("*.md")):
        u = uri_mod.path_to_uri(path, state.fs.data_dir)
        if cursor and u <= cursor:
            continue
        try:
            mf = frontmatter.parse(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        items.append({
            "source_uri": u,
            "node_type": mf.frontmatter.get("node_type"),
            "l0_abstract": mf.body.splitlines()[0][:200] if mf.body.strip() else None,
            "status": mf.frontmatter.get("status"),
        })
        if len(items) >= limit:
            break
    return items
