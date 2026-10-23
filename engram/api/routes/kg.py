"""Knowledge-graph visualization API."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from engram.api.auth import AuthDep
from engram.deps import get_state
from engram.tenancy import current_tenant_id

router = APIRouter(prefix="/api/v1/kg", tags=["kg"], dependencies=[AuthDep])


class GraphNode(BaseModel):
    id: str
    source_uri: str
    label: str | None = None
    node_type: str | None = None
    status: str | None = None
    l0_abstract: str | None = None
    relative_path: str | None = None
    language: str | None = None
    signature: str | None = None
    line_start: int | None = None
    line_end: int | None = None


class GraphEdge(BaseModel):
    source: str
    target: str
    type: str
    label: str | None = None
    status: str | None = None


class GraphResponse(BaseModel):
    nodes: list[GraphNode]
    edges: list[GraphEdge]
    limit: int
    depth: int


class CodeProjectResponse(BaseModel):
    source_uri: str
    name: str
    l0_abstract: str | None = None
    created_at: str | None = None


class CodeMapResponse(BaseModel):
    project_uri: str
    nodes: list[GraphNode]
    edges: list[GraphEdge]
    limit: int
    depth: int


class CodeRelationship(BaseModel):
    source_uri: str
    display_name: str | None = None
    node_type: str | None = None
    relation: str | None = None
    label: str | None = None
    source_path: str | None = None
    line_start: int | None = None
    line_end: int | None = None
    confidence: float | None = None
    resolution: str | None = None


class CodeNodeDetailResponse(BaseModel):
    node: dict[str, Any]
    outgoing: list[CodeRelationship]
    incoming: list[CodeRelationship]


@router.get("/graph", response_model=GraphResponse)
def graph(
    root_uri: str | None = Query(default=None),
    depth: int = Query(default=1, ge=0, le=4),
    limit: int = Query(default=100, ge=1, le=500),
    type: Literal[
        "ENTITY",
        "EPISODE",
        "EVENT",
        "FACT",
        "DOCUMENT",
        "DIRECTORY",
        "SESSION_SUMMARY",
        "COLLECTION",
        "PROFILE",
        "PREFERENCE",
        "PROJECT",
        "FILE",
        "CLASS",
        "FUNCTION",
        "METHOD",
        "EXTERNAL_MODULE",
    ]
    | None = Query(default=None),
) -> GraphResponse:
    state = get_state()
    try:
        payload: dict[str, Any] = state.neo4j.graph(
            root_uri=root_uri,
            depth=depth,
            limit=limit,
            node_type=type,
            tenant_id=current_tenant_id(),
        )
    except AttributeError as err:
        raise HTTPException(
            status_code=500, detail="KG backend does not support graph view"
        ) from err
    except Exception as err:
        raise HTTPException(status_code=503, detail=f"KG graph query failed: {err}") from err
    return GraphResponse(
        nodes=[GraphNode(**n) for n in payload.get("nodes", [])],
        edges=[GraphEdge(**e) for e in payload.get("edges", [])],
        limit=limit,
        depth=depth,
    )


@router.get("/projects", response_model=list[CodeProjectResponse])
def code_projects() -> list[CodeProjectResponse]:
    state = get_state()
    try:
        projects = state.neo4j.list_code_projects(tenant_id=current_tenant_id())
    except AttributeError as err:
        raise HTTPException(status_code=500, detail="KG backend does not support Code map") from err
    except Exception as err:
        raise HTTPException(status_code=503, detail=f"Code project query failed: {err}") from err
    return [CodeProjectResponse(**project) for project in projects]


@router.get("/code-map", response_model=CodeMapResponse)
def code_map(
    project_uri: str = Query(..., min_length=1),
    depth: int = Query(default=2, ge=0, le=6),
    limit: int = Query(default=200, ge=1, le=500),
) -> CodeMapResponse:
    state = get_state()
    try:
        payload: dict[str, Any] = state.neo4j.code_map(
            project_uri=project_uri,
            depth=depth,
            limit=limit,
            tenant_id=current_tenant_id(),
        )
    except AttributeError as err:
        raise HTTPException(status_code=500, detail="KG backend does not support Code map") from err
    except Exception as err:
        raise HTTPException(status_code=503, detail=f"Code map query failed: {err}") from err
    if not payload.get("nodes"):
        raise HTTPException(status_code=404, detail="code project not found")
    return CodeMapResponse(
        project_uri=project_uri,
        nodes=[GraphNode(**node) for node in payload.get("nodes", [])],
        edges=[GraphEdge(**edge) for edge in payload.get("edges", [])],
        limit=limit,
        depth=depth,
    )


@router.get("/node-details", response_model=CodeNodeDetailResponse)
def code_node_details(source_uri: str = Query(..., min_length=1)) -> CodeNodeDetailResponse:
    state = get_state()
    try:
        payload = state.neo4j.code_node_details(
            source_uri=source_uri, tenant_id=current_tenant_id()
        )
    except AttributeError as err:
        raise HTTPException(
            status_code=500, detail="KG backend does not support code details"
        ) from err
    except Exception as err:
        raise HTTPException(status_code=503, detail=f"Code details query failed: {err}") from err
    if payload is None:
        raise HTTPException(status_code=404, detail="code node not found")
    clean = {
        direction: [item for item in payload.get(direction, []) if item.get("source_uri")]
        for direction in ("incoming", "outgoing")
    }
    return CodeNodeDetailResponse(node=payload["node"], **clean)
