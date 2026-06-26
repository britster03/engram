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


@router.get("/graph", response_model=GraphResponse)
def graph(
    root_uri: str | None = Query(default=None),
    depth: int = Query(default=1, ge=0, le=4),
    limit: int = Query(default=100, ge=1, le=500),
    type: Literal["ENTITY", "EVENT", "FACT", "DOCUMENT", "DIRECTORY", "SESSION_SUMMARY"] | None = Query(default=None),
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
        raise HTTPException(status_code=500, detail="KG backend does not support graph view") from err
    except Exception as err:
        raise HTTPException(status_code=503, detail=f"KG graph query failed: {err}") from err
    return GraphResponse(
        nodes=[GraphNode(**n) for n in payload.get("nodes", [])],
        edges=[GraphEdge(**e) for e in payload.get("edges", [])],
        limit=limit,
        depth=depth,
    )
