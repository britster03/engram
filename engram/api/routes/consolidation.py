"""Consolidation ops endpoints: status + manual trigger (§11.1)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from engram.api.auth import AuthDep
from engram.deps import get_state

router = APIRouter(prefix="/api/v1/consolidation", tags=["consolidation"], dependencies=[AuthDep])


class TriggerRequest(BaseModel):
    node_id: str
    task_type: str = "CONSOLIDATE_OVERVIEW"
    priority: int = 5
    subtree: bool = False


class StatusResponse(BaseModel):
    queue_depth: int
    by_task_type: dict[str, int]
    by_status: dict[str, int]


@router.get("/status", response_model=StatusResponse)
def status() -> StatusResponse:
    state = get_state()
    conn = state.sqlite.get_conn()
    by_task_type: dict[str, int] = {}
    for row in conn.execute(
        "SELECT task_type, COUNT(*) AS c FROM consolidation_tasks "
        "WHERE status IN ('PENDING', 'PROCESSING') GROUP BY task_type"
    ).fetchall():
        by_task_type[row["task_type"]] = int(row["c"])
    by_status: dict[str, int] = {}
    for row in conn.execute(
        "SELECT status, COUNT(*) AS c FROM consolidation_tasks GROUP BY status"
    ).fetchall():
        by_status[row["status"]] = int(row["c"])
    return StatusResponse(
        queue_depth=state.sqlite.queue_depth(),
        by_task_type=by_task_type,
        by_status=by_status,
    )


@router.post("/trigger")
def trigger(req: TriggerRequest) -> dict[str, Any]:
    state = get_state()
    allowed = {
        "CONSOLIDATE_OVERVIEW",
        "REGENERATE_MANIFEST",
        "PROPAGATE_OVERVIEW",
        "ATOMIZE",
        "NORMALIZE",
        "TEMPORALIZE",
        "INTEGRATE",
        "UNMERGE",
    }
    if req.task_type not in allowed:
        raise HTTPException(status_code=400, detail=f"unknown task_type: {req.task_type}")
    task_id = state.sqlite.enqueue_task(
        node_id=req.node_id, task_type=req.task_type, priority=req.priority
    )
    return {"task_id": task_id, "status": "PENDING" if task_id else "ALREADY_QUEUED"}
