"""Event ledger endpoints: retry a FAILED event (§11.1, §12)."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from engram.api.auth import AuthDep
from engram.deps import get_state
from engram.projection_status import projection_status_for_event
from engram.tenancy import current_tenant_id

router = APIRouter(prefix="/api/v1/events", tags=["events"], dependencies=[AuthDep])


class EventResponse(BaseModel):
    event_id: str
    status: str
    retry_count: int
    projection_status: str


@router.get("/{event_id}", response_model=EventResponse)
def get_event(event_id: str) -> EventResponse:
    state = get_state()
    tenant_id = current_tenant_id()
    event = state.control_plane.get_event(event_id, tenant_id=tenant_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    return EventResponse(
        event_id=event_id,
        status=str(event["status"]),
        retry_count=int(event.get("retry_count") or 0),
        projection_status=(
            projection_status_for_event(state, event_id, tenant_id=tenant_id) or "PENDING"
        ),
    )


@router.post("/{event_id}/retry", response_model=EventResponse)
def retry_event(event_id: str) -> EventResponse:
    state = get_state()
    tenant_id = current_tenant_id()
    event = state.control_plane.get_event(event_id, tenant_id=tenant_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    if event.get("status") != "FAILED":
        raise HTTPException(status_code=409, detail="only failed events can be retried")
    # Postgres/Temporal retries create a new workflow generation in the same
    # transaction as the status reset. Non-Temporal mode uses PostgreSQL polling.
    if hasattr(state.control_plane, "retry_event") and state.cfg.temporal.enabled:
        refreshed = state.control_plane.retry_event(event_id, tenant_id=tenant_id)
        if refreshed is None:
            raise HTTPException(status_code=409, detail="only failed events can be retried")
    else:
        with state.control_plane.transaction() as conn:
            conn.execute(
                "UPDATE events SET status = 'RECEIVED', error_message = NULL, "
                "retry_count = retry_count + 1 WHERE event_id = ? AND tenant_id = ?",
                (event_id, tenant_id),
            )
        refreshed = state.control_plane.get_event(event_id, tenant_id=tenant_id) or event
    return EventResponse(
        event_id=event_id,
        status=refreshed["status"],
        retry_count=refreshed.get("retry_count", 0),
        projection_status=(
            projection_status_for_event(state, event_id, tenant_id=tenant_id) or "PENDING"
        ),
    )
