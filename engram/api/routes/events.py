"""Event ledger endpoints: retry a FAILED event (§11.1, §12)."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from engram.api.auth import AuthDep
from engram.deps import get_state
from engram.tenancy import current_tenant_id

router = APIRouter(prefix="/api/v1/events", tags=["events"], dependencies=[AuthDep])


class EventResponse(BaseModel):
    event_id: str
    status: str
    retry_count: int


@router.post("/{event_id}/retry", response_model=EventResponse)
def retry_event(event_id: str) -> EventResponse:
    state = get_state()
    tenant_id = current_tenant_id()
    event = state.sqlite.get_event(event_id, tenant_id=tenant_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    # Reset to RECEIVED and let the background worker try again.
    with state.sqlite.transaction() as conn:
        conn.execute(
            "UPDATE events SET status = 'RECEIVED', error_message = NULL, "
            "retry_count = retry_count + 1 WHERE event_id = ? AND tenant_id = ?",
            (event_id, tenant_id),
        )
    refreshed = state.sqlite.get_event(event_id, tenant_id=tenant_id) or event
    return EventResponse(
        event_id=event_id,
        status=refreshed["status"],
        retry_count=refreshed.get("retry_count", 0),
    )
