"""Event ledger endpoints: retry a FAILED event (§11.1, §12)."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from engram.api.auth import AuthDep
from engram.deps import AppState, get_state, make_ingest_context
from engram.ingest.worker import process_event

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/events", tags=["events"], dependencies=[AuthDep])


class EventResponse(BaseModel):
    event_id: str
    status: str
    retry_count: int


@router.post("/{event_id}/retry", response_model=EventResponse)
def retry_event(event_id: str, background: BackgroundTasks) -> EventResponse:
    state = get_state()
    event = state.sqlite.get_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    # Reset to RECEIVED and let the background worker try again.
    with state.sqlite.transaction() as conn:
        conn.execute(
            "UPDATE events SET status = 'RECEIVED', error_message = NULL, "
            "retry_count = retry_count + 1 WHERE event_id = ?",
            (event_id,),
        )
    background.add_task(_drive, state, event_id)
    refreshed = state.sqlite.get_event(event_id) or event
    return EventResponse(
        event_id=event_id,
        status=refreshed["status"],
        retry_count=refreshed.get("retry_count", 0),
    )


def _drive(state: AppState, event_id: str) -> None:
    try:
        ctx = make_ingest_context(state)
        process_event(ctx, event_id)
    except Exception:
        log.exception("retry worker failed for %s", event_id)
