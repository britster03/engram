"""Event ledger endpoints: retry a FAILED event (§11.1, §12)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator

from engram.api.auth import AuthDep
from engram.deps import get_state
from engram.ingest.provenance import source_context
from engram.projection_status import projection_status_for_event
from engram.storage.idempotency import IdempotencyConflictError, payload_fingerprint
from engram.tenancy import current_tenant_id

router = APIRouter(prefix="/api/v1/events", tags=["events"], dependencies=[AuthDep])


class EventResponse(BaseModel):
    event_id: str
    status: str
    retry_count: int
    projection_status: str


class ReprocessRequest(BaseModel):
    idempotency_key: str = Field(min_length=1, max_length=128)
    reason: str = Field(min_length=1, max_length=1000)

    @field_validator("idempotency_key", "reason")
    @classmethod
    def nonempty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be blank")
        return value


@router.post("/{event_id}/reprocess", response_model=EventResponse, status_code=202)
def reprocess_event(event_id: str, request: ReprocessRequest) -> EventResponse:
    from engram.ingest.reprocessing import request_reprocessing

    state = get_state()
    if not (state.cfg.canonical_memory.enabled and state.cfg.temporal.enabled
            and state.cfg.canonical_memory.evidence_mode == "enforce"):
        raise HTTPException(status_code=409, detail="reprocessing requires canonical strict evidence mode and Temporal")
    tenant_id = current_tenant_id()
    try:
        event = request_reprocessing(state, event_id, tenant_id=tenant_id,
                                     request_key=request.idempotency_key, reason=request.reason)
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except (IdempotencyConflictError, ValueError) as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return EventResponse(event_id=event["event_id"], status=event["status"],
                         retry_count=event.get("retry_count", 0),
                         projection_status=projection_status_for_event(state, event["event_id"], tenant_id=tenant_id) or "PENDING")


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


@router.get("/{event_id}/verification")
def get_event_verification(event_id: str) -> dict[str, Any]:
    """Inspect accepted/rejected/deferred claims and source-based reasons."""
    state = get_state()
    tenant_id = current_tenant_id()
    event = state.control_plane.get_event(event_id, tenant_id=tenant_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    repository = getattr(state, "memory_repository", None)
    if repository is None:
        raise HTTPException(status_code=404, detail="claim verification is not available")
    report = repository.get_claim_verification(event_id, tenant_id=tenant_id)
    if report is None:
        raise HTTPException(status_code=404, detail="claim verification has not completed")
    payload = event["payload"]
    return {
        **report,
        "sources": source_context(payload),
        "preceding_context": payload.get("session_context"),
        "source_matches": report["binding"]["source_hash"] == payload_fingerprint(payload),
    }


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
        try:
            refreshed = state.control_plane.retry_event(event_id, tenant_id=tenant_id)
        except ValueError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
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
