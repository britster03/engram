"""Shared acceptance contract for public and Admin explicit ingestion."""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from engram.api.schemas import IngestRequest, IngestResponse
from engram.projection_status import projection_status_for_event
from engram.storage.idempotency import IdempotencyConflictError, request_pair_id


def accept_ingest(state: Any, req: IngestRequest, *, tenant_id: str) -> IngestResponse:
    req.effective_pair()
    pid = request_pair_id(req.idempotency_key)
    excluded = {"idempotency_key"}
    if not req.entity_bindings:
        excluded.add("entity_bindings")  # preserve existing keyed request payloads across upgrades
    try:
        event_id, is_new = state.control_plane.record_event(
            pair_id=pid,
            session_id=req.session_id,
            source=req.source,
            event_type="INGEST",
            payload=req.model_dump(mode="json", exclude=excluded),
            tenant_id=tenant_id,
            require_matching_payload=True,
        )
    except IdempotencyConflictError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    event = None if is_new else state.control_plane.get_event(event_id, tenant_id=tenant_id)
    projection_status = (
        projection_status_for_event(state, event_id, tenant_id=tenant_id)
        if not is_new else None
    )
    return IngestResponse(
        event_id=event_id,
        pair_id=pid,
        status=str(event["status"]) if event else "RECEIVED",
        projection_status=projection_status or "PENDING",
    )
