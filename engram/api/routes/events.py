"""Event ledger endpoints: retry a FAILED event (§11.1, §12)."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

from engram.api.auth import AuthDep
from engram.deps import get_state
from engram.tenancy import current_tenant_id

router = APIRouter(prefix="/api/v1/events", tags=["events"], dependencies=[AuthDep])


class EventResponse(BaseModel):
    event_id: str
    status: str
    retry_count: int


class EventStatusRequest(BaseModel):
    event_ids: list[str] = Field(..., min_length=1, max_length=500)

    @field_validator("event_ids")
    @classmethod
    def _unique_ids(cls, value: list[str]) -> list[str]:
        if any(not event_id or len(event_id) > 128 for event_id in value):
            raise ValueError("event IDs must contain 1 to 128 characters")
        if len(set(value)) != len(value):
            raise ValueError("event_ids must be unique")
        return value


class EventReadiness(BaseModel):
    event_id: str
    pair_id: str
    status: str
    terminal: bool
    memory_ready: bool
    outbox_state: str | None = None
    source_uri: str | None = None
    completed_stage: str | None = None
    artifact_count: int = 0
    filesystem_ready_count: int = 0
    kg_ready_count: int = 0
    artifact_error_count: int = 0
    error: str | None = None
    created_at: str | None = None
    processed_at: str | None = None


class EventStatusResponse(BaseModel):
    requested_count: int
    found_count: int
    terminal_count: int
    ready_count: int
    failed_count: int
    memory_ready: bool
    missing_ids: list[str]
    failures: list[EventReadiness]
    events: list[EventReadiness]


class EventListResponse(BaseModel):
    source: str | None
    total_count: int
    event_ids: list[str]


_TERMINAL_STATUSES = {"COMPLETE", "GATED_SKIP", "FAILED"}


@router.get("", response_model=EventListResponse)
def list_events(
    source: str | None = Query(default=None, min_length=1, max_length=64),
    limit: int = Query(default=500, ge=1, le=500),
) -> EventListResponse:
    """Enumerate a bounded, tenant-scoped event set for exact corpus checks."""
    state = get_state()
    total, event_ids = state.sqlite.list_event_ids(
        tenant_id=current_tenant_id(),
        source=source,
        limit=limit,
    )
    return EventListResponse(source=source, total_count=total, event_ids=event_ids)


@router.post("/status", response_model=EventStatusResponse)
def event_status(req: EventStatusRequest) -> EventStatusResponse:
    """Report exact event readiness without relying on queue-level heuristics."""
    state = get_state()
    tenant_id = current_tenant_id()
    rows = state.sqlite.get_event_readiness(req.event_ids, tenant_id=tenant_id)
    found = {row["event_id"] for row in rows}
    events: list[EventReadiness] = []
    for row in rows:
        status = str(row["status"])
        artifact_count = int(row.get("artifact_count") or 0)
        filesystem_ready_count = int(row.get("filesystem_ready_count") or 0)
        kg_ready_count = int(row.get("kg_ready_count") or 0)
        artifact_error_count = int(row.get("artifact_error_count") or 0)
        completed_stage = row.get("completed_stage")
        if artifact_count:
            memory_ready = (
                completed_stage
                in {"KG_COMMITTED", "CONSOLIDATION_COMMITTED", "COMPLETE"}
                and filesystem_ready_count == artifact_count
                and kg_ready_count == artifact_count
                and artifact_error_count == 0
            )
        else:
            # Compatibility for ledgers created before artifact-level outbox records.
            memory_ready = status == "GATED_SKIP" or (
                status in {"INDEXED", "COMPLETE"} and row.get("outbox_state") == "INDEXED"
            )
        events.append(
            EventReadiness(
                event_id=row["event_id"],
                pair_id=row["pair_id"],
                status=status,
                terminal=status in _TERMINAL_STATUSES,
                memory_ready=memory_ready,
                outbox_state=row.get("outbox_state"),
                source_uri=row.get("source_uri"),
                completed_stage=completed_stage,
                artifact_count=artifact_count,
                filesystem_ready_count=filesystem_ready_count,
                kg_ready_count=kg_ready_count,
                artifact_error_count=artifact_error_count,
                error=row.get("error_message"),
                created_at=row.get("created_at"),
                processed_at=row.get("processed_at"),
            )
        )
    failures = [event for event in events if event.status == "FAILED"]
    missing = [event_id for event_id in req.event_ids if event_id not in found]
    ready_count = sum(event.memory_ready for event in events)
    return EventStatusResponse(
        requested_count=len(req.event_ids),
        found_count=len(events),
        terminal_count=sum(event.terminal for event in events),
        ready_count=ready_count,
        failed_count=len(failures),
        memory_ready=ready_count == len(req.event_ids) and not missing and not failures,
        missing_ids=missing,
        failures=failures,
        events=events,
    )


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
