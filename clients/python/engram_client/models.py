"""Response models mirror the server's Pydantic schemas."""

from __future__ import annotations

from pydantic import BaseModel


class RetrievalMetadata(BaseModel):
    cascade_depth_reached: str
    levels_visited: list[str]
    predicted_depth: str | None = None
    nodes_retrieved: int = 0
    total_context_tokens: int = 0
    reentries: int = 0
    latency_ms: dict[str, float] = {}
    l0_decision: str | None = None
    l0_reason: str | None = None


class QueryResponse(BaseModel):
    answer: str
    session_id: str | None = None
    retrieval_metadata: RetrievalMetadata


class IngestResponse(BaseModel):
    event_id: str
    pair_id: str
    status: str


class SessionState(BaseModel):
    session_id: str
    status: str
    turn_count: int
    created_at: str
    compacted_turns: int
    key_facts: list[str]


class TenantPayload(BaseModel):
    tenant_id: str
    display_name: str
    status: str
    created_at: str
    quotas: dict[str, int]
    api_key_count: int


class HealthPayload(BaseModel):
    status: str
    components: dict[str, bool]


class ConsolidationStatus(BaseModel):
    queue_depth: int
    by_task_type: dict[str, int]
    by_status: dict[str, int]
