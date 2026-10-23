"""Request/response schemas for the REST API (§11).

All string fields carry explicit length caps. Combined with
`engram.api.body_limit.BodySizeLimitMiddleware` this gives defence in
depth against oversized payloads.
"""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator

# --- Per-field caps (characters, not tokens) -----------------------------
MAX_CONTENT_LENGTH = 32_000  # per turn
MAX_QUERY_LENGTH = 8_000  # single user query
MAX_SESSION_CONTEXT_LENGTH = 64_000
MAX_SESSION_SUMMARY_LENGTH = 16_000
MAX_ID_LENGTH = 128


class TurnContent(BaseModel):
    content: str = Field(..., max_length=MAX_CONTENT_LENGTH)
    timestamp: str | None = Field(default=None, max_length=64)
    turn_idx: int | None = Field(default=None, ge=0, le=1_000_000)
    # Optional tool-call / tool-result payloads (§5.2 turn groups).
    tool_calls: list[dict[str, Any]] | None = Field(default=None, max_length=20)
    tool_results: list[dict[str, Any]] | None = Field(default=None, max_length=20)


class TurnPair(BaseModel):
    user: TurnContent
    assistant: TurnContent


class TurnGroup(BaseModel):
    """Extended ingest unit (§5.2) covering tool-using assistant flows.

    The conventional shape is:
        user → assistant(tool_call) → tool_result → assistant(final_response)

    The outer API collapses this into `user_turn + assistant_turn` (the first
    user turn and the final assistant turn) so the downstream gate/extract
    pipeline still operates on a pair, with the tool steps preserved in the
    event payload for provenance.
    """

    user: TurnContent
    assistant: TurnContent
    intermediate: list[TurnContent] | None = None  # tool-call, tool-result, etc.


class EntityBinding(BaseModel):
    """Caller-authorized identity; extraction cannot choose cross-source IDs."""
    name: str = Field(min_length=1, max_length=256)
    aliases: list[str] = Field(default_factory=list, max_length=20)
    entity_id: UUID | None = None
    identity_key: str | None = Field(default=None, min_length=1, max_length=128)
    attributes: dict[str, str] = Field(default_factory=dict, max_length=20)

    @model_validator(mode="after")
    def identity_required(self) -> EntityBinding:
        if (self.entity_id is None) == (self.identity_key is None):
            raise ValueError("provide exactly one of entity_id or identity_key")
        if not self.name.strip() or (self.identity_key is not None and not self.identity_key.strip()):
            raise ValueError("entity identity must not be blank")
        if any(not a.strip() or len(a) > 256 for a in self.aliases):
            raise ValueError("entity aliases must contain 1-256 characters")
        if any(not k.strip() or len(k) > 128 or not v.strip() or len(v) > 512 for k, v in self.attributes.items()):
            raise ValueError("invalid distinguishing attribute")
        return self


class IngestRequest(BaseModel):
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=MAX_ID_LENGTH)
    session_id: str | None = Field(default=None, max_length=MAX_ID_LENGTH)
    turn_pair: TurnPair | None = None
    turn_group: TurnGroup | None = None
    source: str = Field(default="client", max_length=32)
    session_summary: str | None = Field(default=None, max_length=MAX_SESSION_SUMMARY_LENGTH)
    session_context: str | None = Field(default=None, max_length=MAX_SESSION_CONTEXT_LENGTH)
    force_store: bool = False
    entity_bindings: list[EntityBinding] = Field(default_factory=list, max_length=100)

    @field_validator("entity_bindings")
    @classmethod
    def unambiguous_bindings(cls, bindings: list[EntityBinding]) -> list[EntityBinding]:
        seen: set[str] = set()
        for binding in bindings:
            local: set[str] = set()
            for name in {binding.name, *binding.aliases}:
                normalized = " ".join(name.replace("_", " ").casefold().split())
                if normalized in seen:
                    raise ValueError("entity binding names and aliases must be unambiguous")
                local.add(normalized)
            seen.update(local)
        return bindings

    def effective_pair(self) -> TurnPair:
        if self.turn_pair is not None:
            return self.turn_pair
        if self.turn_group is not None:
            return TurnPair(user=self.turn_group.user, assistant=self.turn_group.assistant)
        raise ValueError("IngestRequest requires either turn_pair or turn_group")

    @field_validator("session_id", "source", "idempotency_key")
    @classmethod
    def _safe_ascii(cls, v: str | None) -> str | None:
        if v is None:
            return v
        # Session IDs and source tags should be safe for logs, filenames, metric labels.
        if any(c.isspace() for c in v):
            raise ValueError("must not contain whitespace")
        return v


class IngestResponse(BaseModel):
    event_id: str
    pair_id: str
    status: str
    projection_status: Literal["PENDING", "PROJECTING", "INDEXED", "FAILED"] = "PENDING"


class QueryRequest(BaseModel):
    session_id: str | None = Field(default=None, max_length=MAX_ID_LENGTH)
    query: str = Field(..., min_length=1, max_length=MAX_QUERY_LENGTH)
    session_context: str | None = Field(default=None, max_length=MAX_SESSION_CONTEXT_LENGTH)
    max_depth: str | None = Field(default=None, pattern=r"^L[0-4]$|^SESSION$")
    max_reentries: int | None = Field(default=None, ge=0, le=5)
    stream: bool = False


class QueryResponse(BaseModel):
    answer: str
    session_id: str | None = None
    retrieval_metadata: dict[str, Any]
    answerability: str
    sources: list[dict[str, Any]] = Field(default_factory=list)
    conflicts: list[str] = Field(default_factory=list)
    verification_rejections: list[str] = Field(default_factory=list)
    route_metadata: dict[str, Any] = Field(default_factory=dict)
    projection_metadata: dict[str, Any] = Field(default_factory=dict)


class ChatCompletionMessage(BaseModel):
    role: Literal["system", "user", "assistant"]
    content: str = Field(..., max_length=MAX_CONTENT_LENGTH)


class ChatCompletionRequest(BaseModel):
    messages: list[ChatCompletionMessage] = Field(..., min_length=1)
    session_id: str | None = Field(default=None, max_length=MAX_ID_LENGTH)
    client_message_id: str | None = Field(default=None, min_length=1, max_length=MAX_ID_LENGTH)
    stream: bool = False
    session_context: str | None = Field(default=None, max_length=MAX_SESSION_CONTEXT_LENGTH)
    max_depth: str | None = Field(default=None, pattern=r"^L[0-4]$|^SESSION$")
    max_reentries: int | None = Field(default=None, ge=0, le=5)

    @field_validator("messages")
    @classmethod
    def _messages_rules(cls, v: list[ChatCompletionMessage]) -> list[ChatCompletionMessage]:
        if not any(m.role == "user" for m in v):
            raise ValueError("messages must contain at least one user message")
        if v[-1].role != "user":
            raise ValueError("last message must be from user")
        if not v[-1].content.strip() or len(v[-1].content) > MAX_QUERY_LENGTH:
            raise ValueError(f"last user message must contain 1-{MAX_QUERY_LENGTH} characters")
        return v

    @field_validator("session_id")
    @classmethod
    def _safe_ascii(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if any(c.isspace() for c in v):
            raise ValueError("must not contain whitespace")
        return v


class ChatCompletionResponse(BaseModel):
    answer: str
    session_id: str
    run_id: str | None = None
    input_event_id: str | None = None
    retrieval_metadata: dict[str, Any]
    answerability: str
    sources: list[dict[str, Any]] = Field(default_factory=list)
    conflicts: list[str] = Field(default_factory=list)
    verification_rejections: list[str] = Field(default_factory=list)
    finish_reason: str = "stop"


class HealthResponse(BaseModel):
    status: str
    components: dict[str, bool]


class ConfigResponse(BaseModel):
    retrieval: dict[str, Any]
    core_model: dict[str, Any]
    frontier_llm: dict[str, Any]
