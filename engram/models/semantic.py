"""Typed contracts and bounded repair for semantic model tasks."""

from __future__ import annotations

from typing import Any, Literal, TypeVar

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    ValidationError,
    model_validator,
)

from engram import metrics as metrics_mod
from engram.models.core import CompletionResult, CoreModelError, CoreModelProvider


class SemanticModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class GateWriteOutput(SemanticModel):
    store: StrictBool
    reason: str = Field(default="", max_length=2_000)


class ExtractedTriplet(SemanticModel):
    subject: str = Field(min_length=1, max_length=1_000)
    relation: str = Field(min_length=1, max_length=200, pattern=r"^[a-z0-9_]+$")
    object: str = Field(min_length=1, max_length=2_000)
    confidence: float = Field(ge=0.0, le=1.0)
    object_kind: Literal["ENTITY", "LITERAL"] | None = None
    explicit_correction: StrictBool = False


class ExtractOutput(SemanticModel):
    resolved_text: str = Field(max_length=64_000)
    triplets: list[ExtractedTriplet] = Field(max_length=200)
    l0_abstract: str = Field(min_length=1, max_length=2_000)


RetrievalCommandName = Literal[
    "find",
    "ls",
    "cat",
    "overview",
    "rel",
    "history",
    "t_top_k_vector",
    "t_children_of",
    "t_neighbours_by_relation",
    "t_path_between",
    "t_temporal_filter",
    "t_history_chain",
    "t_find_by_uri_prefix",
    "t_cross_references",
]


class RetrievalCommand(SemanticModel):
    # A Literal is carried into the provider-level JSON schema, preventing a
    # planner from inventing commands that Engram will silently ignore.
    template: RetrievalCommandName
    params: dict[str, Any] = Field(default_factory=dict)


class L1PlanOutput(SemanticModel):
    session_sufficient: StrictBool = False
    predicted_depth: Literal["SESSION", "L1", "L2", "L3", "L4"] = "L4"
    mode: Literal["AGFS", "KG", "HYBRID"] = "HYBRID"
    entry_points: list[str] = Field(default_factory=list, max_length=50)
    vector_queries: list[str] = Field(default_factory=list, max_length=3)
    commands: list[RetrievalCommand] = Field(default_factory=list, max_length=20)
    session_answer_context: str | None = Field(default=None, max_length=64_000)


class Coverage(SemanticModel):
    aspects_covered: list[str] = Field(default_factory=list, max_length=100)
    aspects_missing: list[str] = Field(default_factory=list, max_length=100)


class LnPlanOutput(SemanticModel):
    previous_level_sufficient: StrictBool = False
    terminate_cascade: StrictBool = False
    commands: list[RetrievalCommand] = Field(default_factory=list, max_length=20)
    coverage: Coverage = Field(default_factory=Coverage)


class OverviewOutput(SemanticModel):
    overview: str = Field(min_length=1, max_length=128_000)


class EntityLinkOutput(SemanticModel):
    matched_id: str | None = Field(default=None, max_length=2_000)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    reason: str = Field(default="", max_length=2_000)


class ConflictOutput(SemanticModel):
    case: Literal["DUPLICATE", "CONTRADICTION", "CO_EXISTENCE"]
    existing_edge_id: str | None = Field(default=None, max_length=2_000)
    reason: str = Field(default="", max_length=2_000)


class SessionCompactOutput(SemanticModel):
    compacted: str = Field(max_length=128_000)
    key_facts: list[str] = Field(default_factory=list, max_length=500)
    key_entities: list[str] = Field(default_factory=list, max_length=500)


class UnmergeSplit(SemanticModel):
    name: str = Field(min_length=1, max_length=1_000)
    l0_abstract: str = Field(min_length=1, max_length=2_000)
    triplets: list[ExtractedTriplet] = Field(default_factory=list, max_length=200)


class UnmergeOutput(SemanticModel):
    splits: list[UnmergeSplit] = Field(default_factory=list, max_length=100)


class FrontierOutput(SemanticModel):
    verdict: Literal["ANSWER", "NEED_MORE"]
    answer: str | None = Field(default=None, max_length=128_000)
    reason: str | None = Field(default=None, max_length=8_000)
    suggested_queries: list[str] = Field(default_factory=list, max_length=10)
    suggested_depth: Literal["L1", "L2", "L3", "L4"] | None = None

    @model_validator(mode="after")
    def validate_verdict_payload(self) -> FrontierOutput:
        if self.verdict == "ANSWER" and not self.answer:
            raise ValueError("ANSWER requires non-empty answer")
        if self.verdict == "NEED_MORE" and not self.reason:
            raise ValueError("NEED_MORE requires a reason")
        return self


SemanticT = TypeVar("SemanticT", bound=SemanticModel)


def complete_validated(
    provider: CoreModelProvider,
    *,
    task: str,
    schema: type[SemanticT],
    system_prompt: str,
    user_prompt: str,
    max_tokens: int | None = None,
    temperature: float | None = None,
) -> tuple[SemanticT, CompletionResult]:
    """Validate semantic JSON and retry one time with a bounded repair request."""
    provider_name = str(getattr(getattr(provider, "cfg", None), "provider", "unknown"))
    repair = user_prompt
    last_error: ValidationError | None = None
    for attempt in range(2):
        try:
            result = provider.complete(
                system_prompt=system_prompt,
                user_prompt=repair,
                output_schema=schema.model_json_schema(),
                max_tokens=max_tokens,
                temperature=temperature,
            )
        except Exception as err:
            metrics_mod.core_model_calls.labels(
                task=task,
                provider=provider_name,
            ).inc(max(1, int(getattr(err, "provider_calls", 1))))
            raise
        metrics_mod.core_model_calls.labels(
            task=task,
            provider=provider_name,
        ).inc(max(1, result.provider_calls))
        if result.tokens_in is not None:
            metrics_mod.core_model_tokens.labels(
                task=task,
                provider=provider_name,
                direction="in",
            ).inc(result.tokens_in)
        if result.tokens_out is not None:
            metrics_mod.core_model_tokens.labels(
                task=task,
                provider=provider_name,
                direction="out",
            ).inc(result.tokens_out)
        if result.latency_ms is not None:
            metrics_mod.core_model_latency.labels(
                task=task,
                provider=provider_name,
            ).observe(result.latency_ms / 1000.0)
        try:
            return schema.model_validate(result.output), result
        except ValidationError as err:
            last_error = err
            metrics_mod.semantic_output_schema_failures.labels(
                task=task, provider=provider_name
            ).inc()
            if attempt == 0:
                repair = (
                    f"{user_prompt}\n\nThe prior response violated the output schema. "
                    "Return one corrected JSON object only. Do not add prose or fields."
                )
    count = len(last_error.errors()) if last_error is not None else 0
    raise CoreModelError(f"{task} output failed schema validation ({count} issue(s))")


def validate_frontier_output(output: Any) -> FrontierOutput:
    """Convert provider JSON into the shared frontier contract."""
    try:
        return FrontierOutput.model_validate(output)
    except ValidationError as err:
        raise CoreModelError(
            f"frontier output failed schema validation ({len(err.errors())} issue(s))"
        ) from err
