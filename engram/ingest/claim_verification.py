"""Source-bound semantic review of final conversational claim candidates.

The model classifies supplied candidates; Python validates complete coverage,
decides which classifications are eligible, and binds the durable result to
the exact source, candidates, tenant, event and verification policy.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from engram import prompts, tokens
from engram.config import CanonicalMemoryConfig, CoreModelConfig
from engram.ingest.provenance import source_context
from engram.models.core import CoreModelError
from engram.predicate_registry import PREDICATE_POLICY_VERSION
from engram.storage.idempotency import payload_fingerprint

VERIFICATION_VERSION = "claim-support-v1"


class ClaimVerificationBudgetError(ValueError):
    """The complete input must fit before any candidate can be accepted."""


class _Review(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    claim_id: str = Field(min_length=1)
    verdict: Literal["SUPPORTED", "CONTRADICTED", "UNCLEAR"]
    assertion: Literal[
        "DIRECT", "INFERRED", "REPORTED", "HYPOTHETICAL", "QUESTION", "UNCERTAIN", "UNKNOWN"
    ]
    polarity: Literal["POSITIVE", "NEGATIVE", "UNKNOWN"]
    reason: str = Field(min_length=1, max_length=1000)

    @field_validator("reason")
    @classmethod
    def _nonempty_reason(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("claim verification requires a reason")
        return value


class _ReviewBatch(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    reviews: list[_Review]


def verification_policy(cfg: Any) -> str:
    memory = getattr(cfg, "canonical_memory", None) or CanonicalMemoryConfig()
    model = getattr(cfg, "core_model", None) or CoreModelConfig()
    return payload_fingerprint(
        {
            "version": VERIFICATION_VERSION,
            "prompt": prompts.source_digest("verify_claims"),
            "predicate_policy": PREDICATE_POLICY_VERSION,
            "model": {
                name: getattr(model, name, None)
                for name in (
                    "provider",
                    "model_path",
                    "api_base",
                    "temperature",
                    "reasoning_effort",
                )
            },
            "budgets": verification_budgets(memory),
            "acceptance": "supported-direct-positive-no-unresolved-entity-ids",
        }
    )


def verification_budgets(memory: Any) -> dict[str, int]:
    defaults = CanonicalMemoryConfig()
    return {
        name: int(
            getattr(
                memory,
                "claim_verification_" + name,
                getattr(defaults, "claim_verification_" + name),
            )
        )
        for name in ("max_claims", "batch_size", "input_tokens", "output_tokens")
    }


def verification_binding(
    *,
    tenant_id: str,
    event_id: str,
    payload: Mapping[str, Any],
    extraction: Mapping[str, Any],
    policy: str,
) -> dict[str, str]:
    return {
        "tenant_id": tenant_id,
        "event_id": event_id,
        "policy": policy,
        "source_hash": payload_fingerprint(dict(payload)),
        "extraction_hash": payload_fingerprint(dict(extraction)),
    }


def verification_artifact_key(binding: Mapping[str, str]) -> str:
    return VERIFICATION_VERSION + ":" + payload_fingerprint(dict(binding))


def _claim_id(index: int, claim: Mapping[str, Any]) -> str:
    return f"c{index}-" + payload_fingerprint(dict(claim))[:16]


def _unrepresentable_reason(claim: Mapping[str, Any]) -> str | None:
    # The conversational writer cannot verify model-selected canonical IDs
    # against names, or represent signed/modal relationships consistently.
    for field in (
        "subject_id",
        "subject_memory_id",
        "subject_entity_id",
        "object_id",
        "object_memory_id",
        "object_entity_id",
    ):
        if claim.get(field) is not None:
            return "Canonical entity IDs require separately verified entity attribution."
    for field in ("subject", "object", "object_value"):
        value = claim.get(field)
        if isinstance(value, Mapping) and any(
            value.get(key) is not None for key in ("id", "uuid", "entity_id", "memory_id")
        ):
            return "Structured entity IDs require separately verified entity attribution."
    if claim.get("negated") or str(claim.get("polarity", "")).upper() in {"NEGATIVE", "NEGATED"}:
        return "Signed relationships are not supported by the conversational writer."
    if claim.get("modality") not in (None, "", "ASSERTED", "asserted"):
        return "Modal relationships are not supported by the conversational writer."
    return None


def _disposition(review: _Review, claim: Mapping[str, Any]) -> str:
    if review.verdict == "CONTRADICTED":
        return "REJECTED"
    if (
        review.verdict == "SUPPORTED"
        and review.assertion == "DIRECT"
        and review.polarity == "POSITIVE"
        and _unrepresentable_reason(claim) is None
    ):
        return "ACCEPTED"
    return "DEFERRED"


def review_claims(
    *,
    core: Any,
    cfg: Any,
    payload: Mapping[str, Any],
    extraction: Mapping[str, Any],
    binding: Mapping[str, str],
) -> dict[str, Any]:
    candidates = extraction["triplets"]
    memory = getattr(cfg, "canonical_memory", None) or CanonicalMemoryConfig()
    budgets = verification_budgets(memory)
    if len(candidates) > budgets["max_claims"]:
        raise ClaimVerificationBudgetError("claim verification exceeds the configured claim budget")
    reviews: dict[str, _Review] = {}
    pending = []
    for index, claim in enumerate(candidates):
        identity = _claim_id(index, claim)
        reason = _unrepresentable_reason(claim)
        if reason:
            reviews[identity] = _Review(
                claim_id=identity,
                verdict="UNCLEAR",
                assertion="UNKNOWN",
                polarity="UNKNOWN",
                reason=reason,
            )
        else:
            pending.append({"claim_id": identity, "claim": claim})
    system = prompts.render("verify_claims")
    schema = _ReviewBatch.model_json_schema()
    # Providers append the schema to their system instructions. Include that
    # input and a small message-framing allowance in our complete-input budget.
    fixed_tokens = (
        tokens.count_tokens(system) + tokens.count_tokens(json.dumps(schema, indent=2)) + 256
    )
    batches = []
    for start in range(0, len(pending), budgets["batch_size"]):
        batch = pending[start : start + budgets["batch_size"]]
        user = json.dumps(
            {
                "sources": source_context(payload),
                "preceding_context": payload.get("session_context"),
                "source_policy": payload.get("source_policy", "explicit-ingest"),
                "candidates": batch,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        if fixed_tokens + tokens.count_tokens(user) > budgets["input_tokens"]:
            raise ClaimVerificationBudgetError(
                "claim verification input exceeds the configured token budget; source was not truncated"
            )
        batches.append((batch, user))
    # Check all budgets before spending a model call or persisting a review.
    for batch, user in batches:
        result = core.complete(
            system_prompt=system,
            user_prompt=user,
            output_schema=schema,
            max_tokens=budgets["output_tokens"],
        )
        try:
            parsed = _ReviewBatch.model_validate(result.output)
        except ValidationError as error:
            raise CoreModelError("claim verifier returned malformed output") from error
        expected = {item["claim_id"] for item in batch}
        received = [review.claim_id for review in parsed.reviews]
        if len(received) != len(expected) or set(received) != expected:
            raise CoreModelError(
                "claim verifier must return exactly one decision per supplied claim"
            )
        reviews.update((review.claim_id, review) for review in parsed.reviews)
    decisions = []
    counts = {"accepted": 0, "rejected": 0, "deferred": 0}
    for index, claim in enumerate(candidates):
        review = reviews[_claim_id(index, claim)]
        disposition = _disposition(review, claim)
        counts[disposition.lower()] += 1
        decisions.append({**review.model_dump(), "claim_index": index, "disposition": disposition})
    return {
        "binding": dict(binding),
        "candidates": candidates,
        "reviews": decisions,
        "counts": counts,
    }


def accepted_claims(
    report: Mapping[str, Any],
    *,
    binding: Mapping[str, str],
    extraction: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Revalidate persisted decisions at commit; never trust an accepted-ID list."""
    candidates = extraction["triplets"]
    if report.get("binding") != dict(binding) or payload_fingerprint(
        {"claims": report.get("candidates")}
    ) != payload_fingerprint({"claims": candidates}):
        raise ValueError("claim verification does not match the source, candidates or policy")
    decisions = report.get("reviews")
    if not isinstance(decisions, list) or len(decisions) != len(candidates):
        raise ValueError("claim verification has incomplete decisions")
    accepted = []
    counts = {"accepted": 0, "rejected": 0, "deferred": 0}
    for index, (claim, decision) in enumerate(zip(candidates, decisions, strict=True)):
        if not isinstance(decision, dict):
            raise ValueError("claim verification decision must be an object")
        review = _Review.model_validate(
            {
                key: value
                for key, value in decision.items()
                if key not in {"claim_index", "disposition"}
            }
        )
        disposition = _disposition(review, claim)
        if (
            type(decision.get("claim_index")) is not int
            or decision["claim_index"] != index
            or review.claim_id != _claim_id(index, claim)
            or decision.get("disposition") != disposition
        ):
            raise ValueError("claim verification decision does not match its candidate")
        counts[disposition.lower()] += 1
        if disposition == "ACCEPTED":
            accepted.append(dict(claim))
    if report.get("counts") != counts:
        raise ValueError("claim verification counts do not match its decisions")
    return accepted


def original_source_text(payload: Mapping[str, Any]) -> str:
    pair = payload.get("turn_pair") or payload.get("turn_group") or payload
    parts = []
    for role in ("user", "assistant"):
        if role == "assistant":
            for index, step in enumerate(pair.get("intermediate") or []):
                if isinstance(step, Mapping):
                    parts.append(f"Intermediate {index}: {step.get('content', '')}")
        value = pair.get(role)
        if isinstance(value, Mapping):
            parts.append(f"{role.capitalize()}: {value.get('content', '')}")
    return "\n".join(parts)


def verified_abstract(claims: list[dict[str, Any]]) -> str:
    """Render reviewed values without reusing an unchecked model paraphrase."""
    lines = []
    for claim in claims:
        obj = claim.get("object_value") if "object_value" in claim else claim.get("object")
        line = (
            f"{claim.get('subject')} {claim.get('predicate')} {json.dumps(obj, ensure_ascii=False)}"
        )
        for field in ("valid_from", "valid_until"):
            if claim.get(field) is not None:
                line += f"; {field}={claim[field]}"
        lines.append(line)
    return "\n".join(lines) or "Original source; no supported canonical claims."
