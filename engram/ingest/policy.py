"""Immutable processing plans: a retry must not mix generator policies."""

from __future__ import annotations

import hashlib
from functools import cache
from pathlib import Path
from typing import Any

from engram import prompts
from engram.ingest.claim_verification import verification_policy
from engram.storage.idempotency import payload_fingerprint

PLAN_KEY = "processing-plan-v1"


class ProcessingPolicyChangedError(ValueError):
    pass


@cache
def _code_digest(relative: str) -> str:
    return hashlib.sha256((Path(__file__).resolve().parents[1] / relative).read_bytes()).hexdigest()


def processing_policy(cfg: Any) -> dict[str, Any]:
    model = cfg.core_model.model_dump(exclude={"api_key", "timeout_seconds"})
    endpoint = model.pop("api_base", None)
    model["api_base_hash"] = hashlib.sha256(str(endpoint).encode()).hexdigest()
    descriptor = {
        "version": cfg.canonical_memory.extraction_policy_version,
        "model": model,
        "prompts": {name: prompts.source_digest(name) for name in ("gate_write", "extract")},
        "implementation": {
            name: _code_digest(name)
            for name in (
                "ingest/worker.py",
                "ingest/user_memory.py",
                "ingest/provenance.py",
                "predicate_registry.py",
                "models/providers/openai_compat.py",
            )
        },
        "verification_policy": verification_policy(cfg),
        "evidence_mode": cfg.canonical_memory.evidence_mode,
        "identity_policy": "source-scoped-or-explicit-v2",
    }
    return {"fingerprint": payload_fingerprint(descriptor), "descriptor": descriptor}


def processing_plan(repository: Any, event: Any, tenant_id: str, cfg: Any) -> dict[str, Any]:
    """Pin the first policy durably, then refuse cached work under another policy.

    Legacy compatibility deployments retain their old keys. Strict deployments
    never label an unversioned artifact as produced by the current policy.
    """
    if getattr(getattr(cfg, "canonical_memory", None), "evidence_mode", "legacy") != "enforce":
        return {"keys": {stage: stage + "-v1" for stage in ("gate", "extract", "typed")}}
    event_id = event["event_id"]
    current = processing_policy(cfg)
    existing = repository.get_ingest_artifact(event_id, "EXTRACTION", PLAN_KEY, tenant_id=tenant_id)
    if existing is None:
        plan = {
            **current,
            "source_hash": payload_fingerprint(event["payload"]),
            "keys": {
                stage: stage + ":" + current["fingerprint"]
                for stage in ("gate", "extract", "typed")
            },
        }
        repository.record_ingest_artifact(
            event_id=event_id,
            artifact_type="EXTRACTION",
            artifact_key=PLAN_KEY,
            payload=plan,
            tenant_id=tenant_id,
            extractor="policy",
            extractor_version="v1",
        )
        existing = repository.get_ingest_artifact(
            event_id, "EXTRACTION", PLAN_KEY, tenant_id=tenant_id
        )
    plan = dict(existing.payload)
    if plan.get("source_hash") != payload_fingerprint(event["payload"]):
        raise ProcessingPolicyChangedError("source changed after processing plan was pinned")
    if plan.get("fingerprint") != current["fingerprint"] and event["status"] not in {
        "COMPLETE",
        "GATED_SKIP",
    }:
        raise ProcessingPolicyChangedError(
            "processing policy changed; use the explicit reprocess operation"
        )
    return plan
