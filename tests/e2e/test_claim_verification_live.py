"""Opt-in semantic-quality smoke check against the configured real Core model."""

import os

import pytest

from engram.config import get_config
from engram.ingest.claim_verification import (
    accepted_claims,
    review_claims,
    verification_binding,
    verification_policy,
)
from engram.ingest.provenance import source_context, validate_extraction
from engram.models.providers import build_providers
from engram.predicate_registry import normalize_extracted_triplets


@pytest.mark.e2e
@pytest.mark.skipif(
    os.environ.get("ENGRAM_TEST_CLAIM_VERIFIER_LIVE") != "1",
    reason="requires explicit live-model opt-in and configured credentials",
)
def test_live_verifier_preserves_subject_direction_negation_and_uncertainty():
    cfg = get_config()
    core, _ = build_providers(cfg.core_model, cfg.frontier_llm)
    statements = [
        "Alice does not work at Acme.",
        "Beta employs Bob.",
        "Carol manages Dan.",
        "Eve might join Delta.",
        "It is false that Frank works at Gamma.",
    ]
    payload = {
        "turn_pair": {"user": {"content": " ".join(statements)}, "assistant": {"content": ""}}
    }
    raw = [
        {"subject": "Alice", "relation": "works_at", "object": "Acme"},
        {"subject": "Bob", "relation": "works_at", "object": "Beta"},
        {"subject": "Dan", "relation": "manages", "object": "Carol"},
        {"subject": "Eve", "relation": "works_at", "object": "Delta"},
        {"subject": "Frank", "relation": "works_at", "object": "Gamma"},
    ]
    for candidate, statement in zip(raw, statements, strict=True):
        candidate["source_span"] = {"role": "user", "quote": statement}
    raw[-1]["source_span"]["quote"] = "Frank works at Gamma"
    extraction = validate_extraction(
        {"triplets": normalize_extracted_triplets(raw), "normalization_stage": "typed-v1"},
        source_context(payload),
    )
    binding = verification_binding(
        tenant_id="live-test",
        event_id="live-test",
        payload=payload,
        extraction=extraction,
        policy=verification_policy(cfg),
    )
    report = review_claims(
        core=core, cfg=cfg, payload=payload, extraction=extraction, binding=binding
    )
    accepted = accepted_claims(report, binding=binding, extraction=extraction)
    assert [claim["subject"] for claim in accepted] == ["Bob"], report["reviews"]
