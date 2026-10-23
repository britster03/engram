"""Verification contract tests; scripted verdicts do not measure model quality."""

import copy
import json
from types import SimpleNamespace

import pytest

from engram.config import CanonicalMemoryConfig, CoreModelConfig
from engram.ingest.claim_verification import (
    ClaimVerificationBudgetError,
    accepted_claims,
    review_claims,
    verification_artifact_key,
    verification_binding,
    verification_policy,
)
from engram.models.core import CoreModelError


class ScriptedReviewer:
    def __init__(self, verdict="SUPPORTED", assertion="DIRECT", polarity="POSITIVE"):
        self.verdict, self.assertion, self.polarity = verdict, assertion, polarity
        self.calls = []

    def complete(self, **kwargs):
        data = json.loads(kwargs["user_prompt"])
        self.calls.append(data)
        return SimpleNamespace(
            output={
                "reviews": [
                    {
                        "claim_id": item["claim_id"],
                        "verdict": self.verdict,
                        "assertion": self.assertion,
                        "polarity": self.polarity,
                        "reason": "Scripted source-based review.",
                    }
                    for item in data["candidates"]
                ]
            }
        )


def inputs(text="Alice works at Acme.", **claim_changes):
    cfg = SimpleNamespace(canonical_memory=CanonicalMemoryConfig(), core_model=CoreModelConfig())
    payload = {"turn_pair": {"user": {"content": text}, "assistant": {"content": "Recorded."}}}
    extraction = {
        "triplets": [
            {
                "subject": "Alice",
                "predicate": "WORKS_AT",
                "object": "Acme",
                "source_span": {"role": "user", "quote": text},
                **claim_changes,
            }
        ]
    }
    binding = verification_binding(
        tenant_id="tenant",
        event_id="event",
        payload=payload,
        extraction=extraction,
        policy=verification_policy(cfg),
    )
    return cfg, payload, extraction, binding


def run_review(model, data):
    cfg, payload, extraction, binding = data
    return review_claims(
        core=model, cfg=cfg, payload=payload, extraction=extraction, binding=binding
    )


@pytest.mark.parametrize(
    "text,verdict,assertion,polarity,expected",
    [
        ("Alice works at Acme.", "SUPPORTED", "DIRECT", "POSITIVE", "ACCEPTED"),
        ("Acme employs Alice.", "SUPPORTED", "DIRECT", "POSITIVE", "ACCEPTED"),
        ("Alice does not work at Acme.", "CONTRADICTED", "DIRECT", "POSITIVE", "REJECTED"),
        ("Alice might join Acme.", "UNCLEAR", "UNCERTAIN", "UNKNOWN", "DEFERRED"),
        ("Does Alice work at Acme?", "UNCLEAR", "QUESTION", "UNKNOWN", "DEFERRED"),
        ("Alice says Bob works at Acme.", "SUPPORTED", "REPORTED", "POSITIVE", "DEFERRED"),
        ("If Alice joined Acme...", "SUPPORTED", "HYPOTHETICAL", "POSITIVE", "DEFERRED"),
        ("Alice does not work at Acme.", "SUPPORTED", "DIRECT", "NEGATIVE", "DEFERRED"),
        ("Alice references Acme.", "SUPPORTED", "INFERRED", "POSITIVE", "DEFERRED"),
    ],
)
def test_python_enforces_verdict_assertion_and_polarity(
    text, verdict, assertion, polarity, expected
):
    data = inputs(text)
    report = run_review(ScriptedReviewer(verdict, assertion, polarity), data)
    accepted = accepted_claims(report, binding=data[3], extraction=data[2])
    assert report["reviews"][0]["disposition"] == expected
    assert bool(accepted) == (expected == "ACCEPTED")
    assert sum(report["counts"].values()) == 1


def test_reviewer_receives_full_source_not_only_cherry_picked_quote():
    data = inputs("It is false that Alice works at Acme.")
    data[2]["triplets"][0]["source_span"]["quote"] = "Alice works at Acme"
    model = ScriptedReviewer("CONTRADICTED")
    run_review(model, data)
    assert model.calls[0]["sources"]["user"]["content"] == "It is false that Alice works at Acme."
    assert model.calls[0]["candidates"][0]["claim"]["source_span"]["quote"] == "Alice works at Acme"


@pytest.mark.parametrize(
    "changes",
    [
        {"negated": True},
        {"polarity": "NEGATIVE"},
        {"modality": "possible"},
        {"subject_id": "model-selected-id"},
        {"object_entity_id": "another-id"},
        {"subject": {"name": "Alice", "id": "another-id"}},
    ],
)
def test_unrepresentable_claims_are_deferred_without_a_model_call(changes):
    data = inputs(**changes)
    model = ScriptedReviewer()
    report = run_review(model, data)
    assert report["counts"] == {"accepted": 0, "rejected": 0, "deferred": 1}
    assert model.calls == []


def test_literal_boolean_false_is_not_a_negative_relationship():
    data = inputs("Alice's access is disabled.", predicate="HAS_ACCESS", object_value=False)
    report = run_review(ScriptedReviewer(), data)
    assert accepted_claims(report, binding=data[3], extraction=data[2])[0]["object_value"] is False


@pytest.mark.parametrize(
    "failure", ["missing", "duplicate", "foreign", "unknown_verdict", "rewritten"]
)
def test_malformed_or_incomplete_model_output_cannot_accept_claims(failure):
    class BrokenReviewer(ScriptedReviewer):
        def complete(self, **kwargs):
            result = super().complete(**kwargs)
            reviews = result.output["reviews"]
            if failure == "missing":
                reviews.clear()
            elif failure == "duplicate":
                reviews.append(dict(reviews[0]))
            elif failure == "foreign":
                reviews[0]["claim_id"] = "unknown"
            elif failure == "unknown_verdict":
                reviews[0]["verdict"] = "probably"
            else:
                reviews[0]["subject"] = "Bob"
            return result

    with pytest.raises(CoreModelError):
        run_review(BrokenReviewer(), inputs())


def test_verification_batches_preserve_complete_claim_identity():
    cfg, payload, extraction, _ = inputs()
    cfg.canonical_memory.claim_verification_batch_size = 2
    extraction["triplets"] *= 5
    binding = verification_binding(
        tenant_id="tenant",
        event_id="event",
        payload=payload,
        extraction=extraction,
        policy=verification_policy(cfg),
    )
    model = ScriptedReviewer()
    report = run_review(model, (cfg, payload, extraction, binding))
    assert [len(call["candidates"]) for call in model.calls] == [2, 2, 1]
    assert len(accepted_claims(report, binding=binding, extraction=extraction)) == 5


def test_over_budget_source_is_not_truncated_or_sent_to_model():
    data = inputs("Alice works at Acme. " * 4000)
    data[0].canonical_memory.claim_verification_input_tokens = 1000
    model = ScriptedReviewer()
    with pytest.raises(ClaimVerificationBudgetError, match="not truncated"):
        run_review(model, data)
    assert model.calls == []


def test_over_budget_claim_count_is_explicit():
    data = inputs()
    data[0].canonical_memory.claim_verification_max_claims = 1
    data[2]["triplets"] *= 2
    with pytest.raises(ClaimVerificationBudgetError, match="claim budget"):
        run_review(ScriptedReviewer(), data)


@pytest.mark.parametrize(
    "tamper", ["source", "candidate", "policy", "tenant", "decision", "counts"]
)
def test_persisted_review_cannot_be_reused_for_different_input(tamper):
    data = inputs()
    report = run_review(ScriptedReviewer(), data)
    binding = dict(data[3])
    extraction = copy.deepcopy(data[2])
    if tamper == "source":
        binding["source_hash"] = "changed"
    elif tamper == "candidate":
        extraction["triplets"][0]["subject"] = "Bob"
    elif tamper in {"policy", "tenant"}:
        binding["policy" if tamper == "policy" else "tenant_id"] = "changed"
    elif tamper == "decision":
        report["reviews"][0]["disposition"] = "DEFERRED"
    else:
        report["counts"]["accepted"] = 100
    with pytest.raises(ValueError):
        accepted_claims(report, binding=binding, extraction=extraction)


def test_model_prompt_and_budget_changes_invalidate_review_identity(monkeypatch):
    cfg, payload, extraction, binding = inputs()
    original = verification_artifact_key(binding)
    cfg.core_model.model_path = "different-model"
    changed = verification_policy(cfg)
    assert changed != binding["policy"]
    cfg.canonical_memory.claim_verification_batch_size = 1
    assert verification_policy(cfg) != changed
    from engram.ingest import claim_verification

    monkeypatch.setattr(claim_verification.prompts, "source_digest", lambda _: "new-prompt")
    final = verification_binding(
        tenant_id="tenant",
        event_id="event",
        payload=payload,
        extraction=extraction,
        policy=verification_policy(cfg),
    )
    assert verification_artifact_key(final) != original
