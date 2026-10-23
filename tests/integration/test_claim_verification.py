"""Real SQL acceptance/rollback and audit checks with a scripted reviewer."""

from __future__ import annotations

import copy
import json
import uuid
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from temporalio.exceptions import ApplicationError

from engram.config import EngramConfig
from engram.ingest.claim_verification import verification_policy
from engram.ingest.policy import processing_plan
from engram.models.core import CoreModelError
from engram.predicate_registry import normalize_extracted_triplets
from engram.storage.memory_repository import InvalidClaimError, MemoryRepository
from engram.temporal import activities
from engram.temporal.activities import commit_canonical_activity
from engram.tenancy import current_tenant_id
from tests.postgres_support import PostgresTestStore

TENANT = "verification-tenant"


class Reviewer:
    def __init__(self):
        self.calls = []
        self.decisions = {}
        self.fail = False

    def complete(self, **kwargs):
        assert current_tenant_id() == TENANT
        if self.fail:
            raise CoreModelError("verification service unavailable")
        data = json.loads(kwargs["user_prompt"])
        self.calls.append(data)
        reviews = []
        for item in data["candidates"]:
            index = int(item["claim_id"].split("-", 1)[0][1:])
            verdict, assertion, polarity = self.decisions.get(
                index, ("SUPPORTED", "DIRECT", "POSITIVE")
            )
            reviews.append(
                {
                    "claim_id": item["claim_id"],
                    "verdict": verdict,
                    "assertion": assertion,
                    "polarity": polarity,
                    "reason": f"Scripted {verdict.lower()} decision for candidate {index}.",
                }
            )
        return SimpleNamespace(output={"reviews": reviews})


@pytest.fixture
def state(monkeypatch):
    store = PostgresTestStore()
    repository = MemoryRepository(store)
    repository.ensure_schema()
    cfg = EngramConfig.model_validate(
        {
            "api": {"api_key": "test"},
            "event_ledger": {"dsn": store.dsn},
            "canonical_memory": {
                "enabled": True,
                "evidence_mode": "enforce",
                "allow_filesystem_runtime": False,
            },
            "temporal": {"enabled": True},
        }
    )
    state = SimpleNamespace(
        cfg=cfg, control_plane=store, memory_repository=repository, core=Reviewer()
    )
    monkeypatch.setattr(activities, "get_state", lambda: state)
    yield state
    store.close()


def seed(state, source="Alice works at Acme.", claims=None, *, quote=None, typed=True, bindings=None):
    claims = (
        claims
        if claims is not None
        else [{"subject": "Alice", "relation": "works_at", "object": "Acme"}]
    )
    raw = [{**claim, "source_span": {"role": "user", "quote": quote or source}} for claim in claims]
    payload = {
        "force_store": True,
        "turn_pair": {
            "user": {"content": source, "turn_idx": 0},
            "assistant": {"content": "Recorded.", "turn_idx": 1},
        },
        "entity_bindings": bindings or [],
    }
    event, _ = state.control_plane.record_event(
        pair_id=uuid.uuid4().hex,
        session_id="session",
        source="test",
        event_type="INGEST",
        payload=payload,
        tenant_id=TENANT,
    )
    extraction = {
        "resolved_text": "Unchecked generated text: Bob works at Acme.",
        "l0_abstract": "Unchecked generated summary: Bob works at Acme.",
        "triplets": normalize_extracted_triplets(raw) if typed else raw,
    }
    if typed:
        extraction["normalization_stage"] = "typed-v1"
    keys = processing_plan(state.memory_repository, state.control_plane.get_event(event), TENANT, state.cfg)["keys"]
    for key, value in (
        (keys["gate"], {"store": True}),
        (keys["typed"] if typed else keys["extract"], extraction),
    ):
        state.memory_repository.record_ingest_artifact(
            event_id=event,
            artifact_type="EXTRACTION",
            artifact_key=key,
            payload=value,
            content=json.dumps(value),
            media_type="application/json",
            tenant_id=TENANT,
        )
    return event, payload, extraction


def claims_for(state, event):
    return (
        state.control_plane.get_conn()
        .execute(
            "SELECT * FROM memory_claims WHERE tenant_id=? AND source_event_id=?",
            (TENANT, event),
        )
        .fetchall()
    )


@pytest.mark.parametrize("negative", [False, True])
def test_fresh_gate_extraction_and_commit_require_the_additional_review(
    state, monkeypatch, negative
):
    source = "Alice does not work at Acme." if negative else "Alice works at Acme."
    event, _ = state.control_plane.record_event(
        pair_id=uuid.uuid4().hex,
        session_id="fresh-session",
        source="test",
        event_type="INGEST",
        payload={
            "force_store": True,
            "turn_pair": {
                "user": {"content": source},
                "assistant": {"content": "Recorded."},
            },
        },
        tenant_id=TENANT,
    )
    monkeypatch.setattr(
        activities,
        "_call_extract",
        lambda *args, **kwargs: {
            "resolved_text": "Unchecked generated statement.",
            "triplets": [
                {
                    "subject": "Alice",
                    "relation": "works_at",
                    "object": "Acme",
                    "source_span": {"role": "user", "quote": source},
                }
            ],
        },
    )
    if negative:
        state.core.decisions[0] = ("CONTRADICTED", "DIRECT", "POSITIVE")
    assert activities.canonical_gate_activity(event, TENANT)["store"] is True
    assert activities.canonical_extract_activity(event, TENANT) == "READY"
    assert state.core.calls == []
    assert commit_canonical_activity(event, TENANT) == "COMPLETE"
    assert len(state.core.calls) == 1
    assert len(claims_for(state, event)) == (0 if negative else 1)


@pytest.mark.parametrize("typed", [False, True])
def test_supported_claim_commits_original_source_and_idempotent_review(state, typed):
    event, payload, _ = seed(state, typed=typed)
    assert commit_canonical_activity(event, TENANT) == "COMPLETE"
    assert len(claims_for(state, event)) == 1
    assert commit_canonical_activity(event, TENANT) == "COMPLETE"
    assert len(state.core.calls) == 1
    report = state.memory_repository.get_claim_verification(event, tenant_id=TENANT)
    assert report["committed"] is True
    assert report["counts"] == {"accepted": 1, "rejected": 0, "deferred": 0}
    rows = (
        state.control_plane.get_conn()
        .execute(
            "SELECT body, abstract FROM memory_versions WHERE tenant_id=? AND source_event_id=?",
            (TENANT, event),
        )
        .fetchall()
    )
    assert rows and all("Unchecked generated" not in row["body"] + row["abstract"] for row in rows)
    assert any(payload["turn_pair"]["user"]["content"] in row["body"] for row in rows)


@pytest.mark.parametrize(
    "source,quote,claim,decision",
    [
        (
            "Alice does not work at Acme.",
            None,
            {"subject": "Alice", "relation": "works_at", "object": "Acme"},
            ("CONTRADICTED", "DIRECT", "POSITIVE"),
        ),
        (
            "Alice works at Acme.",
            None,
            {"subject": "Bob", "relation": "works_at", "object": "Acme"},
            ("UNCLEAR", "UNKNOWN", "UNKNOWN"),
        ),
        (
            "It is false that Alice works at Acme.",
            "Alice works at Acme",
            {"subject": "Alice", "relation": "works_at", "object": "Acme"},
            ("CONTRADICTED", "DIRECT", "POSITIVE"),
        ),
        (
            "Alice manages Bob.",
            None,
            {"subject": "Bob", "relation": "manages", "object": "Alice"},
            ("UNCLEAR", "UNKNOWN", "UNKNOWN"),
        ),
        (
            "Alice might join Acme.",
            None,
            {"subject": "Alice", "relation": "works_at", "object": "Acme"},
            ("UNCLEAR", "UNCERTAIN", "UNKNOWN"),
        ),
    ],
)
def test_unsupported_claims_never_create_claims_or_entities(state, source, quote, claim, decision):
    event, payload, _ = seed(state, source, [claim], quote=quote)
    state.core.decisions[0] = decision
    assert commit_canonical_activity(event, TENANT) == "COMPLETE"
    assert claims_for(state, event) == []
    entities = (
        state.control_plane.get_conn()
        .execute(
            "SELECT * FROM memory_nodes WHERE tenant_id=? AND memory_type='ENTITY'",
            (TENANT,),
        )
        .fetchall()
    )
    assert entities == []
    assert state.control_plane.get_event(event)["payload"] == payload
    report = state.memory_repository.get_claim_verification(event, tenant_id=TENANT)
    assert report["counts"]["accepted"] == 0
    assert report["reviews"][0]["reason"]


def test_mixed_claims_keep_original_candidate_index_and_only_supported_entities(state):
    event, _, _ = seed(
        state,
        claims=[
            {"subject": "Bob", "relation": "works_at", "object": "Acme"},
            {"subject": "Alice", "relation": "works_at", "object": "Acme"},
        ],
    )
    state.core.decisions[0] = ("UNCLEAR", "UNKNOWN", "UNKNOWN")
    assert commit_canonical_activity(event, TENANT) == "COMPLETE"
    rows = claims_for(state, event)
    assert len(rows) == 1 and rows[0]["source_triplet_index"] == 1
    assert rows[0]["metadata"]["claim_verification_policy"] == verification_policy(state.cfg)
    names = (
        state.control_plane.get_conn()
        .execute(
            "SELECT canonical_name FROM memory_nodes WHERE tenant_id=? AND memory_type='ENTITY'",
            (TENANT,),
        )
        .fetchall()
    )
    assert {row["canonical_name"] for row in names} == {"Alice", "Acme"}


def test_verification_outage_does_not_accept_claims_or_complete_event(state):
    event, _, _ = seed(state)
    state.core.fail = True
    with pytest.raises(CoreModelError, match="unavailable"):
        commit_canonical_activity(event, TENANT)
    assert claims_for(state, event) == []
    assert state.control_plane.get_event(event)["status"] == "RECEIVED"
    assert state.memory_repository.get_claim_verification(event, tenant_id=TENANT) is None
    state.core.fail = False
    assert commit_canonical_activity(event, TENANT) == "COMPLETE"


def test_empty_candidate_set_preserves_source_without_a_model_call(state):
    event, payload, _ = seed(state, claims=[])
    assert commit_canonical_activity(event, TENANT) == "COMPLETE"
    assert state.core.calls == []
    assert state.control_plane.get_event(event)["payload"] == payload
    assert state.memory_repository.get_claim_verification(event, tenant_id=TENANT)["counts"] == {
        "accepted": 0,
        "rejected": 0,
        "deferred": 0,
    }


def test_cached_extraction_with_fabricated_quote_cannot_bypass_span_validation(state):
    event, _, _ = seed(state, quote="Invented quotation")
    with pytest.raises(ApplicationError, match="exact original"):
        commit_canonical_activity(event, TENANT)
    assert state.core.calls == []
    assert claims_for(state, event) == []


def test_malformed_model_review_leaves_no_persisted_acceptance(state):
    event, _, _ = seed(state)
    state.core.complete = lambda **kwargs: SimpleNamespace(output={"reviews": []})
    with pytest.raises(CoreModelError, match="exactly one decision"):
        commit_canonical_activity(event, TENANT)
    assert state.memory_repository.get_claim_verification(event, tenant_id=TENANT) is None
    assert claims_for(state, event) == []


def test_verification_budget_failure_is_explicit_and_nonretryable(state):
    state.cfg.canonical_memory.claim_verification_input_tokens = 1000
    event, payload, _ = seed(state, source="Alice works at Acme. " * 4000)
    with pytest.raises(ApplicationError) as error:
        commit_canonical_activity(event, TENANT)
    assert error.value.type == "ClaimVerificationBudgetExceeded"
    assert error.value.non_retryable
    assert state.core.calls == []
    assert state.control_plane.get_event(event)["payload"] == payload


def test_review_survives_failed_commit_and_retry_without_another_model_call(state, monkeypatch):
    event, _, _ = seed(state)
    original = state.memory_repository.commit_conversational_event

    def fail_commit(**kwargs):
        raise RuntimeError("simulated database outage")

    monkeypatch.setattr(state.memory_repository, "commit_conversational_event", fail_commit)
    with pytest.raises(RuntimeError, match="database outage"):
        commit_canonical_activity(event, TENANT)
    assert (
        state.memory_repository.get_claim_verification(event, tenant_id=TENANT)["committed"]
        is False
    )
    monkeypatch.setattr(state.memory_repository, "commit_conversational_event", original)
    assert commit_canonical_activity(event, TENANT) == "COMPLETE"
    assert len(state.core.calls) == 1


def test_changed_policy_cannot_reuse_pending_review(state, monkeypatch):
    event, _, _ = seed(state)
    original = state.memory_repository.commit_conversational_event
    monkeypatch.setattr(
        state.memory_repository,
        "commit_conversational_event",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("before commit")),
    )
    with pytest.raises(RuntimeError):
        commit_canonical_activity(event, TENANT)
    state.cfg.core_model.model_path = "new-model"
    monkeypatch.setattr(state.memory_repository, "commit_conversational_event", original)
    with pytest.raises(ApplicationError) as error:
        commit_canonical_activity(event, TENANT)
    assert error.value.type == "ProcessingPolicyChanged"
    assert len(state.core.calls) == 1
    report = state.memory_repository.get_claim_verification(event, tenant_id=TENANT)
    assert report["binding"]["policy"] != verification_policy(state.cfg)


def test_repository_guard_rejects_missing_or_mismatched_verification(state):
    event, _, extraction = seed(state)
    with pytest.raises(InvalidClaimError, match="requires"):
        state.memory_repository.commit_conversational_event(
            event_id=event,
            tenant_id=TENANT,
            gate={"store": True},
            extraction=extraction,
            verification_policy=verification_policy(state.cfg),
        )
    with pytest.raises(InvalidClaimError, match="different input"):
        state.memory_repository.commit_conversational_event(
            event_id=event,
            tenant_id=TENANT,
            gate={"store": True},
            extraction=extraction,
            verification_policy=verification_policy(state.cfg),
            verification_artifact_key="wrong-key",
        )
    assert claims_for(state, event) == []


def test_source_change_between_review_and_commit_is_rejected_transactionally(state):
    event, payload, _ = seed(state)
    complete = state.core.complete

    def review_then_change_source(**kwargs):
        result = complete(**kwargs)
        changed = copy.deepcopy(payload)
        changed["turn_pair"]["user"]["content"] = "Alice does not work at Acme."
        with state.control_plane.transaction() as conn:
            conn.execute(
                "UPDATE events SET payload=? WHERE tenant_id=? AND event_id=?",
                (json.dumps(changed), TENANT, event),
            )
        return result

    state.core.complete = review_then_change_source
    with pytest.raises(ApplicationError, match=r"different input|pinned source policy"):
        commit_canonical_activity(event, TENANT)
    assert claims_for(state, event) == []
    assert state.control_plane.get_conn().execute("SELECT * FROM memory_nodes").fetchall() == []


def test_verification_endpoint_is_tenant_scoped_and_shows_rejection(state, monkeypatch):
    from engram.api.routes import events

    event, _, _ = seed(state)
    state.core.decisions[0] = ("CONTRADICTED", "DIRECT", "POSITIVE")
    commit_canonical_activity(event, TENANT)
    monkeypatch.setattr(events, "get_state", lambda: state)
    monkeypatch.setattr(events, "current_tenant_id", lambda: TENANT)
    app = FastAPI()
    app.add_api_route("/events/{event_id}/verification", events.get_event_verification)
    with TestClient(app) as client:
        response = client.get(f"/events/{event}/verification")
        assert response.status_code == 200
        assert response.json()["committed"] is True
        assert response.json()["counts"]["rejected"] == 1
        assert response.json()["source_matches"] is True
        assert response.json()["sources"]["user"]["content"] == "Alice works at Acme."
        assert state.memory_repository.get_claim_verification(event, tenant_id="foreign") is None
        monkeypatch.setattr(events, "current_tenant_id", lambda: "foreign")
        assert client.get(f"/events/{event}/verification").status_code == 404
