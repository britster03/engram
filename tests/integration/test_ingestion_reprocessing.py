"""Source generations use real PostgreSQL transactions and scripted model output."""

import pytest
from temporalio.exceptions import ApplicationError

from engram.ingest.policy import processing_plan, processing_policy
from engram.ingest.reprocessing import request_reprocessing
from engram.storage.idempotency import IdempotencyConflictError
from engram.temporal import activities
from tests.integration.test_claim_verification import TENANT, seed
from tests.integration.test_claim_verification import state as verification_state


@pytest.fixture
def state(monkeypatch):
    yield from verification_state.__wrapped__(monkeypatch)


def node_ids(state, name):
    return [
        row["id"]
        for row in state.control_plane.get_conn()
        .execute(
            "SELECT id FROM memory_nodes WHERE tenant_id = ? AND canonical_name = ? AND memory_type = 'ENTITY'",
            (TENANT, name),
        )
        .fetchall()
    ]


def current_claims(state):
    return (
        state.control_plane.get_conn()
        .execute("SELECT * FROM memory_claims WHERE tenant_id = ? AND status = 'ACTIVE'", (TENANT,))
        .fetchall()
    )


def reprocess(state, event, *, key="new-generation", reason="Policy update"):
    return request_reprocessing(state, event, tenant_id=TENANT, request_key=key, reason=reason)


def run_child(state, child, monkeypatch, *, claims=None, store=True):
    event_id = child["event_id"]
    extraction = {
        "triplets": claims
        if claims is not None
        else [
            {
                "subject": "Alice",
                "relation": "works_at",
                "object": "Acme",
                "source_span": {"role": "user", "quote": "Alice works at Acme."},
            }
        ]
    }
    monkeypatch.setattr(activities, "_call_extract", lambda *args, **kwargs: extraction)
    if not store:
        plan = processing_plan(state.memory_repository, child, TENANT, state.cfg)
        state.memory_repository.record_ingest_artifact(
            event_id=event_id,
            artifact_type="EXTRACTION",
            artifact_key=plan["keys"]["gate"],
            payload={"store": False},
            tenant_id=TENANT,
        )
    activities.canonical_gate_activity(event_id, TENANT)
    activities.canonical_extract_activity(event_id, TENANT)
    return activities.commit_canonical_activity(event_id, TENANT)


def test_same_name_in_same_session_does_not_join_independent_sources(state):
    for company in ("Acme", "Beta"):
        event, _, _ = seed(
            state,
            source=f"John Smith works at {company}.",
            claims=[{"subject": "John Smith", "relation": "works_at", "object": company}],
        )
        activities.commit_canonical_activity(event, TENANT)
    assert len(node_ids(state, "John Smith")) == 2
    assert len({claim["subject_id"] for claim in current_claims(state)}) == 2


def test_attributes_distinguish_homonyms_and_survive_object_normalization(state):
    source = "John Smith (email a@example.test) reports to John Smith (email b@example.test)."
    event, _, _ = seed(
        state,
        source=source,
        claims=[
            {
                "subject": {"name": "John Smith", "attributes": {"email": "a@example.test"}},
                "relation": "reports_to",
                "object": {"name": "John Smith", "attributes": {"email": "b@example.test"}},
            }
        ],
    )
    activities.commit_canonical_activity(event, TENANT)
    claims = current_claims(state)
    assert len(node_ids(state, "John Smith")) == 2
    assert claims[0]["subject_id"] != claims[0]["object_entity_id"]
    nodes = [
        state.memory_repository.get_memory(identity, tenant_id=TENANT)
        for identity in node_ids(state, "John Smith")
    ]
    assert {n.metadata["distinguishing_attributes"]["email"] for n in nodes} == {
        "a@example.test",
        "b@example.test",
    }


def test_caller_binding_is_required_for_cross_source_identity(state):
    first, _, _ = seed(
        state,
        bindings=[
            {"name": "Alice", "identity_key": "employee-1", "attributes": {"employee_id": "1"}}
        ],
    )
    activities.commit_canonical_activity(first, TENANT)
    alice = node_ids(state, "Alice")[0]
    second, _, _ = seed(
        state,
        source="Alice is an engineer.",
        claims=[{"subject": "Alice", "relation": "role", "object": "Engineer"}],
        bindings=[{"name": "Alice", "entity_id": str(alice)}],
    )
    activities.commit_canonical_activity(second, TENANT)
    assert node_ids(state, "Alice") == [alice]
    assert len(current_claims(state)) == 2


def test_conflicting_explicit_identity_attributes_fail_atomically(state):
    for index in (1, 2):
        event, _, _ = seed(
            state,
            bindings=[
                {
                    "name": "Alice",
                    "identity_key": "employee",
                    "attributes": {"employee_id": str(index)},
                }
            ],
        )
        if index == 1:
            activities.commit_canonical_activity(event, TENANT)
        else:
            with pytest.raises(ApplicationError, match="conflicts"):
                activities.commit_canonical_activity(event, TENANT)
    assert len(current_claims(state)) == 1


def test_policy_pins_model_prompts_typing_and_excludes_credentials(state):
    original = processing_policy(state.cfg)
    state.cfg.core_model.api_key = "changed-secret"
    assert processing_policy(state.cfg) == original
    state.cfg.core_model.max_tokens += 1
    assert processing_policy(state.cfg) != original
    assert "api_key" not in original["descriptor"]["model"]


def test_completed_unversioned_mutation_replays_after_upgrade_without_new_model_call(state):
    original, _, extraction = seed(state)
    state.memory_repository.commit_conversational_event(
        event_id=original, tenant_id=TENANT, gate={"store": True}, extraction=extraction,
    )
    with state.control_plane.transaction() as conn:
        conn.execute("DELETE FROM ingest_artifacts WHERE tenant_id = ? AND event_id = ?", (TENANT, original))
    state.cfg.canonical_memory.extraction_policy_version = "post-upgrade"
    state.core.fail = True
    assert activities.commit_canonical_activity(original, TENANT) == "COMPLETE"
    assert state.core.calls == []


@pytest.mark.parametrize("accepted", [True, False])
def test_reprocessing_atomically_replaces_results_and_retains_source_history(
    state, monkeypatch, accepted
):
    original, source, _ = seed(state)
    activities.commit_canonical_activity(original, TENANT)
    old_claim = current_claims(state)[0]
    state.cfg.canonical_memory.extraction_policy_version = "updated-policy"
    child = reprocess(state, original)
    assert len(current_claims(state)) == 1  # old generation stays live while work is pending
    assert reprocess(state, original)["event_id"] == child["event_id"]
    with pytest.raises(IdempotencyConflictError):
        reprocess(state, original, reason="Different intent")
    assert run_child(state, child, monkeypatch, claims=None if accepted else []) == "COMPLETE"
    active = current_claims(state)
    assert len(active) == int(accepted)
    assert (
        state.memory_repository.get_claim(old_claim["id"], tenant_id=TENANT).status == "RETRACTED"
    )
    if accepted:
        assert active[0]["id"] != old_claim["id"]
        assert active[0]["asserted_at"] == old_claim["asserted_at"]
    assert state.control_plane.get_event(original)["payload"] == source
    assert state.control_plane.get_event(original)["status"] == "COMPLETE"
    states = (
        state.control_plane.get_conn()
        .execute("SELECT event_id, state FROM ingest_reprocessings")
        .fetchall()
    )
    assert {r["event_id"]: r["state"] for r in states} == {
        original: "SUPERSEDED",
        child["event_id"]: "ACTIVE",
    }
    assert (
        state.control_plane.get_conn()
        .execute("SELECT count(*) AS n FROM memory_evidence WHERE source_event_id = ?", (original,))
        .fetchone()["n"]
        > 0
    )


def test_reprocessing_gate_skip_retires_prior_claims(state, monkeypatch):
    original, _, _ = seed(state)
    activities.commit_canonical_activity(original, TENANT)
    child = reprocess(state, original)
    assert run_child(state, child, monkeypatch, store=False) == "GATED_SKIP"
    assert current_claims(state) == []


def test_reprocessing_failure_keeps_previous_generation_and_blocks_stale_retry(state, monkeypatch):
    original, _, _ = seed(state)
    activities.commit_canonical_activity(original, TENANT)
    first = reprocess(state, original, key="attempt-one")
    state.core.fail = True
    with pytest.raises(Exception, match="unavailable"):
        run_child(state, first, monkeypatch)
    assert len(current_claims(state)) == 1
    activities.mark_canonical_ingest_failed_activity(
        first["event_id"], TENANT, "verification unavailable"
    )
    state.core.fail = False
    second = reprocess(state, original, key="attempt-two")
    assert state.control_plane.retry_event(first["event_id"], tenant_id=TENANT) is None
    assert run_child(state, second, monkeypatch) == "COMPLETE"
    assert len(current_claims(state)) == 1


def test_independently_supported_shared_claim_survives_one_source_reprocessing(state, monkeypatch):
    bindings = [
        {"name": "Alice", "identity_key": "person-1"},
        {"name": "Acme", "identity_key": "company-1"},
    ]
    original, _, _ = seed(state, bindings=bindings)
    other, _, _ = seed(state, bindings=bindings)
    for event in (original, other):
        activities.commit_canonical_activity(event, TENANT)
    shared_id = current_claims(state)[0]["id"]
    child = reprocess(state, original)
    run_child(state, child, monkeypatch, claims=[])
    assert [c["id"] for c in current_claims(state)] == [shared_id]


def test_failed_sql_promotion_rolls_back_retirement_and_reuses_verification(state, monkeypatch):
    original, _, _ = seed(state)
    activities.commit_canonical_activity(original, TENANT)
    child = reprocess(state, original)
    complete = state.memory_repository._complete_mutation_in_tx
    monkeypatch.setattr(
        state.memory_repository,
        "_complete_mutation_in_tx",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("commit interrupted")),
    )
    with pytest.raises(RuntimeError, match="interrupted"):
        run_child(state, child, monkeypatch)
    assert len(current_claims(state)) == 1
    states = (
        state.control_plane.get_conn()
        .execute("SELECT event_id, state FROM ingest_reprocessings")
        .fetchall()
    )
    assert {r["event_id"]: r["state"] for r in states} == {
        original: "ACTIVE",
        child["event_id"]: "PENDING",
    }
    calls = len(state.core.calls)
    monkeypatch.setattr(state.memory_repository, "_complete_mutation_in_tx", complete)
    assert activities.commit_canonical_activity(child["event_id"], TENANT) == "COMPLETE"
    assert len(state.core.calls) == calls


def test_reprocessing_is_tenant_scoped_and_prevents_overlapping_requests(state):
    original, _, _ = seed(state)
    activities.commit_canonical_activity(original, TENANT)
    child = reprocess(state, original)
    with pytest.raises(ValueError, match="still running"):
        reprocess(state, original, key="overlap")
    with pytest.raises(LookupError):
        request_reprocessing(
            state, original, tenant_id="other", request_key="foreign", reason="test"
        )
    assert child["event_id"] != original


def test_concurrent_reprocessing_replays_one_request(state):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    original, _, _ = seed(state)
    activities.commit_canonical_activity(original, TENANT)
    barrier = Barrier(4)

    def submit(_):
        barrier.wait(timeout=10)
        return reprocess(state, original)["event_id"]

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(submit, range(4)))
    assert len(set(results)) == 1
    assert (
        state.control_plane.get_conn()
        .execute(
            "SELECT count(*) AS n FROM ingest_reprocessings WHERE state = 'PENDING'",
        )
        .fetchone()["n"]
        == 1
    )


def test_reprocess_route_authentication_conflicts_and_tenant_isolation(state, monkeypatch):
    from types import SimpleNamespace

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from engram.api import auth
    from engram.api.routes import events

    original, _, _ = seed(state)
    activities.commit_canonical_activity(original, TENANT)
    monkeypatch.setattr(events, "get_state", lambda: state)
    monkeypatch.setattr(
        auth,
        "_lookup_tenant",
        lambda token: SimpleNamespace(tenant_id=TENANT if token == "owner" else "foreign"),
    )
    app = FastAPI()
    app.include_router(events.router)
    with TestClient(app) as client:
        url = f"/api/v1/events/{original}/reprocess"
        body = {"idempotency_key": "request-key", "reason": "Policy update"}
        assert client.post(url, json=body).status_code == 401
        assert (
            client.post(url, json=body, headers={"Authorization": "Bearer foreign"}).status_code
            == 404
        )
        first = client.post(url, json=body, headers={"Authorization": "Bearer owner"})
        assert first.status_code == 202
        assert first.json()["event_id"] != original
        assert (
            client.post(url, json=body, headers={"Authorization": "Bearer owner"}).json()
            == first.json()
        )
        assert (
            client.post(
                url, json={**body, "reason": "Changed"}, headers={"Authorization": "Bearer owner"}
            ).status_code
            == 409
        )


def test_attributes_added_to_known_entity_are_persisted_and_checked_later(state):
    for index, attributes in enumerate(({}, {"employee_id": "1"}, {"employee_id": "2"})):
        event, _, _ = seed(
            state,
            bindings=[{"name": "Alice", "identity_key": "stable-person", "attributes": attributes}],
        )
        if index < 2:
            activities.commit_canonical_activity(event, TENANT)
        else:
            with pytest.raises(ApplicationError, match="conflicts"):
                activities.commit_canonical_activity(event, TENANT)
    alice = state.memory_repository.get_memory(node_ids(state, "Alice")[0], tenant_id=TENANT)
    assert alice.metadata["distinguishing_attributes"] == {"employee_id": "1"}
