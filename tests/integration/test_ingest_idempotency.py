"""Explicit ingest acceptance against the real PostgreSQL control plane."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests.postgres_support import PostgresTestStore


@pytest.fixture(params=[False, True], ids=["legacy", "canonical"])
def client(monkeypatch, request):
    import engram.api.app as api
    from engram.admin import routes as admin

    store = PostgresTestStore()
    state = SimpleNamespace(
        control_plane=store,
        cfg=SimpleNamespace(
            consolidation=SimpleNamespace(max_backlog=1000),
            canonical_memory=SimpleNamespace(enabled=request.param),
        ),
    )
    if request.param:
        from engram.storage.memory_repository import MemoryRepository

        state.memory_repository = MemoryRepository(store)
        state.memory_repository.ensure_schema()
    monkeypatch.setattr(api, "get_state", lambda: state)
    monkeypatch.setattr(admin, "verify_ui_auth", lambda request: None)
    monkeypatch.setattr(
        admin, "_bind_default_tenant", lambda: (state, SimpleNamespace(tenant_id="_default"))
    )
    app = FastAPI()
    app.add_api_route("/ingest", api.ingest, methods=["POST"])
    app.add_api_route("/batch", api.ingest_batch, methods=["POST"])
    app.add_api_route("/admin/ingest", admin.admin_ingest, methods=["POST"])
    with TestClient(app) as c:
        yield c, store
    store.close()


def _request(user="Alice works at Acme.", **kwargs):
    return {
        "turn_pair": {"user": {"content": user}, "assistant": {"content": "Recorded."}},
        **kwargs,
    }


@pytest.mark.parametrize("path", ["/ingest", "/admin/ingest"])
@pytest.mark.parametrize("session_id", [None, "same-session"])
def test_unkeyed_submissions_preserve_both_inputs(client, path, session_id):
    c, store = client
    first = c.post(path, json=_request(session_id=session_id))
    second = c.post(path, json=_request("Bob works at Beta.", session_id=session_id))
    assert first.status_code == second.status_code == 202
    assert first.json()["event_id"] != second.json()["event_id"]
    event = store.get_event(second.json()["event_id"])
    assert event["payload"]["turn_pair"]["user"]["content"] == "Bob works at Beta."
    assert len(store.get_conn().execute("SELECT * FROM workflow_dispatches").fetchall()) == 2


def test_identical_unkeyed_submissions_are_separate_requests(client):
    c, _ = client
    first = c.post("/ingest", json=_request()).json()
    second = c.post("/ingest", json=_request()).json()
    assert first["event_id"] != second["event_id"]


@pytest.mark.parametrize("path", ["/ingest", "/admin/ingest"])
def test_keyed_retry_replays_event_and_current_status(client, path):
    c, store = client
    body = _request(idempotency_key="request-1")
    first = c.post(path, json=body)
    store.set_event_status(first.json()["event_id"], "COMPLETE")
    # Explicit defaults and dictionary ordering do not change request meaning.
    replay = c.post(path, json={**body, "force_store": False, "source": "client"})
    assert replay.status_code == 202
    assert replay.json()["event_id"] == first.json()["event_id"]
    assert replay.json()["status"] == "COMPLETE"
    assert len(store.get_conn().execute("SELECT * FROM events").fetchall()) == 1
    assert len(store.get_conn().execute("SELECT * FROM workflow_dispatches").fetchall()) == 1


@pytest.mark.parametrize(
    "change",
    [
        {"turn_pair": {"user": {"content": "Bob"}, "assistant": {"content": "Recorded."}}},
        {"session_id": "another-session"},
        {"session_context": "Different extraction context"},
        {"force_store": True},
        {"source": "another-source"},
    ],
)
@pytest.mark.parametrize("path", ["/ingest", "/admin/ingest"])
def test_key_reuse_with_changed_input_is_rejected(client, change, path):
    c, store = client
    body = _request(idempotency_key="request-1")
    first = c.post(path, json=body)
    conflict = c.post(path, json={**body, **change})
    assert conflict.status_code == 409
    assert "different input" in conflict.json()["detail"]
    assert store.get_event(first.json()["event_id"])["payload"]["force_store"] is False
    assert len(store.get_conn().execute("SELECT * FROM events").fetchall()) == 1


def test_batch_keeps_unkeyed_rows_distinct_and_replays_keyed_rows(client):
    c, store = client
    body = [_request(), _request("Bob works at Beta."), _request(idempotency_key="batch-1")]
    first = c.post("/batch", json=body)
    replay = c.post("/batch", json=body)
    assert first.status_code == replay.status_code == 202
    first_ids = [row["event_id"] for row in first.json()]
    replay_ids = [row["event_id"] for row in replay.json()]
    assert len(set(first_ids)) == 3
    assert first_ids[:2] != replay_ids[:2]
    assert first_ids[2] == replay_ids[2]
    assert len(store.get_conn().execute("SELECT * FROM events").fetchall()) == 5


def test_batch_conflict_rolls_back_earlier_rows_and_dispatches(client):
    c, store = client
    first = c.post("/ingest", json=_request(idempotency_key="existing"))
    conflict = c.post(
        "/batch",
        json=[_request(idempotency_key="new"), _request("Changed", idempotency_key="existing")],
    )
    assert conflict.status_code == 409
    events = store.get_conn().execute("SELECT event_id FROM events").fetchall()
    assert [row["event_id"] for row in events] == [first.json()["event_id"]]
    assert len(store.get_conn().execute("SELECT * FROM workflow_dispatches").fetchall()) == 1
