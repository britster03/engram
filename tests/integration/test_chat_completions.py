"""Integration tests for the chat completions endpoint.

Uses a minimal FastAPI app with the chat router mounted and the global
AppState patched to point at stub providers so no real API keys are needed.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from engram.api.routes import chat as chat_route
from engram.api.routes import sessions as sessions_route
from engram.audit import AuditLog
from engram.cache import EmbeddingCache, MemoryCache, OverviewCache
from engram.config import EngramConfig
from engram.deps import AppState, get_state, reset_state
from engram.storage.filesystem import FilesystemStore
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from engram.storage.redis_cache import SessionCache
from engram.tenancy import TenantRegistry
from tests.integration.providers import (
    DeterministicCoreProvider,
    DeterministicEmbeddingService,
    DeterministicFrontierProvider,
)
from tests.postgres_support import PostgresTestStore


def _build_state(cfg: EngramConfig) -> AppState:
    store = PostgresTestStore()
    fs = FilesystemStore(cfg.filesystem.data_dir, create_dirs=cfg.filesystem.create_dirs)
    fake_neo = InMemoryKnowledgeGraph()
    session_cache = SessionCache(
        cfg.session_cache, default_ttl_seconds=cfg.session.timeout_minutes * 60
    )
    core = DeterministicCoreProvider()
    frontier = DeterministicFrontierProvider()
    embed = DeterministicEmbeddingService()  # type: ignore[arg-type]
    tenant_registry = TenantRegistry(store)
    tenant_registry.ensure_default(legacy_api_key=cfg.api.api_key)
    audit = AuditLog(store)
    embed_cache = EmbeddingCache(MemoryCache())
    overview_cache = OverviewCache(MemoryCache())
    return AppState(
        cfg=cfg,
        control_plane=store,
        fs=fs,
        neo4j=fake_neo,
        session_cache=session_cache,
        core=core,
        frontier=frontier,
        embed=embed,
        tenant_registry=tenant_registry,
        audit=audit,
        embedding_cache=embed_cache,
        overview_cache=overview_cache,
    )


@pytest.fixture
def cfg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> EngramConfig:
    monkeypatch.setenv("ENGRAM_API_KEY", "test-key")
    monkeypatch.setenv("NEO4J_ADMIN_PASSWORD", "x")
    cfg = EngramConfig.model_validate(
        {
            "api": {"api_key": "test-key"},
            "core_model": {"provider": "openai_responses", "api_key": "x"},
            "frontier_llm": {"provider": "openai_responses", "api_key": "x"},
            "filesystem": {"data_dir": str(tmp_path / "mem")},
            "event_ledger": {"dsn": "postgresql://test:test/test"},
            "session_cache": {"backend": "memory"},
            "knowledge_graph": {"writer_password": "x", "reader_password": "x"},
            "retrieval": {"l0_skip": True},
        }
    )
    return cfg


@pytest.fixture
def client(cfg: EngramConfig, monkeypatch: pytest.MonkeyPatch):
    app = FastAPI()
    app.include_router(chat_route.router)
    app.include_router(sessions_route.router)

    state = _build_state(cfg)
    import engram.deps as deps_mod

    monkeypatch.setattr(deps_mod, "_state", state)
    import engram.config as config_mod

    monkeypatch.setattr(config_mod, "_cached", cfg)

    with TestClient(app) as c:
        c.headers["Authorization"] = "Bearer test-key"
        yield c

    reset_state()


def _chat_payload(
    messages: list[dict[str, str]], stream: bool = False, session_id: str | None = None
) -> dict[str, Any]:
    body: dict[str, Any] = {"messages": messages, "stream": stream}
    if session_id is not None:
        body["session_id"] = session_id
    return body


def test_chat_completions_buffered(client: TestClient, cfg: EngramConfig):
    payload = _chat_payload([{"role": "user", "content": "What is my name?"}])
    r = client.post("/api/v1/chat/completions", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert "answer" in data
    assert data["session_id"].startswith("sess-")
    assert "retrieval_metadata" in data
    assert data["finish_reason"] == "stop"


def test_chat_completions_auto_creates_session(client: TestClient, cfg: EngramConfig):
    payload = _chat_payload([{"role": "user", "content": "Hello!"}])
    r = client.post("/api/v1/chat/completions", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["session_id"].startswith("sess-")


def test_chat_completions_explicit_session(client: TestClient, cfg: EngramConfig):
    create_resp = client.post("/api/v1/sessions")
    assert create_resp.status_code == 201
    session_id = create_resp.json()["session_id"]

    payload = _chat_payload([{"role": "user", "content": "Hello!"}], session_id=session_id)
    r = client.post("/api/v1/chat/completions", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["session_id"] == session_id


def test_chat_completions_session_not_found(client: TestClient, cfg: EngramConfig):
    payload = _chat_payload(
        [{"role": "user", "content": "Hello!"}], session_id="sess-does-not-exist"
    )
    r = client.post("/api/v1/chat/completions", json=payload)
    assert r.status_code == 404


def test_chat_completions_invalid_role(client: TestClient, cfg: EngramConfig):
    payload = _chat_payload([{"role": "bad_role", "content": "Hello!"}])
    r = client.post("/api/v1/chat/completions", json=payload)
    assert r.status_code == 422


def test_chat_completions_empty_messages(client: TestClient, cfg: EngramConfig):
    payload = _chat_payload([])
    r = client.post("/api/v1/chat/completions", json=payload)
    assert r.status_code == 422


def test_chat_completions_last_message_not_user(client: TestClient, cfg: EngramConfig):
    payload = _chat_payload(
        [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "hey"},
        ]
    )
    r = client.post("/api/v1/chat/completions", json=payload)
    assert r.status_code == 422


def test_chat_completions_streaming(client: TestClient, cfg: EngramConfig):
    payload = _chat_payload([{"role": "user", "content": "Tell me a story."}], stream=True)
    r = client.post("/api/v1/chat/completions", json=payload)
    assert r.status_code == 200
    assert "text/event-stream" in r.headers.get("content-type", "")
    body = r.text
    assert "event: metadata" in body
    assert "event: delta" in body
    assert "event: done" in body


def test_chat_completions_turn_persisted(client: TestClient, cfg: EngramConfig):
    payload = _chat_payload([{"role": "user", "content": "Hello!"}])
    r = client.post("/api/v1/chat/completions", json=payload)
    assert r.status_code == 200
    session_id = r.json()["session_id"]

    get_resp = client.get(f"/api/v1/sessions/{session_id}")
    assert get_resp.status_code == 200
    sess = get_resp.json()
    assert sess["turn_count"] >= 2


def test_chat_completions_records_history_without_reingesting_answers(
    client: TestClient, cfg: EngramConfig
):
    payload = _chat_payload([{"role": "user", "content": "Hello!"}])
    r = client.post("/api/v1/chat/completions", json=payload)
    assert r.status_code == 200
    session_id = r.json()["session_id"]

    store = get_state().control_plane
    rows = (
        store.get_conn()
        .execute(
            "SELECT event_id, event_type, source, session_id, status FROM events "
            "WHERE session_id = ? AND source = 'retrieval_session'",
            (session_id,),
        )
        .fetchall()
    )
    assert len(rows) == 1
    assert rows[0]["event_type"] == "QUERY"
    assert rows[0]["source"] == "retrieval_session"
    assert rows[0]["session_id"] == session_id
    assert rows[0]["status"] == "COMPLETE"
    dispatches = (
        store.get_conn()
        .execute(
            "SELECT dispatch_id FROM workflow_dispatches WHERE aggregate_id = ?",
            (rows[0]["event_id"],),
        )
        .fetchall()
    )
    assert dispatches == []


def test_chat_completions_empty_messages_no_user(client: TestClient, cfg: EngramConfig):
    payload = _chat_payload([{"role": "assistant", "content": "hey"}])
    r = client.post("/api/v1/chat/completions", json=payload)
    assert r.status_code == 422


def test_chat_consumes_supplied_prior_messages(client: TestClient):
    response = client.post(
        "/api/v1/chat/completions",
        json=_chat_payload(
            [
                {"role": "user", "content": "My name is Ravi."},
                {"role": "assistant", "content": "Hello Ravi."},
                {"role": "user", "content": "What name did I give you?"},
            ]
        ),
    )
    assert response.status_code == 200
    assert response.json()["retrieval_metadata"]["answer_mode"] == "current-session"


def test_streaming_reports_persistence_failure_before_answer(client: TestClient, monkeypatch):
    def fail(*args, **kwargs):
        raise ConnectionError("simulated persistence outage")

    monkeypatch.setattr(chat_route, "_append_turn_and_record", fail)
    response = client.post(
        "/api/v1/chat/completions",
        json=_chat_payload(
            [
                {"role": "user", "content": "Explain binary search"},
            ],
            stream=True,
        ),
    )
    assert "event: error" in response.text
    assert "event: delta" not in response.text


def test_cached_session_can_continue_after_cache_loss(client: TestClient):
    response = client.post(
        "/api/v1/chat/completions",
        json=_chat_payload(
            [
                {"role": "user", "content": "My name is Ravi"},
            ]
        ),
    )
    session_id = response.json()["session_id"]
    get_state().session_cache.delete(session_id)
    response = client.post(
        "/api/v1/chat/completions",
        json=_chat_payload(
            [
                {"role": "user", "content": "What is my name?"},
            ],
            session_id=session_id,
        ),
    )
    assert response.status_code == 200
    assert response.json()["retrieval_metadata"]["answer_mode"] == "current-session"


def test_idempotent_chat_replays_without_duplicate_user_or_answer(client: TestClient):
    body = _chat_payload([{"role": "user", "content": "My name is Ravi."}])
    body["client_message_id"] = "retry-one"
    first = client.post("/api/v1/chat/completions", json=body)
    second = client.post("/api/v1/chat/completions", json=body)
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    session = client.get("/api/v1/sessions/" + first.json()["session_id"]).json()
    assert session["turn_count"] == 2
    body["messages"][0]["content"] = "A different message"
    assert client.post("/api/v1/chat/completions", json=body).status_code == 409


def test_failed_generation_preserves_accepted_user_message(client: TestClient, monkeypatch):
    def fail(*args, **kwargs):
        raise RuntimeError("secret-provider-error")

    monkeypatch.setattr(chat_route, "run_query", fail)
    response = client.post(
        "/api/v1/chat/completions",
        json=_chat_payload(
            [
                {"role": "user", "content": "Keep this message despite a provider failure"},
            ]
        ),
    )
    assert response.status_code == 503
    assert "secret-provider-error" not in response.text
    detail = response.json()["detail"]
    session = client.get("/api/v1/sessions/" + detail["session_id"]).json()
    assert session["turn_count"] == 1
    run = client.get("/api/v1/chat/runs/" + detail["run_id"]).json()
    assert run["status"] == "FAILED"


def test_accepted_chat_handoff_is_user_only_and_independent_of_response(
    client: TestClient, cfg, monkeypatch
):
    from engram.storage.memory_repository import MemoryRepository

    state = get_state()
    state.memory_repository = MemoryRepository(state.control_plane)
    state.memory_repository.ensure_schema()
    cfg.canonical_memory.enabled = True
    cfg.canonical_memory.automatic_user_memory_enabled = True

    def fail(*args, **kwargs):
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr(chat_route, "run_query", fail)
    response = client.post(
        "/api/v1/chat/completions",
        json=_chat_payload(
            [
                {"role": "user", "content": "I live in Pune."},
            ]
        ),
    )
    run = client.get("/api/v1/chat/runs/" + response.json()["detail"]["run_id"]).json()
    event = state.control_plane.get_event(run["input_event_id"], tenant_id="_default")
    assert event["event_type"] == "INGEST"
    assert event["payload"]["source_policy"] == "user-only-v1"
    assert event["payload"]["turn_pair"]["assistant"]["content"] == ""
    assert (
        state.control_plane.get_dispatch_for_aggregate("INGEST", event["event_id"])["status"]
        == "PENDING"
    )


def test_response_run_status_is_tenant_scoped(client: TestClient):
    from engram.tenancy import TenantQuotas

    response = client.post(
        "/api/v1/chat/completions",
        json=_chat_payload(
            [
                {"role": "user", "content": "Hello"},
            ]
        ),
    ).json()
    _, key = get_state().tenant_registry.create(
        "other", display_name="Other", quotas=TenantQuotas()
    )
    assert (
        client.get(
            "/api/v1/chat/runs/" + response["run_id"], headers={"Authorization": "Bearer " + key}
        ).status_code
        == 404
    )
    assert (
        client.get(
            "/api/v1/sessions/" + response["session_id"] + "/chat-run",
            headers={"Authorization": "Bearer " + key},
        ).status_code
        == 404
    )


def test_busy_and_duplicate_requests_recover_the_same_run(client: TestClient, monkeypatch):
    entered, release = Event(), Event()
    original = chat_route.run_query

    def delayed(*args, **kwargs):
        entered.set()
        assert release.wait(15)
        return original(*args, **kwargs)

    monkeypatch.setattr(chat_route, "run_query", delayed)
    sid = client.post("/api/v1/sessions").json()["session_id"]
    body = _chat_payload([{"role": "user", "content": "My name is Ravi."}], session_id=sid)
    body["client_message_id"] = "first-message"
    assert client.get(f"/api/v1/sessions/{sid}/chat-run").json() == {"run": None}
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(client.post, "/api/v1/chat/completions", json=body)
        try:
            assert entered.wait(10)
            duplicate = client.post("/api/v1/chat/completions", json=body)
            busy = client.post(
                "/api/v1/chat/completions", json=body | {"client_message_id": "second-message"}
            )
            assert duplicate.status_code == busy.status_code == 409
            assert duplicate.json()["code"] == busy.json()["code"] == "session_busy"
            run = busy.json()["run"]
            assert run["run_id"] == duplicate.json()["run"]["run_id"]
            assert run["client_message_id"] == "first-message"
            assert run["status"] == "RUNNING"
            assert run["deadline"] and run["created_at"]
            assert isinstance(busy.json()["detail"], str)
            found = client.get(
                f"/api/v1/sessions/{sid}/chat-run", params={"client_message_id": "first-message"}
            ).json()["run"]
            assert found["run_id"] == run["run_id"]
            assert client.get(
                f"/api/v1/sessions/{sid}/chat-run", params={"client_message_id": "second-message"}
            ).json() == {"run": None}
            assert client.get(f"/api/v1/sessions/{sid}").json()["turn_count"] == 1
            changed = body | {"messages": [{"role": "user", "content": "Changed input"}]}
            assert client.post("/api/v1/chat/completions", json=changed).status_code == 409
        finally:
            release.set()
        first = future.result()
    assert first.status_code == 200
    assert client.post("/api/v1/chat/completions", json=body).json() == first.json()
    assert client.get(f"/api/v1/sessions/{sid}").json()["turn_count"] == 2


def test_cancel_after_completion_returns_the_completed_response(client: TestClient):
    first = client.post(
        "/api/v1/chat/completions", json=_chat_payload([{"role": "user", "content": "Hello"}])
    ).json()
    cancelled = client.post(f"/api/v1/chat/runs/{first['run_id']}/cancel")
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "COMPLETED"
    assert cancelled.json()["response"] == first


def test_sse_steps_do_not_extend_the_absolute_deadline(client: TestClient, monkeypatch):
    import queue
    from types import SimpleNamespace

    from engram.api.schemas import ChatCompletionRequest
    from engram.storage.chat_runs import accept_message, find_run

    state = get_state()
    manager = chat_route._manager(state)
    session = manager.create()
    run = accept_message(
        manager,
        session.session_id,
        "Question",
        tenant="_default",
        client_message_id="deadline-message",
        input_hash="hash",
        automatic_memory=False,
    )
    run["deadline"] = datetime.now(timezone.utc) + timedelta(seconds=5)
    req = ChatCompletionRequest(messages=[{"role": "user", "content": "Question"}], stream=True)
    waits = []

    class StepQueue:
        def get(self, *, timeout):
            waits.append(timeout)
            return ("retrieval_step", {"step": "l1_plan"})

    class DormantWorker:
        def __init__(self, **kwargs):
            pass

        def start(self):
            pass

        def join(self, **kwargs):
            pass

    clock = iter([0.0, 1.0, 6.0])
    monkeypatch.setattr(chat_route, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    monkeypatch.setattr(chat_route, "queue", SimpleNamespace(Queue=StepQueue, Empty=queue.Empty))
    monkeypatch.setattr(chat_route, "threading", SimpleNamespace(Thread=DormantWorker))
    events = list(
        chat_route._sse_chat_stream_realtime(
            None, req, session, state, tenant_id="_default", run=run
        )
    )
    assert len(waits) == 1 and 0 < waits[0] < 5
    assert any("event: retrieval_step" in item for item in events)
    assert any("query timeout" in item for item in events)
    assert not any("event: delta" in item for item in events)
    persisted = find_run(state.control_plane, "_default", run_id=run["run_id"])
    assert persisted["status"] == "FAILED"
    assert persisted["error_code"] == "deadline_exceeded"
