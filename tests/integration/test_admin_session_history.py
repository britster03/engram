"""Admin durable session-history endpoints."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from engram.admin import routes as admin_routes
from engram.audit import AuditLog
from engram.cache import EmbeddingCache, MemoryCache, OverviewCache
from engram.config import EngramConfig
from engram.deps import AppState, reset_state
from engram.storage.filesystem import FilesystemStore
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from engram.storage.redis_cache import SessionCache
from engram.tenancy import TenantRegistry
from engram.uri import pair_id
from tests.integration.providers import (
    DeterministicCoreProvider,
    DeterministicEmbeddingService,
    DeterministicFrontierProvider,
)
from tests.postgres_support import PostgresTestStore


def _build_state(cfg: EngramConfig) -> AppState:
    store = PostgresTestStore()
    tenant_registry = TenantRegistry(store)
    tenant_registry.ensure_default(legacy_api_key=cfg.api.api_key)
    return AppState(
        cfg=cfg,
        control_plane=store,
        fs=FilesystemStore(cfg.filesystem.data_dir),
        neo4j=InMemoryKnowledgeGraph(),
        session_cache=SessionCache(cfg.session_cache, default_ttl_seconds=3600),
        core=DeterministicCoreProvider(),
        frontier=DeterministicFrontierProvider(),
        embed=DeterministicEmbeddingService(),  # type: ignore[arg-type]
        tenant_registry=tenant_registry,
        audit=AuditLog(store),
        embedding_cache=EmbeddingCache(MemoryCache()),
        overview_cache=OverviewCache(MemoryCache()),
    )


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    cfg = EngramConfig.model_validate(
        {
            "api": {"api_key": "api-key", "admin_key": "admin-key"},
            "core_model": {"provider": "openai_responses", "api_key": "x"},
            "frontier_llm": {"provider": "openai_responses", "api_key": "x"},
            "filesystem": {"data_dir": str(tmp_path / "mem")},
            "event_ledger": {"dsn": "postgresql://test:test/test"},
            "session_cache": {"backend": "memory"},
            "knowledge_graph": {
                "backend": "memory",
                "writer_password": "x",
                "reader_password": "x",
            },
        }
    )
    state = _build_state(cfg)
    import engram.config as config_mod
    import engram.deps as deps_mod

    monkeypatch.setattr(config_mod, "_cached", cfg)
    monkeypatch.setattr(deps_mod, "_state", state)
    app = FastAPI()
    app.include_router(admin_routes.admin_router)
    test_client = TestClient(app)
    test_client.headers["Authorization"] = "Bearer admin-key"
    yield test_client, state
    test_client.close()
    reset_state()


def _record_pair(
    state: AppState,
    session_id: str,
    turn_idx: int,
    user: str,
    assistant: str,
    *,
    tenant_id: str = "_default",
) -> None:
    state.control_plane.record_event(
        pair_id=pair_id(session_id, turn_idx, turn_idx + 1),
        session_id=session_id,
        source="session",
        event_type="INGEST",
        tenant_id=tenant_id,
        payload={
            "session_id": session_id,
            "turn_pair": {
                "user": {
                    "content": user,
                    "turn_idx": turn_idx,
                    "timestamp": f"2026-01-01T00:00:{turn_idx:02d}Z",
                },
                "assistant": {
                    "content": assistant,
                    "turn_idx": turn_idx + 1,
                    "timestamp": f"2026-01-01T00:00:{turn_idx + 1:02d}Z",
                },
            },
        },
    )


def test_history_merges_cached_and_durable_turns_without_duplicates(client):
    c, state = client
    session_id = "sess-merged"
    _record_pair(state, session_id, 0, "first question", "first answer")
    state.session_cache.set(
        session_id,
        {
            "session_id": session_id,
            "status": "ACTIVE",
            "created_at": "2026-01-01T00:00:00Z",
            "turns": [
                {
                    "role": "user",
                    "content": "first question",
                    "turn_idx": 0,
                    "timestamp": "2026-01-01T00:00:00Z",
                },
                {
                    "role": "assistant",
                    "content": "first answer",
                    "turn_idx": 1,
                    "timestamp": "2026-01-01T00:00:01Z",
                },
                {
                    "role": "user",
                    "content": "second question",
                    "turn_idx": 2,
                    "timestamp": "2026-01-01T00:00:02Z",
                },
                {
                    "role": "assistant",
                    "content": "second answer",
                    "turn_idx": 3,
                    "timestamp": "2026-01-01T00:00:03Z",
                },
            ],
        },
        ttl_seconds=3600,
    )

    response = c.get(f"/admin/api/sessions/{session_id}/history")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ACTIVE"
    assert payload["turn_count"] == 4
    assert [turn["content"] for turn in payload["turns"]] == [
        "first question",
        "first answer",
        "second question",
        "second answer",
    ]


def test_durable_history_is_listed_after_cache_expiry_and_is_tenant_scoped(client):
    c, state = client
    archived_id = "sess-archived"
    other_tenant_id = "sess-private"
    _record_pair(state, archived_id, 0, "saved question", "saved answer")
    _record_pair(state, other_tenant_id, 0, "private question", "private answer", tenant_id="other")

    listed = c.get("/admin/api/sessions")
    history = c.get(f"/admin/api/sessions/{archived_id}/history")
    foreign = c.get(f"/admin/api/sessions/{other_tenant_id}/history")
    missing = c.get("/admin/api/sessions/sess-missing/history")

    assert listed.status_code == 200
    session = next(item for item in listed.json()["sessions"] if item["id"] == archived_id)
    assert session["status"] == "ARCHIVED"
    assert session["turns"] == 2
    assert history.status_code == 200
    assert [turn["content"] for turn in history.json()["turns"]] == [
        "saved question",
        "saved answer",
    ]
    assert foreign.status_code == 404
    assert missing.status_code == 404


@pytest.mark.parametrize("cached", [False, True])
def test_separate_durable_chat_events_have_no_empty_or_duplicate_turns(client, cached):
    from engram.api.routes.chat import _manager
    from engram.storage.chat_runs import accept_message, finish_response
    from engram.tenancy import set_current_tenant

    c, state = client
    set_current_tenant(None)
    manager = _manager(state)
    session = manager.create()
    runs = []
    for index in range(2):
        run = accept_message(
            manager,
            session.session_id,
            "Repeated question",
            tenant="_default",
            client_message_id=f"message-{index}",
            input_hash=str(index),
            automatic_memory=False,
        )
        finish_response(manager, run["run_id"], {"answer": "Repeated answer"}, tenant="_default")
        runs.append(run)
    if cached:
        manager.get(session.session_id)
    else:
        state.session_cache.delete(session.session_id)
    response = c.get(f"/admin/api/sessions/{session.session_id}/history")
    turns = response.json()["turns"]
    assert response.status_code == 200
    assert [turn["content"] for turn in turns] == ["Repeated question", "Repeated answer"] * 2
    assert [turn["turn_idx"] for turn in turns] == [0, 1, 2, 3]
    assert turns[0]["client_message_id"] == "message-0"
    assert turns[1]["run_id"] == runs[0]["run_id"]
    assert len({turn["id"] for turn in turns}) == 4


def test_pending_user_turn_comes_from_postgres_even_with_a_stale_cache(client):
    from engram.api.routes.chat import _manager
    from engram.storage.chat_runs import accept_message
    from engram.tenancy import set_current_tenant

    c, state = client
    set_current_tenant(None)
    manager = _manager(state)
    session = manager.create()
    run = accept_message(
        manager,
        session.session_id,
        "Pending question",
        tenant="_default",
        client_message_id="pending",
        input_hash="hash",
        automatic_memory=False,
    )
    state.session_cache.set(session.session_id, session.to_payload(), ttl_seconds=3600)
    turns = c.get(f"/admin/api/sessions/{session.session_id}/history").json()["turns"]
    assert len(turns) == 1 and turns[0]["content"] == "Pending question"
    status = c.get(f"/admin/api/chat/runs/{run['run_id']}")
    assert status.status_code == 200
    assert status.json()["status"] == "RUNNING"
    discovered = c.get(
        f"/admin/api/sessions/{session.session_id}/chat-run",
        params={"client_message_id": "pending"},
    )
    assert discovered.json()["run"]["run_id"] == run["run_id"]
    assert (
        c.get(
            f"/admin/api/chat/runs/{run['run_id']}", headers={"Authorization": "Bearer wrong"}
        ).status_code
        == 401
    )


def test_empty_durable_session_has_readable_history(client):
    c, _ = client
    sid = c.post("/admin/api/sessions").json()["session_id"]
    history = c.get(f"/admin/api/sessions/{sid}/history")
    assert history.status_code == 200
    assert history.json()["turns"] == []
    assert c.get(f"/admin/api/sessions/{sid}/chat-run").json() == {"run": None}
