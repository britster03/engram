"""Integration tests for the chat completions endpoint.

Uses a minimal FastAPI app with the chat router mounted and the global
AppState patched to point at stub providers so no real API keys are needed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from engram.api.routes import chat as chat_route
from engram.api.routes import sessions as sessions_route
from engram.audit import AuditLog
from engram.cache import EmbeddingCache, MemoryCache, OverviewCache
from engram.config import EngramConfig
from engram.deps import AppState, reset_state
from engram.storage.filesystem import FilesystemStore
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from engram.storage.redis_cache import SessionCache
from engram.storage.sqlite import SqliteStore
from engram.tenancy import TenantRegistry
from tests.integration.providers import (
    DeterministicCoreProvider,
    DeterministicEmbeddingService,
    DeterministicFrontierProvider,
)


def _build_state(cfg: EngramConfig) -> AppState:
    sqlite = SqliteStore(cfg.event_ledger.path)
    fs = FilesystemStore(cfg.filesystem.data_dir, create_dirs=cfg.filesystem.create_dirs)
    fake_neo = InMemoryKnowledgeGraph()
    session_cache = SessionCache(
        cfg.session_cache, default_ttl_seconds=cfg.session.timeout_minutes * 60
    )
    core = DeterministicCoreProvider()
    frontier = DeterministicFrontierProvider()
    embed = DeterministicEmbeddingService()  # type: ignore[arg-type]
    tenant_registry = TenantRegistry(cfg.event_ledger.path)
    tenant_registry.ensure_default(legacy_api_key=cfg.api.api_key)
    audit = AuditLog(cfg.event_ledger.path)
    embed_cache = EmbeddingCache(MemoryCache())
    overview_cache = OverviewCache(MemoryCache())
    return AppState(
        cfg=cfg,
        sqlite=sqlite,
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
    cfg = EngramConfig.model_validate({
        "api": {"api_key": "test-key"},
        "core_model": {"provider": "anthropic", "api_key": "x"},
        "frontier_llm": {"provider": "anthropic", "api_key": "x"},
        "filesystem": {"data_dir": str(tmp_path / "mem")},
        "event_ledger": {"path": str(tmp_path / "ev.db")},
        "consolidation": {"db_path": str(tmp_path / "cons.db")},
        "session_cache": {"backend": "memory"},
        "knowledge_graph": {"writer_password": "x", "reader_password": "x"},
        "retrieval": {"l0_skip": True},
    })
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
    messages: list[dict[str, str]],
    stream: bool = False,
    session_id: str | None = None
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
    payload = _chat_payload([
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "hey"},
    ])
    r = client.post("/api/v1/chat/completions", json=payload)
    assert r.status_code == 422


def test_chat_completions_streaming(client: TestClient, cfg: EngramConfig):
    payload = _chat_payload(
        [{"role": "user", "content": "Tell me a story."}], stream=True
    )
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


def test_chat_completions_ingest_fired(client: TestClient, cfg: EngramConfig):
    payload = _chat_payload([{"role": "user", "content": "Hello!"}])
    r = client.post("/api/v1/chat/completions", json=payload)
    assert r.status_code == 200
    session_id = r.json()["session_id"]

    sqlite = SqliteStore(cfg.event_ledger.path)
    rows = sqlite.get_conn().execute(
        "SELECT source, session_id FROM events WHERE session_id = ? AND source = 'session'",
        (session_id,),
    ).fetchall()
    assert len(rows) >= 1
    assert rows[0]["source"] == "session"
    assert rows[0]["session_id"] == session_id


def test_chat_completions_empty_messages_no_user(client: TestClient, cfg: EngramConfig):
    payload = _chat_payload([{"role": "assistant", "content": "hey"}])
    r = client.post("/api/v1/chat/completions", json=payload)
    assert r.status_code == 422
