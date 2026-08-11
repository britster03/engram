"""Bulk ingest and KG graph API integration tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from engram.api.routes import bulk_ingest, events, kg, memories
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


@pytest.fixture
def cfg(tmp_path: Path) -> EngramConfig:
    return EngramConfig.model_validate({
        "api": {"api_key": "test-key"},
        "core_model": {"provider": "ollama_cloud", "api_key": "x"},
        "frontier_llm": {"provider": "ollama_cloud", "api_key": "x"},
        "filesystem": {"data_dir": str(tmp_path / "mem")},
        "event_ledger": {"path": str(tmp_path / "ev.db")},
        "consolidation": {"db_path": str(tmp_path / "cons.db")},
        "session_cache": {"backend": "memory"},
        "knowledge_graph": {"writer_password": "x", "reader_password": "x"},
    })


def _build_state(cfg: EngramConfig) -> AppState:
    sqlite = SqliteStore(cfg.event_ledger.path)
    fs = FilesystemStore(cfg.filesystem.data_dir)
    neo = InMemoryKnowledgeGraph()
    session_cache = SessionCache(
        cfg.session_cache, default_ttl_seconds=cfg.session.timeout_minutes * 60
    )
    tenant_registry = TenantRegistry(cfg.event_ledger.path)
    tenant_registry.ensure_default(legacy_api_key=cfg.api.api_key)
    return AppState(
        cfg=cfg,
        sqlite=sqlite,
        fs=fs,
        neo4j=neo,
        session_cache=session_cache,
        core=DeterministicCoreProvider(),
        frontier=DeterministicFrontierProvider(),
        embed=DeterministicEmbeddingService(),  # type: ignore[arg-type]
        tenant_registry=tenant_registry,
        audit=AuditLog(cfg.event_ledger.path),
        embedding_cache=EmbeddingCache(MemoryCache()),
        overview_cache=OverviewCache(MemoryCache()),
    )


@pytest.fixture
def client(cfg: EngramConfig, monkeypatch: pytest.MonkeyPatch):
    app = FastAPI()
    app.include_router(bulk_ingest.router)
    app.include_router(kg.router)
    app.include_router(memories.router)
    app.include_router(events.router)
    state = _build_state(cfg)
    import engram.config as config_mod
    import engram.deps as deps_mod

    monkeypatch.setattr(deps_mod, "_state", state)
    monkeypatch.setattr(config_mod, "_cached", cfg)
    with TestClient(app) as c:
        c.headers["Authorization"] = "Bearer test-key"
        yield c, state
    reset_state()


def test_bulk_jsonl_dry_run_reports_rejections(client):
    c, state = client
    body = b'{"user":"I moved to Oslo.","assistant":"Noted."}\n{"assistant":"missing user"}\n'
    resp = c.post(
        "/api/v1/ingest/bulk",
        data={"dry_run": "true", "file_format": "jsonl"},
        files={"file": ("turns.jsonl", body, "application/x-ndjson")},
    )
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "DRY_RUN"
    assert data["accepted_count"] == 1
    assert data["rejected_count"] == 1
    assert data["event_ids"] == []

    job = c.get(f"/api/v1/ingest/bulk/{data['job_id']}")
    assert job.status_code == 200
    assert job.json()["rejected_rows"][0]["row_number"] == 2
    rows = state.sqlite.get_conn().execute("SELECT event_id FROM events").fetchall()
    assert rows == []


def test_bulk_csv_queues_durable_events(client):
    c, state = client
    body = b"session_id,user,assistant\nsess-bulk,I work at Acme,Stored\n"
    resp = c.post(
        "/api/v1/ingest/bulk",
        data={"file_format": "csv"},
        files={"file": ("turns.csv", body, "text/csv")},
    )
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "QUEUED"
    assert data["accepted_count"] == 1
    assert len(data["event_ids"]) == 1
    ev = state.sqlite.get_event(data["event_ids"][0])
    assert ev is not None
    assert ev["tenant_id"] == "_default"
    assert ev["status"] == "RECEIVED"
    assert ev["session_id"] == "sess-bulk"
    job = c.get(f"/api/v1/ingest/bulk/{data['job_id']}")
    assert job.status_code == 200
    assert job.json()["status"] == "QUEUED"
    assert job.json()["completed_at"] is None

    state.sqlite.set_event_status(data["event_ids"][0], "COMPLETE", tenant_id="_default")
    completed = c.get(f"/api/v1/ingest/bulk/{data['job_id']}")
    assert completed.status_code == 200
    completed_data = completed.json()
    assert completed_data["status"] == "COMPLETE"
    assert completed_data["completed_at"] is not None


def test_exact_event_status_reports_ready_failed_and_missing(client):
    c, state = client
    ready_id, _ = state.sqlite.record_event(
        pair_id="ready",
        session_id="s",
        source="test",
        event_type="INGEST",
        payload={},
    )
    failed_id, _ = state.sqlite.record_event(
        pair_id="failed",
        session_id="s",
        source="test",
        event_type="INGEST",
        payload={},
    )
    state.sqlite.fs_outbox_write(ready_id, "mem://user/episodes/ready.md")
    state.sqlite.fs_outbox_mark(ready_id, "INDEXED")
    state.sqlite.set_event_status(ready_id, "COMPLETE")
    state.sqlite.set_event_status(failed_id, "FAILED", error_message="boom")

    response = c.post(
        "/api/v1/events/status",
        json={"event_ids": [ready_id, failed_id, "evt-missing"]},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["memory_ready"] is False
    assert body["ready_count"] == 1
    assert body["failed_count"] == 1
    assert body["missing_ids"] == ["evt-missing"]
    assert body["failures"][0]["error"] == "boom"
    assert body["events"][0]["created_at"] is not None
    assert body["events"][0]["processed_at"] is not None


def test_event_list_is_tenant_scoped_and_reports_unbounded_total(client):
    c, state = client
    expected = []
    for idx in range(3):
        event_id, _ = state.sqlite.record_event(
            pair_id=f"corpus-{idx}",
            session_id="s",
            source="locomo",
            event_type="INGEST",
            payload={},
        )
        expected.append(event_id)
    state.sqlite.record_event(
        pair_id="not-corpus",
        session_id="s",
        source="ui",
        event_type="INGEST",
        payload={},
    )

    response = c.get("/api/v1/events", params={"source": "locomo", "limit": 2})

    assert response.status_code == 200
    assert response.json() == {
        "source": "locomo",
        "total_count": 3,
        "event_ids": expected[:2],
    }


def test_memory_list_root_prefix_falls_back_to_filesystem(client):
    c, state = client
    state.fs.write_atomic(
        "mem://user/entities/qa-alice/qa-alice.md",
        """---
id: qa-alice
node_type: ENTITY
status: ACTIVE
created_at: "2026-06-07T00:00:00Z"
schema_version: 1
---
QA Alice fixture.
""",
    )

    resp = c.get("/api/v1/memories")

    assert resp.status_code == 200
    data = resp.json()
    assert data["items"] == [{
        "source_uri": "mem://user/entities/qa-alice/qa-alice.md",
        "node_type": "ENTITY",
        "l0_abstract": "QA Alice fixture.",
        "status": "ACTIVE",
    }]


def test_kg_graph_is_tenant_scoped_and_bounded(client):
    c, state = client
    state.neo4j.merge_node(
        source_uri="mem://user/entities/alice/alice.md",
        tenant_id="_default",
        properties={"node_type": "ENTITY", "status": "ACTIVE", "l0_abstract": "Alice"},
    )
    state.neo4j.merge_node(
        source_uri="mem://user/facts/f1.md",
        tenant_id="_default",
        properties={"node_type": "FACT", "status": "LOW_CONFIDENCE", "l0_abstract": "Alice may like tea"},
    )
    state.neo4j.merge_edge(
        subject_uri="mem://user/facts/f1.md",
        object_uri="mem://user/entities/alice/alice.md",
        relation_label="subject",
        edge_type="REFERENCES",
        tenant_id="_default",
    )
    state.neo4j.merge_node(
        source_uri="mem://user/entities/alice/alice.md",
        tenant_id="tenant-b",
        properties={"node_type": "ENTITY", "status": "ACTIVE", "l0_abstract": "Wrong tenant"},
    )

    resp = c.get(
        "/api/v1/kg/graph",
        params={"root_uri": "mem://user/entities/alice/alice.md", "depth": 1, "limit": 10},
    )
    assert resp.status_code == 200
    data = resp.json()
    abstracts = {n["l0_abstract"] for n in data["nodes"]}
    assert "Alice" in abstracts
    assert "Alice may like tea" in abstracts
    assert "Wrong tenant" not in abstracts
    assert len(data["nodes"]) <= 10
    assert data["edges"][0]["label"] == "subject"
