"""Bulk ingest and KG graph API integration tests."""

from __future__ import annotations

import io
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from engram.api.routes import bulk_ingest, kg, memories
from engram.audit import AuditLog
from engram.cache import EmbeddingCache, MemoryCache, OverviewCache
from engram.config import EngramConfig
from engram.deps import AppState, reset_state
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


@pytest.fixture
def cfg(tmp_path: Path) -> EngramConfig:
    return EngramConfig.model_validate(
        {
            "api": {"api_key": "test-key"},
            "core_model": {"provider": "openai_responses", "api_key": "x"},
            "frontier_llm": {"provider": "openai_responses", "api_key": "x"},
            "filesystem": {"data_dir": str(tmp_path / "mem")},
            "event_ledger": {"dsn": "postgresql://test:test/test"},
            "session_cache": {"backend": "memory"},
            "knowledge_graph": {"writer_password": "x", "reader_password": "x"},
        }
    )


def _build_state(cfg: EngramConfig) -> AppState:
    store = PostgresTestStore()
    fs = FilesystemStore(cfg.filesystem.data_dir)
    neo = InMemoryKnowledgeGraph()
    session_cache = SessionCache(
        cfg.session_cache, default_ttl_seconds=cfg.session.timeout_minutes * 60
    )
    tenant_registry = TenantRegistry(store)
    tenant_registry.ensure_default(legacy_api_key=cfg.api.api_key)
    return AppState(
        cfg=cfg,
        control_plane=store,
        fs=fs,
        neo4j=neo,
        session_cache=session_cache,
        core=DeterministicCoreProvider(),
        frontier=DeterministicFrontierProvider(),
        embed=DeterministicEmbeddingService(),  # type: ignore[arg-type]
        tenant_registry=tenant_registry,
        audit=AuditLog(store),
        embedding_cache=EmbeddingCache(MemoryCache()),
        overview_cache=OverviewCache(MemoryCache()),
    )


@pytest.fixture
def client(cfg: EngramConfig, monkeypatch: pytest.MonkeyPatch):
    app = FastAPI()
    app.include_router(bulk_ingest.router)
    app.include_router(kg.router)
    app.include_router(memories.router)
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
    rows = state.control_plane.get_conn().execute("SELECT event_id FROM events").fetchall()
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
    ev = state.control_plane.get_event(data["event_ids"][0])
    assert ev is not None
    assert ev["tenant_id"] == "_default"
    assert ev["status"] == "RECEIVED"
    assert ev["session_id"] == "sess-bulk"
    job = c.get(f"/api/v1/ingest/bulk/{data['job_id']}")
    assert job.status_code == 200
    assert job.json()["status"] == "QUEUED"
    assert job.json()["completed_at"] is None

    state.control_plane.set_event_status(data["event_ids"][0], "PROCESSING", tenant_id="_default")
    processing = c.get(f"/api/v1/ingest/bulk/{data['job_id']}")
    assert processing.status_code == 200
    assert processing.json()["status"] == "PROCESSING"
    assert processing.json()["completed_at"] is None

    state.control_plane.set_event_status(data["event_ids"][0], "COMPLETE", tenant_id="_default")
    completed = c.get(f"/api/v1/ingest/bulk/{data['job_id']}")
    assert completed.status_code == 200
    completed_data = completed.json()
    assert completed_data["status"] == "COMPLETE"
    assert completed_data["completed_at"] is not None


def test_bulk_response_serializes_postgres_datetime_timestamps():
    """PostgreSQL timestamps are normalized for API output."""
    timestamp = datetime(2026, 9, 7, 12, 0, tzinfo=timezone.utc)

    class Store:
        def get_event(self, _event_id, *, tenant_id):
            return {
                "event_id": "evt-postgres",
                "status": "RECEIVED",
                "payload": {"turn_pair": {"user": {"content": "hello"}}},
                "created_at": timestamp,
                "processed_at": timestamp,
            }

        def get_fs_outbox(self, _event_id, *, tenant_id):
            return None

        def get_bulk_code_items(self, _job_id, *, tenant_id):
            return []

    response = bulk_ingest._bulk_response(
        {
            "job_id": "bulk-postgres",
            "status": "QUEUED",
            "dry_run": False,
            "total_count": 1,
            "accepted_count": 1,
            "rejected_count": 0,
            "event_ids": ["evt-postgres"],
            "source": "bulk_jsonl",
            "created_at": timestamp,
        },
        state=SimpleNamespace(control_plane=Store()),
        tenant_id="_default",
    )

    assert response.created_at == timestamp.isoformat()
    assert response.events[0].created_at == timestamp.isoformat()
    assert response.events[0].processed_at == timestamp.isoformat()


def test_bulk_job_exposes_per_item_memory_and_graph_progress(client):
    c, state = client
    body = b"session_id,user,assistant\nsess-bulk,Explain the billing module,Stored\n"
    created = c.post(
        "/api/v1/ingest/bulk",
        data={"file_format": "csv"},
        files={"file": ("turns.csv", body, "text/csv")},
    )
    assert created.status_code == 202
    payload = created.json()
    event_id = payload["event_ids"][0]
    assert payload["events"][0] == {
        "event_id": event_id,
        "label": "Explain the billing module",
        "status": "RECEIVED",
        "graph_status": None,
        "retry_count": 0,
        "error_message": None,
        "created_at": payload["events"][0]["created_at"],
        "processed_at": None,
    }
    assert payload["code_items"] == []

    state.control_plane.set_event_status(event_id, "COMPLETE", tenant_id="_default")
    state.control_plane.fs_outbox_write(
        event_id, "mem://user/episodes/billing.md", tenant_id="_default"
    )
    state.control_plane.fs_outbox_mark(event_id, "INDEXED")

    job = c.get(f"/api/v1/ingest/bulk/{payload['job_id']}")
    assert job.status_code == 200
    item = job.json()["events"][0]
    assert item["label"] == "Explain the billing module"
    assert item["status"] == "COMPLETE"
    assert item["graph_status"] == "INDEXED"


def test_code_zip_creates_structured_project_and_code_map(client):
    c, _state = client
    body = io.BytesIO()
    with zipfile.ZipFile(body, "w") as archive:
        archive.writestr(
            "app/service.py",
            "from .repository import find_user\n"
            "class UserService(BaseService):\n"
            "    def login(self, email):\n"
            "        return find_user(email)\n",
        )
        archive.writestr("app/repository.py", "def find_user(email):\n    return email\n")
    created = c.post(
        "/api/v1/ingest/bulk",
        data={"file_format": "zip", "project_name": "User service"},
        files={"file": ("user-service.zip", body.getvalue(), "application/zip")},
    )
    assert created.status_code == 202
    payload = created.json()
    assert payload["status"] == "COMPLETE"
    assert payload["project_uri"] == "mem://projects/user-service"
    assert {item["status"] for item in payload["code_items"]} == {"INDEXED"}
    assert payload["code_items"][0]["node_count"] > 0

    projects = c.get("/api/v1/kg/projects")
    assert projects.status_code == 200
    assert projects.json()[0]["name"] == "User service"
    mapped = c.get(
        "/api/v1/kg/code-map", params={"project_uri": payload["project_uri"], "depth": 4}
    )
    assert mapped.status_code == 200
    assert {node["node_type"] for node in mapped.json()["nodes"]} >= {
        "PROJECT",
        "FILE",
        "CLASS",
        "METHOD",
    }
    login = next(node for node in mapped.json()["nodes"] if node["label"] == "login")
    details = c.get("/api/v1/kg/node-details", params={"source_uri": login["source_uri"]})
    assert details.status_code == 200
    assert details.json()["node"]["signature"] == "def login(self, email)"
    assert all("l0_embedding" not in data for data in [details.json()["node"]])
    assert any(item["relation"] == "CALLS" for item in details.json()["outgoing"])


def test_code_zip_requires_a_project_name(client):
    c, _state = client
    body = io.BytesIO()
    with zipfile.ZipFile(body, "w") as archive:
        archive.writestr("hello.py", "def hello():\n    return 'hello'\n")
    response = c.post(
        "/api/v1/ingest/bulk",
        data={"file_format": "zip"},
        files={"file": ("hello.zip", body.getvalue(), "application/zip")},
    )
    assert response.status_code == 422
    assert "project_name" in response.json()["detail"]


def test_bulk_retry_requeues_only_a_failed_item(client):
    c, state = client
    body = b"session_id,user,assistant\nsess-bulk,First item,Stored\nsess-bulk,Second item,Stored\n"
    created = c.post(
        "/api/v1/ingest/bulk",
        data={"file_format": "csv"},
        files={"file": ("turns.csv", body, "text/csv")},
    )
    assert created.status_code == 202
    job = created.json()
    first_event, failed_event = job["event_ids"]
    state.control_plane.set_event_status(first_event, "COMPLETE", tenant_id="_default")
    state.control_plane.set_event_status(
        failed_event, "FAILED", "provider timed out", tenant_id="_default"
    )

    terminal = c.get(f"/api/v1/ingest/bulk/{job['job_id']}")
    assert terminal.json()["status"] == "FAILED"

    retry = c.post(f"/api/v1/ingest/bulk/{job['job_id']}/events/{failed_event}/retry")
    assert retry.status_code == 200
    assert retry.json()["event_id"] == failed_event
    assert retry.json()["status"] == "RECEIVED"
    assert retry.json()["retry_count"] == 1
    assert retry.json()["error_message"] is None

    assert state.control_plane.get_event(first_event)["status"] == "COMPLETE"
    assert state.control_plane.get_event(failed_event)["status"] == "RECEIVED"
    refreshed = c.get(f"/api/v1/ingest/bulk/{job['job_id']}")
    assert refreshed.json()["status"] == "QUEUED"

    not_failed = c.post(f"/api/v1/ingest/bulk/{job['job_id']}/events/{first_event}/retry")
    assert not_failed.status_code == 409


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
    assert data["items"] == [
        {
            "source_uri": "mem://user/entities/qa-alice/qa-alice.md",
            "node_type": "ENTITY",
            "l0_abstract": "QA Alice fixture.",
            "status": "ACTIVE",
        }
    ]


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
        properties={
            "node_type": "FACT",
            "status": "LOW_CONFIDENCE",
            "l0_abstract": "Alice may like tea",
        },
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
