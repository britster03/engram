"""Consolidation worker coverage: manifest regen + overview generation."""

from __future__ import annotations

from pathlib import Path

import pytest

from engram.config import EngramConfig
from engram.consolidation.tasks import (
    handle_consolidate_overview,
    handle_propagate_overview,
    handle_regenerate_manifest,
)
from engram.consolidation.worker import ConsolidationContext, process_one
from engram.storage.filesystem import FilesystemStore
from engram.storage.sqlite import SqliteStore

from engram.storage.memory_kg import InMemoryKnowledgeGraph
from .providers import DeterministicCoreProvider


@pytest.fixture
def cfg(tmp_path: Path) -> EngramConfig:
    return EngramConfig.model_validate({
        "api": {"api_key": "test-key"},
        "core_model": {"provider": "anthropic", "api_key": "x"},
        "frontier_llm": {"provider": "anthropic", "api_key": "x"},
        "filesystem": {"data_dir": str(tmp_path / "mem")},
        "event_ledger": {"path": str(tmp_path / "ev.db")},
        "knowledge_graph": {"writer_password": "x", "reader_password": "x"},
    })


def _seed(fs: FilesystemStore) -> None:
    fs.write_atomic("mem://user/entities/alice/alice.md",
                    "---\nid: 1\nnode_type: ENTITY\nstatus: ACTIVE\n"
                    "created_at: 2026-04-01T00:00:00Z\nschema_version: 1\n---\n"
                    "Alice is a staff ML engineer.\n")
    fs.write_atomic("mem://user/entities/alice/alice_works_at_meta.md",
                    "---\nid: 2\nnode_type: FACT\nstatus: ACTIVE\n"
                    "created_at: 2026-04-02T00:00:00Z\nschema_version: 1\n---\n"
                    "Alice works at Meta on the recommendations team.\n")


def test_regenerate_manifest_lists_children(cfg: EngramConfig):
    fs = FilesystemStore(cfg.filesystem.data_dir)
    _seed(fs)
    handle_regenerate_manifest(
        node_id="mem://user/entities/alice",
        fs=fs,
        cfg=cfg.consolidation,
    )
    manifest = fs.read_manifest("mem://user/entities/alice")
    assert manifest is not None
    assert "alice.md" in manifest
    assert "alice_works_at_meta.md" in manifest


def test_consolidate_overview_writes_file(cfg: EngramConfig):
    fs = FilesystemStore(cfg.filesystem.data_dir)
    _seed(fs)
    neo = InMemoryKnowledgeGraph()
    handle_consolidate_overview(
        node_id="mem://user/entities/alice",
        fs=fs,
        neo4j=neo,  # type: ignore[arg-type]
        core=DeterministicCoreProvider(),
        cfg=cfg.consolidation,
    )
    overview_path = fs.path_for("mem://user/entities/alice/overview.md")
    assert overview_path.exists()
    body = overview_path.read_text()
    assert "Overview" in body


def test_propagate_overview_enqueues_ancestors(cfg: EngramConfig):
    fs = FilesystemStore(cfg.filesystem.data_dir)
    _seed(fs)
    sqlite = SqliteStore(cfg.event_ledger.path)
    handle_propagate_overview(
        node_id="mem://user/entities/alice",
        sqlite=sqlite,
        cfg=cfg.consolidation,
    )
    # Expect tasks for mem://user/entities and mem://user (ancestors of alice)
    rows = sqlite.get_conn().execute(
        "SELECT node_id, task_type FROM consolidation_tasks ORDER BY node_id"
    ).fetchall()
    node_ids = [r["node_id"] for r in rows]
    assert "mem://user/entities" in node_ids
    assert "mem://user" in node_ids


def test_worker_drains_queue(cfg: EngramConfig):
    fs = FilesystemStore(cfg.filesystem.data_dir)
    _seed(fs)
    sqlite = SqliteStore(cfg.event_ledger.path)
    sqlite.enqueue_task(node_id="mem://user/entities/alice", task_type="REGENERATE_MANIFEST")
    from .providers import DeterministicEmbeddingService
    ctx = ConsolidationContext(
        cfg=cfg,
        sqlite=sqlite,
        fs=fs,
        neo4j=InMemoryKnowledgeGraph(),  # type: ignore[arg-type]
        core=DeterministicCoreProvider(),
        embed=DeterministicEmbeddingService(),  # type: ignore[arg-type]
    )
    assert process_one(ctx) is True
    assert sqlite.queue_depth() == 0
    # Re-running when queue is empty returns False
    assert process_one(ctx) is False
