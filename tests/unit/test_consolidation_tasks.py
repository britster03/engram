"""Tests for ATOMIZE / NORMALIZE / TEMPORALIZE / INTEGRATE task handlers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from engram.config import EngramConfig
from engram.consolidation.tasks import (
    handle_atomize,
    handle_integrate,
    handle_normalize,
    handle_temporalize,
)
from engram.storage.filesystem import FilesystemStore
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from engram.storage.sqlite import SqliteStore
from tests.integration.providers import DeterministicEmbeddingService


@pytest.fixture
def cfg(tmp_path: Path) -> EngramConfig:
    return EngramConfig.model_validate({
        "api": {"api_key": "test-key"},
        "core_model": {"provider": "ollama_cloud", "api_key": "x"},
        "frontier_llm": {"provider": "ollama_cloud", "api_key": "x"},
        "filesystem": {"data_dir": str(tmp_path / "mem")},
        "event_ledger": {"path": str(tmp_path / "ev.db")},
        "knowledge_graph": {"writer_password": "x", "reader_password": "x"},
    })


def _seed_extraction(
    sqlite: SqliteStore, triplets: list[dict], event_id: str = "evt-1"
) -> None:
    sqlite.record_event(
        pair_id=f"p-{event_id}",
        session_id="s",
        source="test",
        event_type="INGEST",
        payload={},
    )
    actual_ids = sqlite.get_conn().execute(
        "SELECT event_id FROM events WHERE pair_id = ?", (f"p-{event_id}",)
    ).fetchone()
    eid = actual_ids["event_id"]
    sqlite.save_extraction(
        event_id=eid,
        resolved_text="x",
        triplets=triplets,
        l0_abstract="x",
    )
    return eid


def test_atomize_splits_compound_objects(cfg: EngramConfig):
    sqlite = SqliteStore(cfg.event_ledger.path)
    eid = _seed_extraction(
        sqlite,
        [{"subject": "alice", "relation": "likes",
          "object": "coffee and tea", "confidence": 0.9}],
    )
    handle_atomize(node_id=eid, sqlite=sqlite, cfg=cfg.consolidation)
    row = sqlite.get_conn().execute(
        "SELECT triplets FROM extractions WHERE event_id = ?", (eid,)
    ).fetchone()
    triplets = json.loads(row["triplets"])
    objects = sorted(t["object"] for t in triplets)
    assert objects == ["coffee", "tea"]


def test_normalize_adds_canonical_form_when_similar(cfg: EngramConfig):
    sqlite = SqliteStore(cfg.event_ledger.path)
    # Extracted form is "Bob"; the canonical form in the KG is "Robert".
    eid = _seed_extraction(
        sqlite,
        [{"subject": "Bob", "relation": "is", "object": "friend", "confidence": 0.9}],
    )
    neo = InMemoryKnowledgeGraph()
    embed = DeterministicEmbeddingService()
    # The stub embedding hashes the text, so to get a high cosine match we
    # seed an entity whose embedding is built from the same surface form but
    # whose l0_abstract canonicalises to a different display name.
    neo.merge_node(
        source_uri="mem://user/entities/bob/bob.md",
        properties={
            "id": "1",
            "node_type": "ENTITY",
            "status": "ACTIVE",
            "l0_abstract": "Robert — mentioned in episode.",
            "l0_embedding": embed.embed("Bob"),
            "retrieval_weight": 1.0,
        },
    )
    handle_normalize(
        node_id=eid,
        sqlite=sqlite,
        neo4j=neo,  # type: ignore[arg-type]
        embed=embed,  # type: ignore[arg-type]
        cfg=cfg.consolidation,
    )
    row = sqlite.get_conn().execute(
        "SELECT triplets FROM extractions WHERE event_id = ?", (eid,)
    ).fetchone()
    triplets = json.loads(row["triplets"])
    assert triplets[0].get("subject_canonical") == "Robert"


def test_temporalize_attaches_date_from_body(cfg: EngramConfig):
    fs = FilesystemStore(cfg.filesystem.data_dir)
    uri = "mem://user/episodes/2026-05-01_started-job.md"
    fs.write_atomic(
        uri,
        "---\nid: 1\nnode_type: DOCUMENT\nstatus: ACTIVE\n"
        "created_at: 2026-05-01T00:00:00Z\nschema_version: 1\n---\n"
        "On 2026-05-04 I start at Meta.\n",
    )
    handle_temporalize(node_id=uri, fs=fs, cfg=cfg.consolidation)
    from engram import frontmatter
    mf = frontmatter.parse(fs.read(uri))
    assert mf.frontmatter["temporal"]["valid_from"] == "2026-05-04"


def test_integrate_requests_kg_only_replay(cfg: EngramConfig):
    sqlite = SqliteStore(cfg.event_ledger.path)
    eid = _seed_extraction(sqlite, [])
    sqlite.advance_event_stage(eid, "FILESYSTEM_COMMITTED", tenant_id="_default")
    sqlite.set_event_status(eid, "COMPLETE")
    handle_integrate(node_id=eid, sqlite=sqlite, cfg=cfg.consolidation)
    refreshed = sqlite.get_event(eid)
    assert refreshed is not None
    assert refreshed["status"] == "RECEIVED"
    state = sqlite.get_event_stage(eid, tenant_id="_default")
    assert state["completed_stage"] == "FILESYSTEM_COMMITTED"
