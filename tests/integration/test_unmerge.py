"""Tests for the /unmerge operation (§8.6)."""

from __future__ import annotations

from pathlib import Path

import pytest

from engram import frontmatter
from engram.config import EngramConfig
from engram.ingest.unmerge import unmerge
from engram.models.core import CompletionResult, CoreModelProvider
from engram.storage.filesystem import FilesystemStore
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from engram.storage.sqlite import SqliteStore

from .providers import DeterministicEmbeddingService


class StubUnmergeCore(CoreModelProvider):
    def complete(self, **_kwargs):  # type: ignore[override]
        return CompletionResult(
            output={
                "splits": [
                    {
                        "name": "Alice Chen",
                        "l0_abstract": "Alice Chen is an ML engineer at Meta.",
                        "triplets": [
                            {
                                "subject": "alice",
                                "relation": "works_at",
                                "object": "meta",
                                "confidence": 0.9,
                            }
                        ],
                    },
                    {
                        "name": "Alice Smith",
                        "l0_abstract": "Alice Smith is the user's sister.",
                        "triplets": [],
                    },
                ]
            },
            raw_text="(stub)",
        )


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


def test_unmerge_splits_merged_entity(cfg: EngramConfig):
    fs = FilesystemStore(cfg.filesystem.data_dir)
    neo = InMemoryKnowledgeGraph()
    sqlite = SqliteStore(cfg.event_ledger.path)
    merged_uri = "mem://user/entities/alice/alice.md"
    fs.write_atomic(merged_uri,
                    "---\nid: merged-1\nnode_type: ENTITY\nstatus: ACTIVE\n"
                    "created_at: 2026-04-01T00:00:00Z\nschema_version: 1\n---\n"
                    "Alice is either an ML engineer at Meta or the user's sister.\n")
    # Register at least one linked_entities row so the unmerge has source context.
    event_id, _ = sqlite.record_event(
        pair_id="p1", session_id="s", source="test", event_type="INGEST", payload={}
    )
    sqlite.save_extraction(
        event_id=event_id,
        resolved_text="Alice works at Meta.",
        triplets=[{"subject": "alice", "relation": "works_at",
                   "object": "meta", "confidence": 0.9}],
        l0_abstract="Alice works at Meta.",
    )
    with sqlite.transaction() as conn:
        conn.execute(
            "INSERT INTO linked_entities (event_id, triplet_idx, subject_node_id, object_node_id) "
            "VALUES (?, 0, ?, ?)",
            (event_id, merged_uri, "mem://user/entities/meta/meta.md"),
        )

    result = unmerge(
        fs=fs,
        neo4j=neo,  # type: ignore[arg-type]
        sqlite=sqlite,
        core=StubUnmergeCore(),
        embed=DeterministicEmbeddingService(),  # type: ignore[arg-type]
        merged_uri=merged_uri,
    )
    assert len(result.split_uris) == 2
    # Every split is a new file with ACTIVE status
    for uri in result.split_uris:
        assert fs.exists(uri)
        mf = frontmatter.parse(fs.read(uri))
        assert mf.frontmatter["status"] == "ACTIVE"
    # Original is HISTORICAL
    original = frontmatter.parse(fs.read(merged_uri))
    assert original.frontmatter["status"] == "HISTORICAL"
    assert set(original.frontmatter["provenance"]["superseded_by"]) == set(result.split_uris)
