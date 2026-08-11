"""Tests for typed retrieval-command execution and MSC enrichment."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from engram.config import EngramConfig
from engram.frontmatter import MemoryFile
from engram.retrieval.orchestrator import (
    OrchestratorContext,
    _execute_commands,
    _format_ltm_blocks,
)
from engram.storage.filesystem import FilesystemStore


def _context(tmp_path: Path) -> OrchestratorContext:
    cfg = EngramConfig.model_validate({
        "api": {"api_key": "test-key"},
        "core_model": {"provider": "ollama_cloud", "api_key": "x"},
        "frontier_llm": {"provider": "ollama_cloud", "api_key": "x"},
        "filesystem": {"data_dir": str(tmp_path / "mem")},
        "event_ledger": {"path": str(tmp_path / "events.db")},
        "session_cache": {"backend": "memory"},
        "knowledge_graph": {"writer_password": "x", "reader_password": "x"},
    })
    return OrchestratorContext(
        cfg=cfg,
        fs=FilesystemStore(cfg.filesystem.data_dir),
        neo4j=None,
        core=None,  # type: ignore[arg-type]
        frontier=None,  # type: ignore[arg-type]
        embed=None,  # type: ignore[arg-type]
    )


def _write_memory(
    ctx: OrchestratorContext,
    uri: str,
    body: str,
    *,
    node_type: str = "DOCUMENT",
    source_episode_uri: str | None = None,
) -> None:
    frontmatter = {
        "id": "memory-1",
        "node_type": node_type,
        "status": "ACTIVE",
        "created_at": "2026-08-11T00:00:00Z",
        "schema_version": 1,
        "provenance": {
            "confidence": 0.95,
            "source_turn_ids": ["D1:1", "D1:2"],
        },
    }
    if source_episode_uri is not None:
        frontmatter["source_episode_uri"] = source_episode_uri
    content = MemoryFile(
        frontmatter=frontmatter,
        body=body,
    ).serialize()
    ctx.fs.write_atomic(uri, content)


def test_cat_enriches_existing_hit_with_full_body(tmp_path: Path) -> None:
    ctx = _context(tmp_path)
    uri = "mem://user/episodes/session-1/event-1.md"
    full_body = "Brief abstract.\nThe decisive detail appears only in the body."
    _write_memory(ctx, uri, full_body)
    existing: list[dict[str, Any]] = [{
        "source_uri": uri,
        "l0_abstract": "Brief abstract.",
        "retrieval_level": "L1",
        "score": 0.9,
    }]

    enriched = _execute_commands(
        ctx,
        [{"template": "cat", "params": {"uri": uri}}],
        level="L2",
        existing=existing,
    )

    assert len(enriched) == 1
    assert enriched[0]["full_body"] == full_body + "\n"
    assert enriched[0]["retrieval_level"] == "L2_cat"
    assert "full_body" not in existing[0]

    trace: dict[str, Any] = {"selected_sources": []}
    blocks = _format_ltm_blocks(ctx, enriched, "L2", trace=trace)
    assert len(blocks) == 1
    assert "The decisive detail appears only in the body." in blocks[0]
    assert trace["selected_sources"] == [{
        "source_uri": uri,
        "retrieval_level": "L2_cat",
        "source_turn_ids": ["D1:1", "D1:2"],
    }]
    assert "decisive detail" not in str(trace)


def test_cat_adds_a_new_hit_with_full_body(tmp_path: Path) -> None:
    ctx = _context(tmp_path)
    uri = "mem://user/episodes/session-1/event-2.md"
    _write_memory(ctx, uri, "Complete source body.")

    enriched = _execute_commands(
        ctx,
        [{"command": "cat", "path": uri}],
        level="L1",
        existing=[],
    )

    assert enriched == [{
        "source_uri": uri,
        "l0_abstract": "Complete source body.",
        "full_body": "Complete source body.\n",
        "retrieval_level": "L1_cat",
    }]


def test_fact_context_includes_authoritative_source_episode_first(tmp_path: Path) -> None:
    ctx = _context(tmp_path)
    episode_uri = "mem://user/episodes/event-3.md"
    fact_uri = "mem://user/facts/event-3/0_reminds-of_art.md"
    _write_memory(
        ctx,
        episode_uri,
        "Summary omitted the reason.\n\n## Source turns\nThe exact reason was art and self-expression.",
    )
    _write_memory(
        ctx,
        fact_uri,
        "the bowl started_on a birthday",
        node_type="FACT",
        source_episode_uri=episode_uri,
    )
    trace: dict[str, Any] = {"selected_sources": []}

    blocks = _format_ltm_blocks(
        ctx,
        [{"source_uri": fact_uri, "retrieval_level": "L1"}],
        "L1",
        trace=trace,
    )

    assert len(blocks) == 2
    assert "art and self-expression" in blocks[0]
    assert "started_on" in blocks[1]
    assert [row["source_uri"] for row in trace["selected_sources"]] == [
        episode_uri,
        fact_uri,
    ]
    assert "art and self-expression" not in str(trace)
