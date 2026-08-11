"""End-to-end flow using stub LLMs + in-memory Neo4j.

This validates wiring (ingest pipeline → KG index → vector search → MSC → frontier)
without requiring Docker or an API key.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from engram.config import EngramConfig
from engram.frontmatter import parse
from engram.ingest.worker import IngestContext, process_event
from engram.retrieval.orchestrator import (
    OrchestratorContext,
    _format_ltm_blocks,
    run_query,
)
from engram.storage.filesystem import FilesystemStore
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from engram.storage.sqlite import SqliteStore
from engram.uri import pair_id as pair_id_fn

from .providers import (
    DeterministicCoreProvider,
    DeterministicEmbeddingService,
    DeterministicFrontierProvider,
)


class LowConfidenceCoreProvider(DeterministicCoreProvider):
    def complete(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        output_schema=None,
        max_tokens=None,
        temperature=None,
    ):  # type: ignore[override]
        from engram.models.core import CompletionResult

        tag = system_prompt.split("]", 1)[0].lstrip("[") if system_prompt.startswith("[") else ""
        if tag == "GATE":
            return CompletionResult(output={"store": True, "reason": "fact"}, raw_text="{}")
        if tag == "EXTRACT":
            return CompletionResult(
                output={
                    "resolved_text": "The user may be moving to Lisbon.",
                    "l0_abstract": "The user may be moving to Lisbon.",
                    "triplets": [
                        {
                            "subject": "user",
                            "relation": "may_move_to",
                            "object": "Lisbon",
                            "confidence": 0.45,
                        }
                    ],
                },
                raw_text="{}",
            )
        if tag == "LINK":
            return CompletionResult(output={"matched_id": None, "confidence": 0.0}, raw_text="{}")
        return super().complete(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            output_schema=output_schema,
            max_tokens=max_tokens,
            temperature=temperature,
        )


class LiteralFactCoreProvider(DeterministicCoreProvider):
    def complete(self, **kwargs):  # type: ignore[no-untyped-def,override]
        from engram.models.core import CompletionResult

        prompt = str(kwargs.get("system_prompt") or "")
        if prompt.startswith("[GATE]"):
            return CompletionResult(output={"store": True, "reason": "fact"}, raw_text="{}")
        if prompt.startswith("[EXTRACT]"):
            return CompletionResult(
                output={
                    "resolved_text": "The user is a software engineer.",
                    "l0_abstract": "The user is a software engineer.",
                    "triplets": [
                        {
                            "subject": "user",
                            "relation": "has_role",
                            "object": "software engineer",
                            "object_kind": "LITERAL",
                            "confidence": 0.95,
                        }
                    ],
                },
                raw_text="{}",
            )
        return super().complete(**kwargs)


class CompoundFactCoreProvider(DeterministicCoreProvider):
    def complete(self, **kwargs):  # type: ignore[no-untyped-def,override]
        from engram.models.core import CompletionResult

        prompt = str(kwargs.get("system_prompt") or "")
        if prompt.startswith("[GATE]"):
            return CompletionResult(output={"store": True, "reason": "fact"}, raw_text="{}")
        if prompt.startswith("[EXTRACT]"):
            return CompletionResult(
                output={
                    "resolved_text": "Caroline likes painting and swimming.",
                    "l0_abstract": "Caroline likes painting and swimming.",
                    "triplets": [
                        {
                            "subject": "Caroline",
                            "relation": "likes",
                            "object": "painting and swimming",
                            "object_kind": "ENTITY",
                            "confidence": 0.95,
                        }
                    ],
                },
                raw_text="{}",
            )
        return super().complete(**kwargs)


class UnexpectedPlannerProvider(DeterministicCoreProvider):
    def complete(self, **kwargs):  # type: ignore[no-untyped-def,override]
        raise AssertionError("semantic planner must not run in this ablation")


@pytest.fixture
def cfg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> EngramConfig:
    monkeypatch.setenv("ENGRAM_API_KEY", "test-key")
    monkeypatch.setenv("NEO4J_ADMIN_PASSWORD", "x")
    # Build a config without going through load_config / env-interpolation
    cfg = EngramConfig.model_validate(
        {
            "api": {"api_key": "test-key"},
            "core_model": {"provider": "ollama_cloud", "api_key": "x"},
            "frontier_llm": {"provider": "ollama_cloud", "api_key": "x"},
            "filesystem": {"data_dir": str(tmp_path / "mem")},
            "event_ledger": {"path": str(tmp_path / "ev.db")},
            "consolidation": {"db_path": str(tmp_path / "cons.db")},
            "session_cache": {"backend": "memory"},
            "knowledge_graph": {"writer_password": "x", "reader_password": "x"},
            # Stub embeddings produce random cosine scores, so the L0 memory-hit
            # fallback would always BYPASS. Skip L0 in tests; the gate has its own
            # dedicated test.
            "retrieval": {"l0_skip": True},
        }
    )
    return cfg


def _build_contexts(cfg: EngramConfig):
    fake_neo = InMemoryKnowledgeGraph()
    sqlite = SqliteStore(cfg.event_ledger.path)
    fs = FilesystemStore(cfg.filesystem.data_dir)
    core = DeterministicCoreProvider()
    frontier = DeterministicFrontierProvider()
    embed = DeterministicEmbeddingService()
    ingest = IngestContext(
        cfg=cfg,
        sqlite=sqlite,
        fs=fs,
        neo4j=fake_neo,  # type: ignore[arg-type]
        core=core,
        embed=embed,
    )  # type: ignore[arg-type]
    orch = OrchestratorContext(
        cfg=cfg,
        fs=fs,
        neo4j=fake_neo,  # type: ignore[arg-type]
        core=core,
        frontier=frontier,
        embed=embed,
    )  # type: ignore[arg-type]
    return ingest, orch, sqlite, fake_neo


def test_ingest_gated_skip_on_pleasantry(cfg: EngramConfig):
    ingest_ctx, _, sqlite, _ = _build_contexts(cfg)
    session_id = "sess-gate"
    pid = pair_id_fn(session_id, 0, 1)
    event_id, _ = sqlite.record_event(
        pair_id=pid,
        session_id=session_id,
        source="test",
        event_type="INGEST",
        payload={
            "turn_pair": {
                "user": {"content": "thanks", "turn_idx": 0},
                "assistant": {"content": "ok", "turn_idx": 1},
            }
        },
    )
    final = process_event(ingest_ctx, event_id)
    assert final == "GATED_SKIP"


def test_atomization_provenance_survives_filesystem_and_graph_projection(
    cfg: EngramConfig,
) -> None:
    ingest_ctx, _, sqlite, neo = _build_contexts(cfg)
    ingest_ctx.core = CompoundFactCoreProvider()
    event_id, _ = sqlite.record_event(
        pair_id=pair_id_fn("sess-atomize", 0, 1),
        session_id="sess-atomize",
        source="test",
        event_type="INGEST",
        payload={
            "turn_pair": {
                "user": {"content": "I like painting and swimming.", "turn_idx": 0},
                "assistant": {"content": "Those sound fun.", "turn_idx": 1},
            }
        },
    )

    assert process_event(ingest_ctx, event_id) == "COMPLETE"
    fact_nodes = {uri: node for uri, node in neo.nodes.items() if node.get("node_type") == "FACT"}
    assert {node["fact_object"] for node in fact_nodes.values()} == {
        "painting",
        "swimming",
    }
    assert {node["fact_atomized_from"] for node in fact_nodes.values()} == {"painting and swimming"}
    for uri in fact_nodes:
        fact_file = parse(ingest_ctx.fs.read(uri))
        assert fact_file.frontmatter["fact"]["atomized_from"] == ("painting and swimming")


def test_full_flow_ingest_then_query(cfg: EngramConfig):
    ingest_ctx, orch_ctx, sqlite, neo = _build_contexts(cfg)
    session_id = "sess-e2e"
    pairs = [
        ("I just accepted a job at Meta on the recommendations team.", "Congrats!"),
        ("I will be in Menlo Park by April 28th.", "Noted."),
        ("My wife's birthday is July 12th.", "Got it."),
    ]
    for idx, (user_msg, asst_msg) in enumerate(pairs):
        pid = pair_id_fn(session_id, idx * 2, idx * 2 + 1)
        event_id, _ = sqlite.record_event(
            pair_id=pid,
            session_id=session_id,
            source="test",
            event_type="INGEST",
            payload={
                "turn_pair": {
                    "user": {"content": user_msg, "turn_idx": idx * 2},
                    "assistant": {"content": asst_msg, "turn_idx": idx * 2 + 1},
                },
            },
        )
        event = sqlite.get_event(event_id)
        assert event is not None
        event["payload"]["turn_pair"]["user"].update(
            {
                "external_id": f"D1:{idx * 2 + 1}",
                "speaker": "Caroline",
                "source_conversation_id": "sample-1",
                "source_session_id": "session_1",
                "source_task": "locomo",
            }
        )
        event["payload"]["turn_pair"]["assistant"].update(
            {
                "external_id": f"D1:{idx * 2 + 2}",
                "speaker": "Melanie",
                "source_conversation_id": "sample-1",
                "source_session_id": "session_1",
                "source_task": "locomo",
            }
        )
        with sqlite.transaction() as conn:
            conn.execute(
                "UPDATE events SET payload = ? WHERE event_id = ?",
                (json.dumps(event["payload"]), event_id),
            )
        assert process_event(ingest_ctx, event_id) == "COMPLETE"

        episode_uri = f"mem://user/episodes/{event_id}.md"
        memory = parse(ingest_ctx.fs.read(episode_uri))
        assert memory.frontmatter["source_turn_ids"] == [
            f"D1:{idx * 2 + 1}",
            f"D1:{idx * 2 + 2}",
        ]
        kg_episode = neo.nodes[episode_uri]
        assert kg_episode["id"] == memory.frontmatter["id"]
        assert kg_episode["source_turn_ids"] == memory.frontmatter["source_turn_ids"]
        assert memory.frontmatter["document"]["source_content_preserved"] is True
        assert "## Source turns" in memory.body
        assert user_msg in memory.body
        assert asst_msg in memory.body

    # KG should have episode nodes and some entity nodes.
    episode_count = sum(1 for n in neo.nodes.values() if n.get("node_type") == "DOCUMENT")
    entity_count = sum(1 for n in neo.nodes.values() if n.get("node_type") == "ENTITY")
    assert episode_count == 3
    assert entity_count >= 3
    works_at_fact = next(
        node
        for node in neo.nodes.values()
        if node.get("node_type") == "FACT" and node.get("fact_relation") == "works_at"
    )
    assert works_at_fact["fact_relation_normalized"] is True
    assert works_at_fact["fact_relation_review_required"] is False

    meta_episode_uri = next(
        uri
        for uri, node in neo.nodes.items()
        if node.get("node_type") == "DOCUMENT" and "Meta" in orch_ctx.fs.read(uri)
    )
    l1_blocks = _format_ltm_blocks(
        orch_ctx,
        [
            {
                "source_uri": meta_episode_uri,
                "l0_abstract": "User has a new job.",
                "retrieval_level": "L1",
            }
        ],
        "L1",
    )
    assert "accepted a job at Meta" in l1_blocks[0]

    # Query it — the stub frontier echoes retrieved sentences; we just check the pipeline runs.
    result = run_query(
        orch_ctx,
        session_id=None,
        query="Where does the user work?",
        include_trace=True,
    )
    md = result.retrieval_metadata.to_dict()
    # Stub LN-plan returns terminate_cascade=true so L2 is visited then the
    # cascade stops. L0 is visited first (skip=True → CONTINUE, still logged).
    assert md["cascade_depth_reached"] in {"L1", "L2", "L4"}
    assert "L1" in md["levels_visited"]
    assert md["nodes_retrieved"] >= 1
    assert result.answer  # non-empty
    assert result.retrieval_metadata.trace_id
    trace = result.retrieval_metadata.trace
    assert trace is not None
    assert trace["hits"]
    assert any(hit["source_turn_ids"] for hit in trace["hits"])
    assert "token_allocation" in trace
    assert {call["family"] for call in trace["model_calls"]} == {"core", "frontier"}
    assert all("provider_calls" in call for call in trace["model_calls"])

    # A benchmark lower bound must override early Ln sufficiency. The stub Ln
    # planner terminates at every layer, so this proves a forced-L4 manifest
    # cannot quietly describe an L2 execution.
    forced_l4 = run_query(
        orch_ctx,
        session_id=None,
        query="Where does the user work?",
        max_depth="L4",
        min_depth="L4",
        include_trace=True,
        retrieval_mode="forced",
    )
    forced_l4_md = forced_l4.retrieval_metadata
    assert forced_l4_md.cascade_depth_reached == "L4"
    assert forced_l4_md.levels_visited == ["L0", "L1", "L2", "L3", "L4"]
    assert forced_l4_md.min_depth == "L4"
    assert forced_l4_md.max_depth == "L4"
    assert forced_l4_md.trace is not None
    assert forced_l4_md.trace["request"] == {"min_depth": "L4", "max_depth": "L4"}

    # Frozen ablations must be materially distinct. Neither mode may invoke
    # the Core planner, and no-memory must not touch any stored memory.
    orch_ctx.core = UnexpectedPlannerProvider()
    no_memory = run_query(
        orch_ctx,
        session_id=None,
        query="Where does the user work?",
        include_trace=True,
        retrieval_mode="no_memory",
    )
    no_memory_md = no_memory.retrieval_metadata
    assert no_memory_md.retrieval_mode == "no_memory"
    assert no_memory_md.cascade_depth_reached == "NO_MEMORY"
    assert no_memory_md.nodes_retrieved == 0
    assert no_memory_md.l0_decision == "NOT_RUN"
    assert no_memory_md.trace is not None
    assert no_memory_md.trace["retrieval_mode"] == "no_memory"
    assert {call["family"] for call in no_memory_md.trace["model_calls"]} == {"frontier"}

    vector_only = run_query(
        orch_ctx,
        session_id=None,
        query="Where does the user work?",
        include_trace=True,
        retrieval_mode="vector_only",
    )
    vector_md = vector_only.retrieval_metadata
    assert vector_md.retrieval_mode == "vector_only"
    assert vector_md.cascade_depth_reached == "L1"
    assert vector_md.nodes_retrieved >= 1
    assert vector_md.l0_decision == "NOT_RUN"
    assert vector_md.trace is not None
    assert vector_md.trace["retrieval_mode"] == "vector_only"
    assert {call["family"] for call in vector_md.trace["model_calls"]} == {"frontier"}


def test_low_confidence_triplet_writes_fact_node(cfg: EngramConfig):
    ingest_ctx, _, sqlite, neo = _build_contexts(cfg)
    ingest_ctx.core = LowConfidenceCoreProvider()  # type: ignore[assignment]
    session_id = "sess-low-confidence"
    pid = pair_id_fn(session_id, 0, 1)
    event_id, _ = sqlite.record_event(
        pair_id=pid,
        session_id=session_id,
        source="test",
        event_type="INGEST",
        payload={
            "turn_pair": {
                "user": {"content": "I might move to Lisbon.", "turn_idx": 0},
                "assistant": {"content": "Noted.", "turn_idx": 1},
            },
        },
    )

    assert process_event(ingest_ctx, event_id) == "COMPLETE"

    facts = [
        node
        for node in neo.nodes.values()
        if node.get("node_type") == "FACT" and node.get("status") == "LOW_CONFIDENCE"
    ]
    assert len(facts) == 1
    assert facts[0]["confidence"] == 0.45
    assert not [
        edge
        for edge in neo.edges
        if edge["type"] == "RELATES_TO" and edge["relation_label"] == "may_move_to"
    ]


def test_high_confidence_literal_is_preserved_as_fact_without_phantom_entity(
    cfg: EngramConfig,
):
    ingest_ctx, _, sqlite, neo = _build_contexts(cfg)
    ingest_ctx.core = LiteralFactCoreProvider()  # type: ignore[assignment]
    session_id = "sess-literal-fact"
    event_id, _ = sqlite.record_event(
        pair_id=pair_id_fn(session_id, 0, 1),
        session_id=session_id,
        source="test",
        event_type="INGEST",
        payload={
            "turn_pair": {
                "user": {
                    "content": "I am a software engineer.",
                    "turn_idx": 0,
                    "external_id": "D1:1",
                },
                "assistant": {
                    "content": "Noted.",
                    "turn_idx": 1,
                    "external_id": "D1:2",
                },
            },
        },
    )

    assert process_event(ingest_ctx, event_id) == "COMPLETE"

    facts = [node for node in neo.nodes.values() if node.get("node_type") == "FACT"]
    assert len(facts) == 1
    assert facts[0]["status"] == "ACTIVE"
    assert facts[0]["fact_object_kind"] == "LITERAL"
    assert facts[0]["source_turn_ids"] == ["D1:1", "D1:2"]
    assert "mem://user/entities/software-engineer/software-engineer.md" not in neo.nodes
    assert not [edge for edge in neo.edges if edge["type"] == "RELATES_TO"]
    assert any(
        edge["type"] == "REFERENCES" and edge["relation_label"] == "assertion" for edge in neo.edges
    )
