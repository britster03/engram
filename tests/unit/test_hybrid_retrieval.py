from __future__ import annotations

import threading
import time

import pytest

from engram import prompts
from engram.retrieval.domain import RetrievalPlan, RetrievalRoute, VerifiedEvidence
from engram.retrieval.evidence import EvidenceGate
from engram.retrieval.hybrid import discover, reciprocal_rank_fusion
from engram.retrieval.orchestrator import OrchestratorContext, _retrieve_route
from engram.storage.lexical import projection_text, scoped_query, tenant_token
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from tests.unit.test_adaptive_retrieval import _canonical_claim, _cfg


def test_rrf_deduplicates_filters_before_ranking_and_breaks_ties_stably():
    result = reciprocal_rank_fusion(
        {
            "bm25": [{"memory_id": x} for x in ["foreign", "b", "b", "a"]],
            "vector": [{"memory_id": x} for x in ["a", "b"]],
        },
        {"a", "b"},
    )
    assert list(result) == ["a", "b"]
    assert result["a"] == pytest.approx(1 / 61 + 1 / 62)
    assert reciprocal_rank_fusion({"bm25": [{"memory_id": "a"}]}, {"a"}) == {"a": 1 / 61}


def test_lucene_query_owns_scope_and_has_no_user_operators():
    query = scoped_query('x" OR search_tenant:* \\ AND (secret)^9', "private:tenant")
    assert query.startswith(
        f"search_tenant:{tenant_token('private:tenant')} AND search_state:active"
    )
    assert "*" not in query and "^" not in query and "\\" not in query
    assert "private:tenant" not in query
    assert scoped_query("---", "a") is None
    with pytest.raises(ValueError):
        scoped_query("a", "")


def test_channel_failure_preserves_success_and_does_not_leak_exception_text():
    def unavailable():
        raise ValueError("private query content")

    result = discover(
        {"bm25": lambda: [{"memory_id": "a", "score": 2}], "vector": unavailable},
        timeout=1,
        limit=2,
    )
    assert result.status == {"bm25": "ok", "vector": "unavailable"}
    assert result.candidates() == [{"memory_id": "a"}]


def test_discovery_deadline_does_not_wait_for_slow_channel():
    release = threading.Event()

    def slow():
        release.wait(1)
        return []

    start = time.monotonic()
    try:
        result = discover({"vector": slow}, timeout=0.02, limit=2)
        assert time.monotonic() - start < 0.5
        assert result.status == {"vector": "timeout"}
    finally:
        release.set()


def test_bm25_works_without_embedding_but_requires_canonical_hydration():
    graph = InMemoryKnowledgeGraph()
    graph.upsert_memory_projection(
        memory_id="memory-angie",
        revision=4,
        tenant_id="_default",
        properties={"search_text": "Angie Director"},
    )

    class Repository:
        def hydrate(self, *, candidates, **kwargs):
            assert candidates[0].memory_id == "memory-angie"
            return [_canonical_claim()]

    ctx = OrchestratorContext(cfg=_cfg(), neo4j=graph, memory_repository=Repository(), embed=None)
    diagnostics = {}
    rows, count = _retrieve_route(
        ctx,
        ctx.repository(),
        RetrievalPlan(query="Angie Director"),
        RetrievalRoute.L2,
        tenant_id="_default",
        diagnostics=diagnostics,
    )
    assert count == 1 and rows[0].canonical
    assert rows[0].retrieval_score == pytest.approx(1 / 61)
    assert diagnostics["retrievers"] == {"bm25": "ok", "vector": "unsupported"}

    class DeletedRepository(Repository):
        def hydrate(self, **kwargs):
            return [_canonical_claim(status="DELETED")]

    ctx.memory_repository = DeletedRepository()
    rows, _ = _retrieve_route(
        ctx,
        ctx.repository(),
        RetrievalPlan(query="Angie Director"),
        RetrievalRoute.L2,
        tenant_id="_default",
    )
    assert rows == []


def test_in_memory_bm25_uses_term_frequency_and_length_and_is_scoped():
    graph = InMemoryKnowledgeGraph()
    for identity, text, tenant in [
        ("short", "quasar", "a"),
        ("long", "quasar " + "ordinary " * 50, "a"),
        ("foreign", "quasar " * 20, "b"),
    ]:
        graph.upsert_memory_projection(
            memory_id=identity, revision=1, tenant_id=tenant, properties={"search_text": text}
        )
    assert [r["memory_id"] for r in graph.bm25_search("quasar", tenant_id="a")] == ["short", "long"]
    graph.delete_memory_projection(memory_id="short", revision=2, tenant_id="a")
    graph.upsert_memory_projection(
        memory_id="short", revision=1, tenant_id="a", properties={"search_text": "quasar"}
    )
    assert [r["memory_id"] for r in graph.bm25_search("quasar", tenant_id="a")] == ["long"]


def test_projection_text_contains_current_claims_and_excludes_retired_claims():
    text = projection_text(
        {
            "node_properties": {"canonical_name": "Angie"},
            "claims": [
                {"status": "ACTIVE", "predicate": "HAS_ROLE", "object_value": "Director"},
                {"status": "SUPERSEDED", "predicate": "HAS_ROLE", "object_value": "Intern"},
            ],
        }
    )
    assert "Director" in text and "Intern" not in text


def test_canonical_overview_prompt_handles_entity_and_literal_claims():
    common = {"claim_id": "c", "subject_id": "alice", "predicate": "WORKS_AT", "status": "ACTIVE"}
    prompt = prompts.render(
        "canonical_overview",
        directory_uri="mem://p",
        children_abstracts=[],
        children_relations=[
            {**common, "object_entity_id": "atlas", "object_value": None},
            {**common, "object_entity_id": None, "object_value": "Director"},
        ],
        overview_max_tokens=100,
    )
    assert "atlas" in prompt and "Director" in prompt and "claim_id=c" in prompt


def test_multiple_cardinality_is_not_an_inferred_conflict():
    rows = [
        VerifiedEvidence.model_validate(
            _canonical_claim(predicate="MEMBER_OF", object_value=value, claim_id=value)
        )
        for value in ("Engineering", "Platform")
    ]
    result = EvidenceGate().assess(
        RetrievalPlan(query="teams", requires_conflict_check=True, predicate_hint="MEMBER_OF"), rows
    )
    assert not result.conflicts and result.state.value == "ANSWERABLE"


def test_no_answerable_state_when_confidence_filter_removes_every_claim():
    result = EvidenceGate(minimum_confidence=0.99).assess(
        RetrievalPlan(query="role", predicate_hint="HAS_ROLE"), [_canonical_claim()]
    )
    assert result.state.value == "INSUFFICIENT_EVIDENCE" and not result.verified_evidence
