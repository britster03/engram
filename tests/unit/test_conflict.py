"""Tests for the rule-based conflict classifier (§6.5)."""

from __future__ import annotations

import pytest

from engram.ingest.conflict import (
    ConflictDecision,
    apply_decision,
    classify,
    restore_decision,
)
from engram.models.core import CompletionResult, CoreModelProvider
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from tests.integration.providers import DeterministicEmbeddingService


class StaticNeo(InMemoryKnowledgeGraph):
    """Override run_template to return a hand-rolled edge list."""

    def __init__(self, edges: list[dict]) -> None:
        super().__init__()
        self._edges = edges
        self.queries: list[tuple[str, dict]] = []

    def run_template(self, cypher, params, timeout_s=None):
        self.queries.append((cypher, params))
        if "RELATES_TO" in cypher and "RETURN elementId(r)" in cypher:
            return list(self._edges)
        return []


def test_no_existing_edges_is_coexistence():
    neo = StaticNeo(edges=[])
    embed = DeterministicEmbeddingService()
    decision = classify(
        neo4j=neo,  # type: ignore[arg-type]
        embed=embed,  # type: ignore[arg-type]
        subject_uri="mem://user/entities/alice/alice.md",
        relation_label="works_at",
        object_uri="mem://user/entities/meta/meta.md",
        object_abstract="Meta",
    )
    assert decision.case == "CO_EXISTENCE"


def test_duplicate_short_circuits():
    edges = [
        {
            "edge_id": 1,
            "relation_label": "works_at",
            "object_uri": "mem://user/entities/meta/meta.md",
            "object_abstract": "Meta",
            "assertion_uri": "mem://user/facts/event-a/0_works-at_meta.md",
        }
    ]
    neo = StaticNeo(edges=edges)
    embed = DeterministicEmbeddingService()
    decision = classify(
        neo4j=neo,  # type: ignore[arg-type]
        embed=embed,  # type: ignore[arg-type]
        subject_uri="mem://user/entities/alice/alice.md",
        relation_label="works_at",
        object_uri="mem://user/entities/meta/meta.md",
        object_abstract="Meta",
    )
    assert decision.case == "DUPLICATE"
    assert decision.existing_edge_id == 1
    assert decision.existing_assertion_uri == "mem://user/facts/event-a/0_works-at_meta.md"
    assert "$tenant_id" in neo.queries[0][0]


def test_different_objects_coexist_without_explicit_correction():
    edges = [
        {
            "edge_id": 42,
            "relation_label": "works_at",
            "object_uri": "mem://user/entities/google/google.md",
            "object_abstract": "Google",
        }
    ]
    neo = StaticNeo(edges=edges)
    embed = DeterministicEmbeddingService()
    decision = classify(
        neo4j=neo,  # type: ignore[arg-type]
        embed=embed,  # type: ignore[arg-type]
        subject_uri="mem://user/entities/alice/alice.md",
        relation_label="works_at",
        object_uri="mem://user/entities/meta/meta.md",
        object_abstract="Meta",
    )
    assert decision.case == "CO_EXISTENCE"
    assert decision.existing_edge_id is None
    assert "no explicit correction" in decision.reason

    correction = classify(
        neo4j=neo,  # type: ignore[arg-type]
        embed=embed,  # type: ignore[arg-type]
        subject_uri="mem://user/entities/alice/alice.md",
        relation_label="works_at",
        object_uri="mem://user/entities/meta/meta.md",
        object_abstract="Meta",
        allow_contradiction=True,
    )
    assert correction.case == "CONTRADICTION"
    assert correction.existing_edge_id == 42


def test_duplicate_searches_all_same_relation_edges():
    edges = [
        {
            "edge_id": "different",
            "relation_label": "works_at",
            "object_uri": "mem://user/entities/google/google.md",
            "object_abstract": "Google",
        },
        {
            "edge_id": "duplicate",
            "relation_label": "works_at",
            "object_uri": "mem://user/entities/meta/meta.md",
            "object_abstract": "Meta",
            "assertion_uri": "mem://user/facts/event-a/0_works-at_meta.md",
        },
    ]
    neo = StaticNeo(edges=edges)
    decision = classify(
        neo4j=neo,  # type: ignore[arg-type]
        embed=DeterministicEmbeddingService(),  # type: ignore[arg-type]
        subject_uri="mem://user/entities/alice/alice.md",
        relation_label="works_at",
        object_uri="mem://user/entities/meta/meta.md",
        object_abstract="Meta",
    )

    assert decision.case == "DUPLICATE"
    assert decision.existing_edge_id == "duplicate"
    assert decision.existing_assertion_uri == "mem://user/facts/event-a/0_works-at_meta.md"


class AmbiguousEmbedding:
    def embed(self, text: str) -> list[float]:
        if text in {"works_at", "Meta"}:
            return [1.0, 0.0]
        return [0.7, 0.7]


class ContradictionCore(CoreModelProvider):
    def complete(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        output_schema=None,
        max_tokens=None,
        temperature=None,
    ) -> CompletionResult:
        return CompletionResult(
            output={
                "case": "CONTRADICTION",
                "existing_edge_id": "42",
                "reason": "the user explicitly corrected the earlier value",
            },
            raw_text="",
        )


def test_core_contradiction_requires_explicit_correction_evidence():
    edges = [
        {
            "edge_id": "42",
            "relation_label": "works_at",
            "object_uri": "mem://user/entities/google/google.md",
            "object_abstract": "Google",
            "assertion_uri": "mem://user/facts/old/0_works-at_google.md",
        }
    ]
    neo = StaticNeo(edges=edges)
    kwargs = {
        "neo4j": neo,
        "embed": AmbiguousEmbedding(),
        "subject_uri": "mem://user/entities/alice/alice.md",
        "relation_label": "works_at",
        "object_uri": "mem://user/entities/meta/meta.md",
        "object_abstract": "Meta",
        "core": ContradictionCore(),
    }

    safe = classify(**kwargs)  # type: ignore[arg-type]
    assert safe.case == "CO_EXISTENCE"
    assert "without explicit correction" in safe.reason

    correction = classify(**kwargs, allow_contradiction=True)  # type: ignore[arg-type]
    assert correction.case == "CONTRADICTION"
    assert correction.existing_edge_id == "42"
    assert correction.existing_assertion_uri == "mem://user/facts/old/0_works-at_google.md"


def test_restore_decision_resolves_runtime_edge_from_stable_assertion_uri() -> None:
    target = "mem://user/facts/old/0_works-at_google.md"
    neo = StaticNeo(
        edges=[
            {
                "edge_id": "deployment-local-42",
                "relation_label": "works_at",
                "object_uri": "mem://user/entities/google/google.md",
                "object_abstract": "Google",
                "assertion_uri": target,
            }
        ]
    )

    decision = restore_decision(
        neo4j=neo,  # type: ignore[arg-type]
        subject_uri="mem://user/entities/alice/alice.md",
        persisted={
            "case": "CONTRADICTION",
            "existing_assertion_uri": target,
            "reason": "explicit correction",
        },
    )

    assert decision == ConflictDecision(
        "CONTRADICTION",
        "deployment-local-42",
        "explicit correction",
        existing_assertion_uri=target,
    )


def test_restore_decision_fails_closed_when_contradiction_target_is_missing() -> None:
    with pytest.raises(RuntimeError, match="contradiction target is unavailable"):
        restore_decision(
            neo4j=StaticNeo(edges=[]),  # type: ignore[arg-type]
            subject_uri="mem://user/entities/alice/alice.md",
            persisted={
                "case": "CONTRADICTION",
                "existing_assertion_uri": "mem://user/facts/missing.md",
            },
        )


def test_apply_decision_for_duplicate_noop_and_contradiction_supersedes():
    """apply_decision makes the correct neo4j mutations."""
    calls: list[str] = []

    class RecordingNeo(InMemoryKnowledgeGraph):
        def __init__(self):
            super().__init__()
            self.queries: list[str] = []

        def merge_edge(self, **kwargs):
            calls.append(f"merge:{kwargs['edge_type']}:{kwargs['relation_label']}")
            super().merge_edge(**kwargs)

        def run_template(self, cypher, params, timeout_s=None):
            self.queries.append(cypher)
            if "SET r.status = 'HISTORICAL'" in cypher:
                calls.append("supersede")
            if "SET r.last_accessed_at" in cypher:
                calls.append("touch")
            return []

    neo = RecordingNeo()
    # DUPLICATE → only a touch, no new edge
    apply_decision(
        neo4j=neo,  # type: ignore[arg-type]
        decision=ConflictDecision(
            "DUPLICATE",
            existing_edge_id=7,
            reason="dup",
            existing_assertion_uri="mem://facts/old",
        ),
        subject_uri="mem://a",
        object_uri="mem://b",
        relation_label="works_at",
        incoming_assertion_uri="mem://facts/new",
    )
    assert "touch" in calls
    assert "merge:DUPLICATE_OF:duplicate_of" in calls
    assert all(c != "merge:RELATES_TO:works_at" for c in calls)
    calls.clear()
    # CONTRADICTION → supersede old, merge new
    apply_decision(
        neo4j=neo,  # type: ignore[arg-type]
        decision=ConflictDecision(
            "CONTRADICTION",
            existing_edge_id=7,
            reason="diff",
            existing_assertion_uri="mem://facts/old",
        ),
        subject_uri="mem://a",
        object_uri="mem://c",
        relation_label="works_at",
        incoming_assertion_uri="mem://facts/new",
    )
    assert "supersede" in calls
    assert "merge:RELATES_TO:works_at" in calls
    assert "merge:SUPERSEDES:supersedes" in calls
    assert all("$tenant_id" in query for query in neo.queries)
