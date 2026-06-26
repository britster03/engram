"""Tests for the rule-based conflict classifier (§6.5)."""

from __future__ import annotations

from engram.ingest.conflict import ConflictDecision, apply_decision, classify
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
    assert "$tenant_id" in neo.queries[0][0]


def test_contradiction_triggers_supersession_plan():
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
    assert decision.case == "CONTRADICTION"
    assert decision.existing_edge_id == 42


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
        decision=ConflictDecision("DUPLICATE", existing_edge_id=7, reason="dup"),
        subject_uri="mem://a",
        object_uri="mem://b",
        relation_label="works_at",
    )
    assert "touch" in calls
    assert all(c != "merge:RELATES_TO:works_at" for c in calls)
    calls.clear()
    # CONTRADICTION → supersede old, merge new
    apply_decision(
        neo4j=neo,  # type: ignore[arg-type]
        decision=ConflictDecision("CONTRADICTION", existing_edge_id=7, reason="diff"),
        subject_uri="mem://a",
        object_uri="mem://c",
        relation_label="works_at",
    )
    assert "supersede" in calls
    assert "merge:RELATES_TO:works_at" in calls
    assert all("$tenant_id" in query for query in neo.queries)
