"""Unit tests for the soft memory decay formula (§10.1)."""

import math

import pytest

from engram.config import DecayConfig
from engram.decay import PRESETS, compute_weight, run_daily


def test_fresh_node_scores_near_one():
    preset = PRESETS["personal_conversation"]
    w = compute_weight(
        preset=preset,
        days_since_last_access=0.0,
        access_count=10,
        p95_access_count=10,
        relates_to_degree=5,
        p95_relates_to_degree=5,
    )
    # alpha + beta + gamma = 1.0 at full recency, saturated frequency and centrality.
    assert 0.99 <= w <= 1.001


def test_old_cold_node_scores_near_zero():
    preset = PRESETS["coding_agent"]
    w = compute_weight(
        preset=preset,
        days_since_last_access=365.0,
        access_count=0,
        p95_access_count=10,
        relates_to_degree=0,
        p95_relates_to_degree=5,
    )
    # Recency ~ e^(-0.05*365) ≈ 0, frequency/centrality = 0, so w ≈ 0.
    assert w < 0.05


def test_percentile_normalisation_caps_at_one():
    preset = PRESETS["knowledge_base"]
    # An outlier with 1000 accesses vs p95 of 10 should not blow up frequency.
    w = compute_weight(
        preset=preset,
        days_since_last_access=0,
        access_count=1000,
        p95_access_count=10,
        relates_to_degree=100,
        p95_relates_to_degree=5,
    )
    assert w <= 1.0


def test_half_life_matches_spec():
    """SDD §10.2 lists ~69 days for personal_conversation; verify order of magnitude."""
    preset = PRESETS["personal_conversation"]
    # After one half-life, recency contribution should be ~0.5 x alpha.
    half_life_days = math.log(2) / preset.half_life_lambda
    assert 60 < half_life_days < 80


def test_daily_decay_updates_only_the_matching_tenant_node():
    class RecordingGraph:
        def __init__(self):
            self.calls = []
            self.batch_calls = 0

        def run_template(self, query, params):
            self.calls.append((query, params))
            if "access_count, 0" in query:
                return [{"v": 1}]
            if "WITH n, count(r) AS v" in query:
                return [{"v": 1}]
            if "RETURN n.tenant_id AS tenant_id" in query:
                self.batch_calls += 1
                if self.batch_calls == 1:
                    return [
                        {
                            "tenant_id": "acme",
                            "source_uri": "mem://user/entities/alice.md",
                            "last_accessed_at": None,
                            "access_count": 1,
                            "degree": 1,
                        }
                    ]
                return []
            return []

    graph = RecordingGraph()
    assert run_daily(graph, DecayConfig(), strict=True) == 1
    write_calls = [call for call in graph.calls if "SET n.retrieval_weight" in call[0]]
    assert write_calls[0][1]["tenant_id"] == "acme"
    assert "tenant_id: $tenant_id" in write_calls[0][0]


def test_strict_daily_decay_surfaces_graph_failures():
    class BrokenGraph:
        def run_template(self, _query, _params):
            raise RuntimeError("neo4j unavailable")

    with pytest.raises(RuntimeError, match="neo4j unavailable"):
        run_daily(BrokenGraph(), DecayConfig(), strict=True)
