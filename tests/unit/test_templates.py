import pytest

from engram.retrieval import templates
from engram.retrieval.orchestrator import _normalize_template_params
from engram.storage.memory_kg import InMemoryKnowledgeGraph


def test_available_templates_cover_spec():
    """§16.3 lists 8 Cypher template files. Ensure we have them."""
    expected = {
        "t_children_of",
        "t_neighbours_by_relation",
        "t_path_between",
        "t_temporal_filter",
        "t_history_chain",
        "t_find_by_uri_prefix",
        "t_cross_references",
        "t_top_k_vector",
    }
    assert set(templates.available_templates()) == expected


def test_unknown_template_raises():
    with pytest.raises(templates.TemplateError):
        templates.run_template(None, "t_evil", {})  # type: ignore[arg-type]


def test_missing_required_params_raises():
    with pytest.raises(templates.TemplateError) as excinfo:
        templates.run_template(None, "t_children_of", {"limit": 5})  # type: ignore[arg-type]
    assert "uri" in str(excinfo.value)


def test_hops_clamp_applied():
    class DummyNeo:
        def run_template(self, cypher, params, timeout_s=None):
            return [params]

    out = templates.run_template(
        DummyNeo(),  # type: ignore[arg-type]
        "t_neighbours_by_relation",
        {"node_uri": "mem://x", "hops": 99, "relation": "works_at"},
    )
    assert out[0]["hops"] == 4


def test_planner_parameter_aliases_normalize_to_template_contract() -> None:
    assert _normalize_template_params(
        "t_neighbours_by_relation",
        {"node_id": "mem://alice", "relation": "works_at"},
    )["node_uri"] == "mem://alice"
    assert _normalize_template_params(
        "t_path_between", {"start_node": "mem://a", "end_node": "mem://b"}
    ) == {
        "start_node": "mem://a",
        "end_node": "mem://b",
        "src_uri": "mem://a",
        "dst_uri": "mem://b",
    }
    alternate = _normalize_template_params(
        "t_path_between", {"from_uri": "mem://a", "target_uri": "mem://b"}
    )
    assert alternate["src_uri"] == "mem://a"
    assert alternate["dst_uri"] == "mem://b"


def test_cross_references_are_bidirectional_for_source_provenance() -> None:
    graph = InMemoryKnowledgeGraph()
    episode_uri = "mem://user/episodes/event-1.md"
    fact_uri = "mem://user/facts/event-1/0.md"
    entity_uri = "mem://user/entities/alice/alice.md"
    for uri, node_type in (
        (episode_uri, "DOCUMENT"),
        (fact_uri, "FACT"),
        (entity_uri, "ENTITY"),
    ):
        graph.merge_node(
            source_uri=uri,
            properties={"node_type": node_type, "status": "ACTIVE"},
        )
    graph.merge_edge(
        subject_uri=episode_uri,
        object_uri=fact_uri,
        relation_label="assertion",
        edge_type="REFERENCES",
    )
    graph.merge_edge(
        subject_uri=fact_uri,
        object_uri=entity_uri,
        relation_label="subject",
        edge_type="REFERENCES",
    )

    entity_adjacent = templates.run_template(
        graph, "t_cross_references", {"node_uri": entity_uri}
    )
    fact_adjacent = templates.run_template(
        graph, "t_cross_references", {"node_uri": fact_uri}
    )

    assert [row["source_uri"] for row in entity_adjacent] == [fact_uri]
    assert {row["source_uri"] for row in fact_adjacent} == {episode_uri, entity_uri}
