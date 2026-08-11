import pytest

from engram.retrieval import templates
from engram.retrieval.orchestrator import _normalize_template_params


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
