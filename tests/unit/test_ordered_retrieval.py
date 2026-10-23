"""Behavioral tests for stage ordering, early exits and source trust."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from engram.models.frontier import FrontierVerdict
from engram.retrieval.cascade import parametric_allowed
from engram.retrieval.domain import VerifiedEvidence
from engram.retrieval.orchestrator import run_query
from engram.retrieval.planner import AdaptivePlanner
from engram.retrieval.tools import ConversationContext, MemoryTools, _pack_records, lexical_query
from tests.unit.test_adaptive_retrieval import (
    DirectRepository,
    RecordingFrontier,
    _canonical_claim,
    _context,
)


def test_general_question_exits_before_any_memory_tool(monkeypatch):
    repo = DirectRepository()
    context = _context(repo, RecordingFrontier())
    monkeypatch.setattr(MemoryTools, "current_session_search", Mock(side_effect=AssertionError))
    result = run_query(context, session_id=None, query="What is a binary search tree?")
    assert result.retrieval_metadata.levels_visited == ["L0"]
    assert result.retrieval_metadata.answer_mode == "parametric"
    assert repo.calls == []
    assert result.verified_evidence == []


@pytest.mark.parametrize(
    "query",
    [
        "What is my name?",
        "Who is Alice?",
        "What is today's price?",
        "What did we decide?",
        "What about that?",
    ],
)
def test_private_or_contextual_question_cannot_exit_parametrically(query):
    assert not parametric_allowed(query, AdaptivePlanner().plan(query))


def test_session_answer_does_not_search_past_or_canonical_memory(monkeypatch):
    repo = DirectRepository()
    context = _context(repo, RecordingFrontier())
    past = Mock(side_effect=AssertionError)
    monkeypatch.setattr(MemoryTools, "past_session_search", past)
    result = run_query(
        context,
        session_id="active",
        query="What name did I just give you?",
        session_context="USER: My name is Ravi.",
    )
    assert result.retrieval_metadata.levels_visited == ["L0", "L1"]
    assert result.retrieval_metadata.answer_mode == "current-session"
    assert result.verified_evidence == []
    assert not repo.calls and not past.called


def test_current_session_cache_failure_falls_back_to_postgres():
    context = _context(DirectRepository(), RecordingFrontier())
    context.session_cache = SimpleNamespace(get=Mock(side_effect=ConnectionError))
    record = {
        "event_id": "evt-1",
        "session_id": "active",
        "payload": {
            "turn_pair": {
                "user": {"content": "My name is Ravi"},
                "assistant": {"content": "Hello"},
            }
        },
    }
    context.control_plane = SimpleNamespace(list_session_events=Mock(return_value=[record]))
    result = run_query(context, session_id="active", query="What is my name?")
    assert result.retrieval_metadata.answer_mode == "current-session"
    assert result.retrieval_metadata.source_records[0]["event_id"] == "evt-1"
    context.control_plane.list_session_events.assert_called_once_with(
        "active", tenant_id="_default", limit=100
    )


def test_past_session_answer_stops_before_facts(monkeypatch):
    repo = DirectRepository()
    context = _context(repo, RecordingFrontier())
    monkeypatch.setattr(
        MemoryTools,
        "past_session_search",
        lambda *_: ConversationContext(
            text="USER in past-session: We chose PostgreSQL for Atlas.",
            sources=[{"event_id": "evt-old", "source_type": "conversation_record"}],
        ),
    )
    result = run_query(context, session_id=None, query="What did we decide for Atlas?")
    assert result.retrieval_metadata.levels_visited == ["L0", "L1", "L2"]
    assert result.retrieval_metadata.source_records[0]["event_id"] == "evt-old"
    assert not repo.calls


def test_current_fact_does_not_exit_on_an_old_session_match(monkeypatch):
    monkeypatch.setattr(
        MemoryTools,
        "past_session_search",
        lambda *_: ConversationContext(
            text="2020 USER: Angie was an intern.",
        ),
    )
    result = run_query(
        _context(DirectRepository(), RecordingFrontier()),
        session_id=None,
        query="What is Angie's current role?",
    )
    assert result.retrieval_metadata.retrieval_route == "L3_FACTS"
    assert result.verified_evidence[0].object_value == "Director"


def test_all_stages_execute_in_order_and_model_need_more_reaches_graph(monkeypatch):
    calls = []
    context = _context(DirectRepository(), RecordingFrontier())
    evidence = VerifiedEvidence.model_validate(_canonical_claim())
    for name in ("current_session_search", "past_session_search"):

        def conversation(*_, name=name):
            calls.append(name)
            return ConversationContext(text="A relevant conversation")

        monkeypatch.setattr(MemoryTools, name, conversation)
    for name in ("facts_search", "semantic_memory_search", "graph_search"):

        def knowledge(*_, name=name):
            calls.append(name)
            return [evidence], 1

        monkeypatch.setattr(MemoryTools, name, knowledge)
    context.frontier.answer = Mock(
        side_effect=[
            FrontierVerdict("NEED_MORE"),
            FrontierVerdict("NEED_MORE"),
            FrontierVerdict("NEED_MORE"),
            FrontierVerdict("NEED_MORE"),
            FrontierVerdict("ANSWER", answer="Angie connects to Atlas."),
        ]
    )
    result = run_query(context, session_id=None, query="Who is connected to Angie?")
    assert calls == [
        "current_session_search",
        "past_session_search",
        "facts_search",
        "semantic_memory_search",
        "graph_search",
    ]
    assert result.retrieval_metadata.levels_visited == [
        "L0",
        "L1",
        "L2",
        "L3_FACTS",
        "L3_VECTOR",
        "L3_GRAPH",
    ]
    assert result.answer == "Angie connects to Atlas."


@pytest.mark.parametrize(
    ("depth", "expected"),
    [("L0", ["L0"]), ("SESSION", ["L0", "L1"]), ("L1", ["L0", "L1"]), ("L2", ["L0", "L1", "L2"])],
)
def test_max_depth_prevents_deeper_reads(depth, expected):
    repo = DirectRepository()
    result = run_query(
        _context(repo, RecordingFrontier()),
        session_id=None,
        query="What is Angie's current role?",
        max_depth=depth,
    )
    assert result.retrieval_metadata.levels_visited == expected
    assert result.retrieval_metadata.stop_reason == "max-depth-reached"
    assert not repo.calls


def test_oversized_conversation_record_is_not_partially_serialized():
    records = [
        {
            "event_id": "long",
            "payload": {
                "turn_pair": {
                    "user": {"content": "large " * 1000},
                }
            },
        },
        {"event_id": "short", "payload": {"turn_pair": {"user": {"content": "coffee"}}}},
    ]
    result = _pack_records(records, 150)
    assert len(result.sources) == 1 and result.sources[0]["event_id"] == "short"
    assert "coffee" in result.text and "large" not in result.text


def test_lexical_query_removes_tsquery_operators():
    assert lexical_query("What did we decide about Atlas? '); DROP TABLE events; --") == (
        "decide | atlas | drop | table | events"
    )


def test_graph_path_is_rejected_when_a_canonical_edge_is_missing(monkeypatch):
    from engram.retrieval import orchestrator, tools

    context = _context(
        DirectRepository(), RecordingFrontier(), neo=SimpleNamespace(run_template=Mock())
    )
    context.memory_repository.resolve_aliases = Mock()
    monkeypatch.setattr(orchestrator, "_resolve_subject_ids", lambda *_, **__: ["alice"])
    monkeypatch.setattr(
        tools,
        "run_template",
        lambda *_, **__: [
            {
                "claims": [
                    {"claim_id": "edge-a"},
                    {"claim_id": "edge-b"},
                ]
            }
        ],
    )
    monkeypatch.setattr(
        orchestrator,
        "_repository_hydrate",
        lambda *_, **__: [
            _canonical_claim(claim_id="edge-a"),
        ],
    )
    rows, count = MemoryTools(context, tenant_id="_default", session_id=None).graph_search(
        AdaptivePlanner().plan("Who is connected to Alice?")
    )
    assert count == 2 and rows == []
