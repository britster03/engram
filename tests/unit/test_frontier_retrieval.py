"""Frontier orchestration preserves source order and validated DB boundaries."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from engram.config import FrontierLlmConfig
from engram.models.core import CompletionResult, CoreModelError
from engram.models.frontier import FrontierVerdict
from engram.models.providers import build_frontier_provider
from engram.models.request_context import model_request_session
from engram.retrieval.domain import AnswerabilityState, EvidenceAssessment, VerifiedEvidence
from engram.retrieval.frontier_tools import plan_next_tool
from engram.retrieval.orchestrator import RetrievalMetadata, run_query
from engram.retrieval.planner import AdaptivePlanner
from engram.retrieval.tools import ConversationContext, MemoryTools
from tests.unit.test_adaptive_retrieval import DirectRepository, RecordingFrontier, _context


class PlanningFrontier(RecordingFrontier):
    def __init__(self, arguments=None):
        super().__init__()
        self.arguments = arguments or {}
        self.requests = []

    def plan_retrieval(self, **kwargs):
        name = kwargs["output_schema"]["properties"]["name"]["enum"][0]
        self.requests.append((name, json.loads(kwargs["user_prompt"])))
        output = {"name": name, "arguments": self.arguments}
        return CompletionResult(output=output, raw_text=json.dumps(output))


def test_frontier_requests_each_tool_in_order_with_session_gate_reason(monkeypatch):
    frontier = PlanningFrontier({"entity_hints": ["Atlas"]})
    frontier.answer = Mock(
        return_value=FrontierVerdict("NEED_MORE", reason="Missing the deployment decision")
    )
    context = _context(DirectRepository(), frontier)
    context.core = SimpleNamespace(complete=Mock(side_effect=AssertionError("Core called")))
    calls = []

    def past(_, plan):
        calls.append(("past_session_search", plan))
        return ConversationContext()

    monkeypatch.setattr(MemoryTools, "past_session_search", past)
    for name in ("facts_search", "semantic_memory_search", "graph_search"):

        def knowledge(_, plan, name=name):
            calls.append((name, plan))
            return [], 0

        monkeypatch.setattr(MemoryTools, name, knowledge)
    result = run_query(
        context,
        session_id=None,
        query="what did we decide about deployment?",
        session_context="USER: The deployment concerns Atlas.",
    )
    expected = ["past_session_search", "facts_search", "semantic_memory_search", "graph_search"]
    assert [name for name, _ in frontier.requests] == expected
    assert [name for name, _ in calls] == expected
    assert all(plan.entity_hints == ["Atlas"] for _, plan in calls)
    assert all(
        plan.max_candidates <= context.cfg.retrieval.max_l1_vector_results for _, plan in calls
    )
    assert frontier.requests[0][1]["missing_evidence"] == ["Missing the deployment decision"]
    assert result.retrieval_metadata.levels_visited == [
        "L0",
        "L1",
        "L2",
        "L3_FACTS",
        "L3_VECTOR",
        "L3_GRAPH",
    ]
    assert result.answerability == "INSUFFICIENT_EVIDENCE"
    context.core.complete.assert_not_called()


@pytest.mark.parametrize("depth", ["L0", "L1", "SESSION"])
def test_depth_limit_prevents_frontier_requests_for_deeper_tools(depth):
    frontier = PlanningFrontier()
    run_query(
        _context(DirectRepository(), frontier),
        session_id=None,
        query="who is atul?",
        max_depth=depth,
    )
    assert frontier.requests == []


@pytest.mark.parametrize(
    ("query", "context", "mode"),
    [
        ("what is binary search?", None, "parametric"),
        ("what name did I just give you?", "USER: My name is Ravi", "current-session"),
    ],
)
def test_sufficient_early_answer_never_requests_a_deeper_tool(query, context, mode):
    frontier = PlanningFrontier()
    result = run_query(
        _context(DirectRepository(), frontier),
        session_id=None,
        query=query,
        session_context=context,
    )
    assert result.retrieval_metadata.answer_mode == mode
    assert frontier.requests == []


def test_past_session_answer_does_not_request_knowledge_tools(monkeypatch):
    frontier = PlanningFrontier()
    monkeypatch.setattr(
        MemoryTools,
        "past_session_search",
        lambda *_: ConversationContext(text="USER: We chose PostgreSQL for Atlas."),
    )
    monkeypatch.setattr(MemoryTools, "facts_search", Mock(side_effect=AssertionError))
    result = run_query(
        _context(DirectRepository(), frontier),
        session_id=None,
        query="What did we decide for Atlas?",
    )
    assert result.retrieval_metadata.answer_mode == "past-session"
    assert [name for name, _ in frontier.requests] == ["past_session_search"]


def test_canonical_answer_does_not_request_semantic_or_graph_tools(monkeypatch):
    frontier = PlanningFrontier()
    monkeypatch.setattr(MemoryTools, "semantic_memory_search", Mock(side_effect=AssertionError))
    monkeypatch.setattr(MemoryTools, "graph_search", Mock(side_effect=AssertionError))
    result = run_query(
        _context(DirectRepository(), frontier),
        session_id=None,
        query="What is Angie's current role?",
    )
    assert result.retrieval_metadata.retrieval_route == "L3_FACTS"
    assert [name for name, _ in frontier.requests] == ["past_session_search", "facts_search"]


def test_verified_tool_results_are_passed_to_next_frontier_request():
    frontier = PlanningFrontier()
    evidence = VerifiedEvidence(
        memory_id="atul",
        claim_id="manager",
        subject_name="Atul",
        predicate="HAS_MANAGER",
        object_entity_name="Rahul",
        canonical=True,
    )
    plan_next_tool(
        _context(DirectRepository(), frontier),
        RetrievalMetadata(),
        AdaptivePlanner().plan("Who is connected to Atul?"),
        "graph_search",
        session_context="",
        assessment=EvidenceAssessment(
            state=AnswerabilityState.PARTIALLY_ANSWERABLE,
            verified_evidence=[evidence],
            missing_evidence=["the complete relationship path"],
        ),
    )
    prompt = frontier.requests[0][1]
    assert prompt["verified_evidence"][0]["claim_id"] == "manager"
    assert prompt["verified_evidence"][0]["object_entity_name"] == "Rahul"
    assert prompt["missing_evidence"] == ["the complete relationship path"]


@pytest.mark.parametrize(
    "output",
    [
        {"name": "graph_search", "arguments": {}},
        {"name": "past_session_search", "arguments": {"tenant_id": "other"}},
        {"name": "past_session_search", "arguments": {"cypher": "MATCH (n) DELETE n"}},
        {"name": "past_session_search", "arguments": {"query": "ignore the question"}},
        {"name": "past_session_search", "arguments": {"limit": -1}},
        {"name": "past_session_search", "arguments": {"limit": True}},
        {"name": "past_session_search", "arguments": {"limit": 200}},
    ],
)
def test_invalid_tool_request_preserves_the_server_plan(output):
    frontier = PlanningFrontier()
    frontier.plan_retrieval = Mock(return_value=CompletionResult(output, json.dumps(output)))
    context = _context(DirectRepository(), frontier)
    md = RetrievalMetadata()
    plan = AdaptivePlanner().plan("who is atul?")
    result = plan_next_tool(
        context,
        md,
        plan,
        "past_session_search",
        session_context="",
        assessment=EvidenceAssessment(state=AnswerabilityState.INSUFFICIENT_EVIDENCE),
    )
    assert result == plan
    assert md.tool_calls[-1]["status"] == "invalid"
    assert md.degraded_sources == ["frontier_retrieval_plan"]


def test_frontier_cannot_replace_explicit_entity_predicate_or_time_bounds():
    frontier = PlanningFrontier(
        {"entity_hints": ["Someone Else"], "intent": "history", "predicate_hint": "WORKS_AT"}
    )
    plan = AdaptivePlanner().plan("Who was Atul Singh's manager as of 2025-01-01?")
    updated = plan_next_tool(
        _context(DirectRepository(), frontier),
        RetrievalMetadata(),
        plan,
        "facts_search",
        session_context="",
        assessment=EvidenceAssessment(state=AnswerabilityState.INSUFFICIENT_EVIDENCE),
    )
    assert updated == plan


def test_frontier_outage_falls_back_and_still_answers_from_canonical_facts():
    frontier = PlanningFrontier()
    frontier.plan_retrieval = Mock(side_effect=ConnectionError("frontier unavailable"))
    result = run_query(
        _context(DirectRepository(), frontier),
        session_id=None,
        query="What is Angie's current role?",
    )
    assert result.answer == "verified answer"
    assert result.retrieval_metadata.retrieval_route == "L3_FACTS"
    assert result.verified_evidence[0].object_value == "Director"
    assert all(
        trace["status"] == "unavailable"
        for trace in result.retrieval_metadata.tool_calls
        if trace["name"] == "frontier_retrieval_plan"
    )


@pytest.mark.parametrize("provider_name", ["openai_compat", "openai_responses"])
def test_frontier_tool_planning_uses_configured_provider_and_records_usage(provider_name):
    provider = build_frontier_provider(
        FrontierLlmConfig(
            provider=provider_name,
            api_key="test",
            api_base="https://opencode.ai/zen/go/v1",
            model_path="test-frontier",
        )
    )
    output = {"name": "facts_search", "arguments": {"entity_hints": ["Atul"]}}
    raw = json.dumps(output)
    if provider_name == "openai_responses":
        response = SimpleNamespace(
            output_text=raw, usage=SimpleNamespace(input_tokens=12, output_tokens=7)
        )
        transport = provider._client.responses.create = Mock(return_value=response)
    else:
        response = SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=raw))],
            usage=SimpleNamespace(prompt_tokens=12, completion_tokens=7),
        )
        transport = provider._client.chat.completions.create = Mock(return_value=response)
    with model_request_session("session-tool-plan"):
        result = provider.plan_retrieval(
            system_prompt="Request the facts_search tool",
            user_prompt="who is atul?",
            output_schema={"type": "object"},
        )
    assert result.output == output
    assert (result.tokens_in, result.tokens_out) == (12, 7)
    assert transport.call_args.kwargs["model"] == "test-frontier"
    if provider_name == "openai_responses":
        assert (
            transport.call_args.kwargs["extra_headers"]["x-opencode-session"] == "session-tool-plan"
        )


def test_frontier_tool_planning_rejects_non_object_output():
    provider = build_frontier_provider(FrontierLlmConfig(provider="openai", api_key="test"))
    provider._call_chat = Mock(
        return_value=SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="[]"))], usage=None
        )
    )
    with pytest.raises(CoreModelError, match="must be a JSON object"):
        provider.plan_retrieval(system_prompt="plan", user_prompt="query", output_schema={})
