"""Entity profiles remain answerable when the frontier provider is unavailable."""

from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

import pytest

from engram.models.core import CoreModelError
from engram.resilience import CircuitOpenError
from engram.retrieval.domain import AnswerabilityState
from engram.retrieval.orchestrator import run_query
from tests.unit.test_adaptive_retrieval import RecordingFrontier, _context


class ProfileRepository:
    entity_id = UUID("11111111-1111-1111-1111-111111111111")
    manager_id = UUID("22222222-2222-2222-2222-222222222222")
    assistant_id = UUID("33333333-3333-3333-3333-333333333333")
    employer_id = UUID("44444444-4444-4444-4444-444444444444")

    def __init__(self, *, incoming_manager=False, conflict=False):
        self.names = {
            self.entity_id: "Atul Singh",
            self.manager_id: "Rahul",
            self.assistant_id: "Noel",
            self.employer_id: "63moons",
        }
        self.claims = [
            self.claim(index, "APPEARS_IN", value=f"conversation-{index}", confidence=0.99)
            for index in range(100)
        ]
        manager = self.claim(100, "HAS_MANAGER", object_id=self.manager_id)
        if incoming_manager:
            manager.subject_id = self.manager_id
            manager.predicate = "MANAGES"
            manager.object_entity_id = self.entity_id
        self.claims.extend(
            [
                manager,
                self.claim(101, "HAS_ASSISTANT_MANAGER", object_id=self.assistant_id),
                self.claim(102, "WORKS_AT", object_id=self.employer_id),
                self.claim(103, "WORKING_ON", value="project_orion"),
                self.claim(104, "WORKING_ON", value="fictional Project Orion"),
            ]
        )
        if conflict:
            # This low-priority claim must be assessed before profile ranking.
            self.claims[99].conflict = True

    def claim(self, index, predicate, *, object_id=None, value=None, confidence=0.9):
        return SimpleNamespace(
            id=UUID(int=1000 + index),
            tenant_id="_default",
            subject_id=self.entity_id,
            predicate=predicate,
            object_entity_id=object_id,
            object_value=value,
            object_type="ENTITY" if object_id else "STRING",
            status="ACTIVE",
            confidence=confidence,
            valid_from=None,
            valid_until=None,
            asserted_at=None,
            conflict=False,
        )

    def resolve_aliases(self, alias, **_kwargs):
        assert alias.casefold() in {"atul", "atul singh"}
        return [SimpleNamespace(entity_id=self.entity_id)]

    def get_current_related_claims(self, entity_id, **_kwargs):
        assert entity_id == self.entity_id
        return self.claims

    def get_memory(self, memory_id, **_kwargs):
        return SimpleNamespace(canonical_name=self.names[memory_id])

    def get_current_state(self, memory_id, **_kwargs):
        return SimpleNamespace(
            node=SimpleNamespace(
                canonical_name=self.names[memory_id],
                canonical_uri=f"mem://memory/{memory_id}",
                revision=1,
                status="ACTIVE",
            ),
            current_version=None,
        )

    def get_evidence(self, **_kwargs):
        return [SimpleNamespace(id="source-evidence")]


def unavailable_frontier(error):
    frontier = RecordingFrontier()
    frontier.answer = Mock(side_effect=error)
    frontier.plan_retrieval = Mock(side_effect=error)
    return frontier


@pytest.mark.parametrize("query", ["who is atul", "who is atul singh"])
@pytest.mark.parametrize("incoming_manager", [False, True])
@pytest.mark.parametrize(
    "error", [CoreModelError("provider returned HTTP 403"), CircuitOpenError("breaker open")]
)
def test_large_profile_answers_from_canonical_facts_during_provider_outage(
    query, incoming_manager, error
):
    frontier = unavailable_frontier(error)
    result = run_query(
        _context(ProfileRepository(incoming_manager=incoming_manager), frontier),
        session_id="active",
        session_context="USER: atul is a software engineer",
        query=query,
    )

    assert result.answer.startswith("Atul Singh ")
    assert "Rahul" in result.answer and "Noel" in result.answer and "63moons" in result.answer
    assert result.answerability_state == AnswerabilityState.ANSWERABLE.value
    md = result.retrieval_metadata
    assert md.answer_mode == "deterministic-canonical"
    assert md.stop_reason == "sufficient-at-L3_FACTS"
    assert md.missing_evidence == []
    assert md.verified_evidence == 5
    assert md.total_context_tokens == 0
    assert md.tool_calls[-1]["records"] == 105
    assert "L3_VECTOR" not in md.levels_visited and "L3_GRAPH" not in md.levels_visited
    # Only the current-session gate tries to generate an answer; the profile
    # is rendered directly despite failed planning and provider calls.
    assert frontier.answer.call_count == 1


def test_profile_ranking_does_not_hide_a_conflicting_claim():
    frontier = unavailable_frontier(CoreModelError("provider returned HTTP 403"))
    result = run_query(
        _context(ProfileRepository(conflict=True), frontier),
        session_id=None,
        query="who is atul",
    )

    assert result.answerability_state == AnswerabilityState.CONFLICTING_EVIDENCE.value
    assert result.retrieval_metadata.answer_mode == "evidence-terminal"
    assert result.retrieval_metadata.stop_reason == "conflicting-canonical-evidence"
    assert len(result.verified_evidence) == 105
    frontier.answer.assert_not_called()
