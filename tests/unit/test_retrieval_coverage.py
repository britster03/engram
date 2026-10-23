from engram.retrieval.context import ContextBuilder
from engram.retrieval.domain import AnswerabilityState, RetrievalPlan, VerifiedEvidence
from engram.retrieval.evidence import EvidenceGate
from engram.retrieval.planner import AdaptivePlanner


def test_composite_question_needs_all_requested_predicates():
    plan = AdaptivePlanner().plan("What is Alice's current role and location?")
    assert set(plan.required_predicates) == {"HAS_ROLE", "LOCATED_IN"}
    assert plan.predicate_hint is None
    role = VerifiedEvidence(
        memory_id="alice", predicate="HAS_ROLE", object_value="Engineer", canonical=True
    )
    assessment = EvidenceGate().assess(plan, [role])
    assert assessment.state == AnswerabilityState.PARTIALLY_ANSWERABLE
    assert "predicate:LOCATED_IN" in assessment.missing_evidence
    location = role.model_copy(update={"predicate": "LOCATED_IN", "object_value": "Pune"})
    assert EvidenceGate().assess(plan, [role, location]).state == AnswerabilityState.ANSWERABLE


def test_path_bundle_cannot_lose_an_edge_to_context_truncation():
    plan = RetrievalPlan(query="How are Alice and Bob connected?", intent="relationship")
    rows = [
        VerifiedEvidence(
            claim_id=str(i),
            memory_id=str(i),
            canonical=True,
            predicate="KNOWS",
            object_value="Bob",
            bundle_ids=["path-one"],
        )
        for i in range(2)
    ]
    builder = ContextBuilder()
    assert builder.pack_evidence(plan, rows[:1], budget=35) == rows[:1]
    assert builder.pack_evidence(plan, rows, budget=35) == []
    assert builder.pack_evidence(plan, rows, budget=1000) == rows
