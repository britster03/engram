from benchmarks.lifecycle.adapter import compare
from benchmarks.lifecycle.observer import Trace, capture
from engram.retrieval.domain import RetrievalPlan, VerifiedEvidence
from engram.retrieval.sufficiency import SufficiencyController
from tests.unit.test_adaptive_retrieval import RecordingFrontier, _context


def test_enforced_comparison_uses_native_pack_without_semantic_model_calls(monkeypatch):
    from engram.retrieval import orchestrator
    rows = [VerifiedEvidence(memory_id="one", claim_id="one-role", tenant_id="tenant",
                             subject_id="one", subject_name="Alice", predicate="HAS_ROLE",
                             object_value="Engineer", object_type="ROLE")]
    monkeypatch.setattr(orchestrator, "_hybrid_retrieve", lambda *a, **kw: (rows, 1))
    def forbidden(*args, **kwargs):
        raise AssertionError("retrieval comparison called a semantic model")
    monkeypatch.setattr(SufficiencyController, "_call", forbidden)
    context = _context(object(), RecordingFrontier())
    context.cfg.retrieval.quality_mode = "enforce"
    with capture(Trace("comparison")):
        all_rows, packed, diagnostics = compare(context, RetrievalPlan(query="Alice career"),
                                                "hybrid", "tenant", "session")
    assert all_rows == packed == rows
    from engram.retrieval.sufficiency import POLICY_VERSION
    assert diagnostics["packing_policy"] == POLICY_VERSION
    assert not context.frontier.calls
