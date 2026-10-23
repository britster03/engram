from types import SimpleNamespace

import pytest

from benchmarks.lifecycle import evaluation as evaluation_module
from benchmarks.lifecycle.adapter import Provenance
from benchmarks.lifecycle.dataset import evaluations, load, select
from benchmarks.lifecycle.observer import Trace


@pytest.mark.parametrize(
    "state,reason,action",
    [
        ("INSUFFICIENT_EVIDENCE", "cascade-exhausted", "ABSTAIN"),
        ("INSUFFICIENT_EVIDENCE", "dependency-unavailable", "UNAVAILABLE"),
        ("INSUFFICIENT_EVIDENCE", "ambiguous-entity-reference", "CLARIFY"),
        ("CONFLICTING_EVIDENCE", "conflicting-source-evidence", "CONFLICT"),
        ("PARTIALLY_ANSWERABLE", "supported-at-exhaustion", "PARTIAL"),
    ],
)
def test_native_terminal_diagnostics_do_not_change_official_adversarial_scoring(
    state, reason, action
):
    dataset = load()
    conversation = next(c for c in dataset if c.id == "conv-26")
    evaluation = next(
        e
        for e in evaluations(conversation, select(dataset, "pilot"))
        if e.question.category == 5 and e.track == "CANONICAL"
    )
    metadata = {
        "quality_policy": "requirement-support-v1",
        "answerability_state": state,
        "stop_reason": reason,
    }
    result = SimpleNamespace(
        answer="I don't have enough verified information to answer the complete question.",
        retrieval_metadata=SimpleNamespace(to_dict=lambda: metadata),
    )
    row = evaluation_module.make_row(
        evaluation,
        Trace("test"),
        Provenance({"postgres": {}}, [], conversation),
        result,
        run_id="test",
        git_sha="test",
        config_hash="test",
    )
    assert row["terminal_action"] == action
    assert row["native_abstention"] is (action == "ABSTAIN")
    assert row["official_locomo_f1"] == 0
    assert row["adversarial_correct"] == 0
    assert row["abstention_diagnostic_source"] == "NATIVE_CONTROLLER_STATE"
