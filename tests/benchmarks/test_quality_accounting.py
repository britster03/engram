import json

import pytest

from benchmarks.lifecycle.adapter import Provenance, session_id
from benchmarks.lifecycle.dataset import load
from benchmarks.lifecycle.evaluation import context_evidence
from benchmarks.lifecycle.metrics import bootstrap_conversations
from benchmarks.lifecycle.observer import Trace, summarize_usage
from engram.retrieval.sufficiency import POLICY_VERSION


@pytest.mark.parametrize("policy", ["requirement-support-v1", "requirement-support-v2", POLICY_VERSION])
def test_quality_context_maps_only_originals_actually_supplied(policy):
    conversation = load()[0]
    turn = conversation.turns[0]
    sid = session_id(conversation.id, turn.session)
    provenance = Provenance({"postgres": {}}, [], conversation)
    item = {
        "id": "turn0",
        "text": turn.content,
        "source_kind": "conversation_record",
        "role": "user",
        "source": {
            "session_id": sid,
            "turn_idx": turn.index * 2,
            "source_type": "session_turn",
            "role": "user",
        },
    }
    trace = Trace("test")
    trace.emit("tool", level="L1", result={"text": json.dumps([item]), "sources": [item["source"]]})
    trace.emit("model_context", text=json.dumps([item]) + "\n{}", phase="generation")
    result = context_evidence(provenance, trace, {"quality_policy": policy})
    assert result["packed_evidence_ids"] == [turn.dia_id]
    assert result["provenance_status"] == result["packed_provenance_status"] == "EXACT"
    assert result["supplied_context_levels"] == ["L1"]


def test_resolved_original_retains_observed_canonical_discovery_level():
    conversation = load()[0]
    turn = conversation.turns[0]
    provenance = Provenance({"postgres": {"memory_claims": [
        {"id": "claim", "source_event_id": "original"}]}},
        [{"event_id": "original", "dia_id": turn.dia_id, "conversation_id": conversation.id}],
        conversation)
    item = {"id": "original-turn", "text": turn.content, "role": "user",
            "source_kind": "conversation_record", "source": {"event_id": "original"}}
    trace = Trace("origin")
    trace.emit("tool", level="L3_FACTS", result=[[{"claim_id": "claim"}], 1])
    trace.emit("model_context", text=json.dumps([item]), phase="generation")
    result = context_evidence(provenance, trace, {"quality_policy": POLICY_VERSION})
    assert result["packed_evidence_ids"] == [turn.dia_id]
    assert result["packed_provenance_status"] == "EXACT"
    assert result["supplied_context_levels"] == ["L3_FACTS"]


def test_semantic_phase_usage_is_separate_without_double_counting_subsets():
    calls = [
        {
            "operation_id": str(i),
            "phase": phase,
            "input_tokens": 10,
            "output_tokens": 5,
            "provider_input_tokens": 10,
            "provider_output_tokens": 5,
            "reasoning_tokens": 3,
            "cached_input_tokens": 4,
            "token_count_source": "provider",
        }
        for i, phase in enumerate(["decomposition", "sufficiency", "output_support", "generation"])
    ]
    result = summarize_usage(calls)
    assert result["total_tokens"] == result["known_total_tokens"] == 60
    assert result["sufficiency_input_tokens"] == 10
    assert result["output_support_output_tokens"] == 5
    assert result["total_reasoning_tokens"] == 12


def test_single_conversation_has_no_between_conversation_interval():
    assert bootstrap_conversations(
        [{"conversation_id": "one", "score": 0}, {"conversation_id": "one", "score": 1}],
        "score",
        samples=50,
    ) == (None, None)


def test_paired_comparison_retains_errors_and_unknown_usage(tmp_path):
    from benchmarks.lifecycle.compare_quality import compare
    from benchmarks.lifecycle.ledger import atomic
    from benchmarks.lifecycle.report import csv_write

    baseline, candidate = tmp_path / "baseline", tmp_path / "candidate"
    for directory, score, status in ((baseline, 0, "COMPLETE"), (candidate, 0, "ERROR")):
        directory.mkdir()
        atomic(
            directory / "run_manifest.json",
            {"expected_evaluations": 1, "profile": "pilot", "seed": 42},
        )
        csv_write(
            directory / "per_question.csv",
            [
                {
                    "evaluation_id": "one",
                    "conversation_id": "c",
                    "benchmark_track": "CANONICAL",
                    "question": "q",
                    "prediction": "",
                    "status": status,
                    "official_locomo_f1": score,
                    "total_tokens": None,
                }
            ],
        )
    result = compare(baseline, candidate, tmp_path / "paired")
    assert result["pairs"] == 1 and result["candidate_errors"] == 1
    assert result["release_status"].startswith("REQUIRES")
