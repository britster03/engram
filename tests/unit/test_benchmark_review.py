"""Review semantics that must not turn missing evidence into measured success."""
import json
from pathlib import Path

import pytest

from scripts.review_lifecycle_benchmark import (
    aggregate,
    digest,
    index_page,
    inspect_detail,
    snapshot,
)


def detail(status="COMPLETE", answer="2022", score=1.0):
    usage = {"known_total_tokens": 120, "estimated_total_tokens": 0,
             "estimated_usage_calls": 0, "unknown_usage_calls": 0, "retry_count": 0}
    return {"evaluation": {
        "evaluation_id": "SESSION_ROUTING:c:q:evidence_session:native", "qa_id": "c:q",
        "trace_id": "trace-1", "status": status, "benchmark_track": "SESSION_ROUTING",
        "conversation_id": "c", "category_name": "temporal", "category_id": 2,
        "checkpoint_variant": "evidence_session", "evaluation_cutoff_session": 1,
        "search_method": "native", "question": "When?", "prediction": answer,
        "official_locomo_f1": score, "total_latency_ms": 2000,
        "gold_evidence_ids": ["D1:1"], "retrieved_ids": ["D1:1"], "packed_evidence_ids": [],
        "answer_mode": "supported-quality", "stop_reason": "sufficient-at-L1"},
        "query_usage": usage, "answer_producing_usage": usage,
        "final_answer_usage": usage, "reference_answer_display": "2022",
        "usage_reconciliation": "MATCH", "model_calls": [
            {"provider_role": "frontier", "known_total_tokens": 120,
             "phase": "generation", "input_tokens": 100, "output_tokens": 20,
             "produced_answer": True, "produced_final_answer": True}],
    }


def row(value):
    return inspect_detail(value, Path("questions/a.json"), {})


def test_pending_scores_unavailable_and_executed_failure_remains_zero():
    good = row(detail())
    bad = row(detail("DEPENDENCY_ERROR", "Unavailable", 0.0))
    pending = detail("NOT_EXECUTED", None, 0.0)
    pending["model_calls"] = []
    waiting = row(pending)
    assert waiting["official_f1"] is None
    assert waiting["known_query_tokens"] is None
    summary = aggregate([good, bad, waiting])
    assert summary["expected"] == 3 and summary["executed"] == 2
    assert summary["errors"] == 1 and summary["pending"] == 1
    assert summary["official_f1"] == 0.5
    assert aggregate([waiting])["official_f1"] is None


def test_unknown_billing_remains_additional_to_verified_subtotal():
    value = detail()
    value["query_usage"]["unknown_usage_calls"] = 1
    value["model_calls"].append({"provider_role": "core", "phase": "sufficiency",
                                 "known_total_tokens": 0, "input_tokens": None,
                                 "output_tokens": None, "status": "INTERRUPTED"})
    result = row(value)
    assert result["known_query_tokens"] == 120
    assert result["usage_status"] == "UNKNOWN_PARTIAL"
    assert result["unknown_usage_calls"] == 1
    assert result["phases"]["sufficiency"]["unknown_usage_calls"] == 1


def test_retrieval_is_not_supplied_evidence_and_flags_are_not_semantic_judges():
    value = detail(answer="It was not 2022", score=0.0)
    result = row(value)
    assert result["retrieved_gold_ids"] == ["D1:1"]
    assert result["supplied_gold_ids"] == []
    assert "REFERENCE_WORDS_WITH_EXTRA_TEXT" in result["diagnostics"]
    assert result["official_f1"] == 0.0
    assert "not a semantic correctness verdict" in " ".join(result["observations"])


def test_notes_are_bound_to_both_answer_and_trace():
    value = detail()
    notes = {value["evaluation"]["evaluation_id"]: {
        "prediction_sha256": digest(b"2022"), "trace_id": "old-trace",
        "assessment": "Previously reviewed answer", "action": "Investigate"}}
    with pytest.raises(ValueError, match="does not match"):
        inspect_detail(value, Path("questions/a.json"), notes)


def test_snapshot_preserves_unicode_multiline_and_excludes_credentials(tmp_path):
    source = tmp_path / "run"
    source.mkdir()
    record = {"text": "\u0928\u092e\u0938\u094d\u0924\u0947\nsecond line <script>"}
    content = json.dumps(record, ensure_ascii=False).encode() + b"\n"
    (source / "result_rows.jsonl").write_bytes(content + b'{"incomplete":')
    (source / "credentials.env").write_text("API_KEY=should-not-be-exported")
    destination = tmp_path / "snapshot"
    metadata = snapshot(source, destination)
    assert (destination / "result_rows.jsonl").read_bytes() == content
    assert metadata["committed_evaluations"] == 1
    assert not (destination / "credentials.env").exists()
    assert metadata["files"]["result_rows.jsonl"]["sha256"] == digest(content)


def test_index_escapes_script_termination_and_uses_text_content():
    review = {"coverage": {"expected": 1, "executed": 1, "pending": 0, "errors": 0},
              "tracks": {}, "categories": [], "engineering_findings": [],
              "evaluations": [{"question": "</script><script>window.injected=1</script>"}],
              "provider_accounting": {"components": {}}, "status": "PROVISIONAL",
              "snapshot": {"completed_at": "now"}, "models": ["model"], "conversation_count": 1}
    rendered = index_page(review)
    assert "</script><script>window.injected" not in rendered
    assert "\\u003c/script>" in rendered
    assert "td.textContent" in rendered
