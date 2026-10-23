import ast
import csv
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmarks.lifecycle.dataset import load
from benchmarks.lifecycle.descriptive import JournalIndex, _detail, generate_details
from benchmarks.lifecycle.ledger import append, atomic
from benchmarks.lifecycle.observer import Trace, capture, instrument_provider
from benchmarks.lifecycle.vendor import verify_official_scorer


def sample_row(**overrides):
    question = load()[0].questions[0]
    return {"evaluation_id": "CANONICAL:test", "qa_id": question.qa_id,
            "conversation_id": question.conversation_id, "category_id": question.category,
            "category_name": "single_hop", "question": question.question,
            "gold_answer": question.answer, "prediction": "June", "trace_id": "current",
            "benchmark_track": "CANONICAL", "question_status": "VALID",
            "status": "COMPLETE", "official_locomo_f1": .5, "total_tokens": 15,
            "known_total_tokens": 15, "total_latency_ms": 123,
            "semantic_preservation_status": "PRESERVED", "answer_mode": "canonical-model",
            "checkpoint_variant": "completed", "search_method": "native", **overrides}


def sdk_call(**overrides):
    return {"attempt_id": "a", "trace_id": "current", "component": "query",
            "operation_id": "op", "phase": "generation", "provider_role": "frontier",
            "provider": "openai_responses", "model": "same-model", "status": "RETURNED",
            "input_tokens": 10, "output_tokens": 5, "provider_input_tokens": 10,
            "provider_output_tokens": 5, "token_count_source": "provider",
            "request": {"instructions": "exact\nहिंदी 😀", "input": "<script>alert('x')</script>"},
            "response_text": '{"verdict":"ANSWER","answer":"June"}', **overrides}


def journals(source, calls, events=None):
    for i, call in enumerate(calls):
        append(source / "provider_attempts.1.jsonl", {**{k: v for k, v in call.items() if k not in {
            "input_tokens", "output_tokens", "provider_input_tokens", "provider_output_tokens",
            "response_text", "raw_usage", "status"}}, "state": "STARTED", "time_ns": i + 1})
        append(source / "provider_attempts.1.jsonl", {**call, "state": "FINISHED", "time_ns": i + 2})
        append(source / "evaluation_attempts.jsonl", {"evaluation_id": "CANONICAL:test",
                                                     "trace_id": call["trace_id"], "state": "STARTED"})
    append(source / "trace.jsonl", {"trace_id": "current", "calls": calls,
                                   "events": events or [], "component": "query"})


def test_complete_scorer_and_reference_are_verified():
    assert verify_official_scorer()["status"] == "VERIFIED"
    path = Path("benchmarks/lifecycle/vendor/official_qa.py")
    functions = {n.name for n in ast.parse(path.read_text()).body if isinstance(n, ast.FunctionDef)}
    assert functions == {"normalize_answer", "f1_score", "f1", "eval_question_answering"}


def test_empty_scorer_cannot_pass_parity(tmp_path):
    base = Path("benchmarks/lifecycle/vendor")
    for name in ("provenance.json", "evaluation.py.reference"):
        (tmp_path / name).write_bytes((base / name).read_bytes())
    (tmp_path / "official_qa.py").write_text("")
    with pytest.raises(RuntimeError, match="missing"):
        verify_official_scorer(tmp_path)


def test_capture_roles_complete_request_and_exact_unicode(tmp_path, monkeypatch):
    monkeypatch.setenv("LOCOMO_TRACE_DIR", str(tmp_path))
    text = "first line\nहिंदी 😀 <script>literal</script>"
    response = SimpleNamespace(output_text="answer", usage={"input_tokens": 4, "output_tokens": 2})
    def provider(role):
        resource = SimpleNamespace(create=lambda **kwargs: response)
        p = SimpleNamespace(cfg=SimpleNamespace(provider="openai_responses"),
                            _client=SimpleNamespace(responses=resource))
        p.complete = lambda **kwargs: resource.create(model="same-model", instructions=text,
            input=text, tools=[{"type": "function", "name": "search"}],
            text={"format": {"type": "json_schema"}}, top_p=.9,
            extra_headers={"Authorization": "never-export"})
        return instrument_provider(p, role)
    trace = Trace("t", evaluation_id="evaluation", qa_id="question")
    with capture(trace):
        provider("core").complete()
        provider("frontier").complete()
    assert [c["provider_role"] for c in trace.calls] == ["core", "frontier"]
    for call in trace.calls:
        assert call["evaluation_id"] == "evaluation"
        assert call["request"]["instructions"] == call["request"]["input"] == text
        assert call["request"]["top_p"] == .9
        assert "tools" in call["request"] and "text" in call["request"]
        assert "never-export" not in json.dumps(call)


def test_report_keeps_full_context_escaped_html_and_csv_roundtrip(tmp_path):
    source, output = tmp_path / "source", tmp_path / "report"
    source.mkdir()
    atomic(source / "run_manifest.json", {"profile": "pilot", "report_schema_version": 2})
    call = sdk_call(request={"instructions": "हिंदी\n" * 40000, "input": "<script>alert('x')</script>"})
    events = [{"kind": "provider_result", "method": "answer", "operation_id": "op",
               "result": {"verdict": "ANSWER", "answer": "June"}},
              {"kind": "store", "store": "neo4j", "function": "search", "result": [{"text": "candidate-only"}]},
              {"kind": "model_context", "operation_id": "op", "phase": "generation", "text": "supplied-only"}]
    journals(source, [call], events)
    manifest = generate_details(source, output, rows=[sample_row()])
    assert manifest["observed_execution_status"] == "CAPTURE_COMPLETE"
    detail = json.loads(next((output / "questions").glob("*.json")).read_text())
    assert detail["model_calls"][0]["request"] == call["request"]
    assert detail["source_reads"][0]["result_preview"][0]["text"] == "candidate-only"
    from benchmarks.lifecycle.ledger import expand_trace
    restored = expand_trace(output, detail["trace_archives"][0])
    assert restored["events"][1]["result"][0]["text"] == "candidate-only"
    assert detail["model_contexts"][0]["text"] == "supplied-only"
    assert detail["answer_link_status"] == "VERIFIED"
    page = next((output / "questions").glob("*.html")).read_text()
    assert "<script>alert('x')</script>" not in page
    assert "&lt;script&gt;" in page
    with (output / "management_compact.csv").open() as stream:
        row = next(csv.DictReader(stream))
    assert row["Known Query Tokens"] == "15"
    assert (output / row["Detail JSON"]).exists()
    assert not (source / "management.csv").exists()
    detail_file = next((output / "questions").glob("*.json"))
    modified = detail_file.stat().st_mtime_ns
    generate_details(source, output, rows=[sample_row()])
    assert detail_file.stat().st_mtime_ns == modified


def test_retries_prior_submissions_unknown_usage_and_final_answer(tmp_path):
    calls = [sdk_call(attempt_id="old", trace_id="prior", operation_id="prior-op", phase="tool_planning",
                      provider_role="frontier", response_text='{"tool":"search"}'),
             sdk_call(attempt_id="failed", status="FAILED", input_tokens=None, output_tokens=None,
                      provider_input_tokens=None, provider_output_tokens=None,
                      token_count_source="unavailable", response_text=None), sdk_call()]
    events = [{"kind": "provider_result", "method": "answer", "operation_id": "op",
               "result": {"verdict": "ANSWER", "answer": "June"}}]
    journals(tmp_path, calls, events)
    index = JournalIndex(tmp_path, tmp_path / "index.sqlite")
    try:
        detail = _detail(sample_row(known_total_tokens=30), index, None, None)
    finally:
        index.close()
    assert detail["query_usage"]["known_total_tokens"] == 30
    assert detail["query_usage"]["total_tokens"] is None
    assert detail["query_usage"]["unknown_usage_calls"] == 1
    assert detail["final_answer_usage"]["known_total_tokens"] == 15
    assert detail["final_answer_usage"]["unknown_usage_calls"] == 1
    assert len(detail["model_calls"]) == 3


def test_interrupted_attempt_keeps_started_request(tmp_path):
    call = sdk_call()
    append(tmp_path / "provider_attempts.1.jsonl", {"attempt_id": "a", "trace_id": "current",
        "component": "query", "operation_id": "op", "request": call["request"], "state": "STARTED"})
    index = JournalIndex(tmp_path, tmp_path / "index.sqlite")
    try:
        observed = index.calls("current")[0]
    finally:
        index.close()
    assert observed["request"] == call["request"]
    assert observed["status"] == "IN_FLIGHT_OR_INTERRUPTED"
    assert observed["input_tokens"] is None


def test_deterministic_answer_never_links_prior_frontier_answer(tmp_path):
    journals(tmp_path, [sdk_call()], [{"kind": "provider_result", "method": "answer",
        "operation_id": "op", "result": {"verdict": "ANSWER", "answer": "June"}}])
    index = JournalIndex(tmp_path, tmp_path / "index.sqlite")
    try:
        detail = _detail(sample_row(answer_mode="deterministic-canonical"), index, None, None)
    finally:
        index.close()
    assert detail["answer_link_status"] == "NO_FRONTIER_GENERATION"
    assert detail["final_answer_usage"]["known_total_tokens"] == 0
    assert detail["query_usage"]["known_total_tokens"] == 15


def test_duplicate_attempt_state_is_rejected(tmp_path):
    record = {**sdk_call(), "state": "FINISHED"}
    append(tmp_path / "provider_attempts.1.jsonl", record)
    append(tmp_path / "provider_attempts.1.jsonl", record)
    with pytest.raises(ValueError, match="Duplicate"):
        JournalIndex(tmp_path, tmp_path / "index.sqlite")


def test_unknown_historical_role_is_not_guessed_from_model(tmp_path):
    call = sdk_call(provider_role=None)
    journals(tmp_path, [call])
    index = JournalIndex(tmp_path, tmp_path / "index.sqlite")
    try:
        detail = _detail(sample_row(), index, None, None)
    finally:
        index.close()
    assert detail["model_calls"][0]["provider_role"] == "unknown"
    assert detail["model_calls"][0]["role_source"] == "unavailable"
