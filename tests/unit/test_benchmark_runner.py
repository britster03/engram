from __future__ import annotations

from typing import Any

import pytest

from benchmarks.engram_client import DrainConfig, EngramClient, IngestFailedError
from benchmarks.loader import Conversation, QAProbe, Turn
from benchmarks.run_locomo import (
    _dataset_provenance_error,
    _effective_drain_timeout,
    _event_latency_seconds,
    _expected_pair_count,
    _metrics_delta,
    _metrics_snapshot,
    _retrieved_turn_ids,
    _selected_questions,
    ingest_conversation,
    summarize,
)


class CapturingClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def ingest_pair(self, **kwargs):
        self.calls.append(kwargs)
        return {"event_id": f"evt-{len(self.calls)}"}


class _QueryResponse:
    status_code = 200
    text = ""

    def json(self) -> dict[str, Any]:
        return {"answer": "ok", "retrieval_metadata": {}}


class _CapturingHttp:
    def __init__(self) -> None:
        self.body: dict[str, Any] | None = None

    def post(self, _path: str, *, json: dict[str, Any], timeout: float):
        self.body = json
        return _QueryResponse()


def _turn(index: int, *, caption: str | None = None) -> Turn:
    return Turn(
        speaker="A" if index % 2 else "B",
        text=f"turn {index}",
        dia_id=f"D1:{index}",
        session_idx=1,
        timestamp="2023-05-08T13:56:00",
        blip_caption=caption,
        image_urls=["https://example.test/image.jpg"] if caption else [],
    )


def test_ingest_conversation_preserves_ids_images_and_prior_only_context() -> None:
    client = CapturingClient()
    conv = Conversation(
        sample_id="sample-1",
        speaker_a="A",
        speaker_b="B",
        turns=[_turn(1, caption="a bicycle"), _turn(2), _turn(3), _turn(4)],
    )

    event_ids = ingest_conversation(client, conv, context_turns=2)  # type: ignore[arg-type]

    assert event_ids == ["evt-1", "evt-2"]
    assert client.calls[0]["session_context"] is None
    assert client.calls[0]["user_external_id"] == "D1:1"
    assert client.calls[0]["user_image_caption"] == "a bicycle"
    assert client.calls[0]["force_store"] is True
    second_context = client.calls[1]["session_context"]
    assert "turn 1" in second_context and "turn 2" in second_context
    assert "turn 3" not in second_context and "turn 4" not in second_context


def test_drain_timeout_scales_for_full_conversations_but_honors_override() -> None:
    assert _effective_drain_timeout(None, 15) == 600.0
    assert _effective_drain_timeout(None, 214) == 6420.0
    assert _effective_drain_timeout(45.0, 214) == 45.0


def test_bundled_dataset_commit_must_match_known_release_bytes() -> None:
    known_hash = "79fa87e90f04081343b8c8debecb80a9a6842b76a7aa537dc9fdf651ea698ff4"
    known_commit = "3eb6f2c585f5e1699204e3c3bdf7adc5c28cb376"

    assert (
        _dataset_provenance_error(sha256=known_hash, upstream_commit=known_commit)
        is None
    )
    assert "expected" in str(
        _dataset_provenance_error(sha256=known_hash, upstream_commit="wrong")
    )
    assert "must identify" in str(
        _dataset_provenance_error(sha256="custom", upstream_commit="unrecorded")
    )


def test_expected_pair_count_uses_source_sessions_and_limit() -> None:
    conv = Conversation(
        sample_id="pairs",
        speaker_a="A",
        speaker_b="B",
        turns=[_turn(1), _turn(2), _turn(3), _turn(4)],
    )
    assert _expected_pair_count(conv, limit_pairs=0) == 2
    assert _expected_pair_count(conv, limit_pairs=1) == 1


def test_retrieved_turn_ids_are_unique_and_rank_preserving() -> None:
    trace = {
        "hits": [
            {"source_turn_ids": ["D1:2", "D1:1"]},
            {"source_turn_ids": ["D1:2", "D1:3"]},
        ]
    }
    assert _retrieved_turn_ids(trace) == ["D1:2", "D1:1", "D1:3"]


def test_benchmark_client_sends_one_unambiguous_retrieval_mode() -> None:
    client = EngramClient(base_url="http://example.test", api_key="test")
    client._http.close()
    capture = _CapturingHttp()
    client._http = capture  # type: ignore[assignment]

    client.query(
        "question", retrieval_mode="forced", min_depth="L4", max_depth="L4"
    )

    assert capture.body is not None
    assert capture.body["retrieval_mode"] == "forced"
    assert capture.body["min_depth"] == "L4"
    assert capture.body["max_depth"] == "L4"
    assert "force_retrieval" not in capture.body


def test_canary_question_selection_is_seeded_and_category_balanced() -> None:
    conv = Conversation(sample_id="sample", speaker_a="A", speaker_b="B")
    conv.qa = [
        QAProbe(f"q{category}-{i}", "a", category, [], category == 5)
        for category in range(1, 6)
        for i in range(4)
    ]
    first = _selected_questions(conv, 10, seed=17)
    second = _selected_questions(conv, 10, seed=17)
    different = _selected_questions(conv, 10, seed=18)
    assert [index for index, _ in first] == [index for index, _ in second]
    assert [index for index, _ in first] != [index for index, _ in different]
    counts = {category: 0 for category in range(1, 6)}
    for _index, probe in first:
        counts[probe.category] += 1
    assert counts == {category: 2 for category in range(1, 6)}


def test_partial_corpus_questions_require_all_evidence_to_be_ingested() -> None:
    conv = Conversation(
        sample_id="partial",
        speaker_a="A",
        speaker_b="B",
        turns=[_turn(1), _turn(2), _turn(3), _turn(4)],
        qa=[
            QAProbe("available", "a", 4, ["D1:1"], False),
            QAProbe("partly missing", "b", 1, ["D1:2", "D1:3"], False),
            QAProbe("adversarial missing", "", 5, ["D1:4"], True),
            QAProbe("unannotated", "c", 3, [], False),
        ],
    )

    selected = _selected_questions(conv, 10, seed=42, limit_pairs=1)
    assert [(index, probe.question) for index, probe in selected] == [(0, "available")]
    assert len(_selected_questions(conv, 10, seed=42)) == 4


def test_summary_uses_official_metrics_as_primary() -> None:
    rows = [
        {
            "category_name": "single_hop",
            "answer_f1": 1.0,
            "evidence_recall_at_5": 0.5,
            "evidence_recall_at_10": 1.0,
            "evidence_recall_at_25": 1.0,
            "judged": False,
        },
        {
            "category_name": "single_hop",
            "answer_f1": 0.0,
            "evidence_recall_at_5": 0.0,
            "evidence_recall_at_10": 0.5,
            "evidence_recall_at_25": 1.0,
            "judged": False,
        },
    ]
    summary = summarize(rows)
    assert summary["overall"]["answer_f1"] == 0.5
    assert summary["overall"]["evidence_recall_at_10"] == 0.75
    assert summary["judge"]["accuracy"] is None


def test_query_operational_summary_aggregates_redacted_trace_usage() -> None:
    rows = [{
        "status": "COMPLETE",
        "category_name": "single_hop",
        "answer_f1": 1.0,
        "evidence": ["D1:1"],
        "evidence_recall_at_5": 1.0,
        "evidence_recall_at_10": 1.0,
        "evidence_recall_at_25": 1.0,
        "query_latency_s": 2.0,
        "retrieval_metadata": {"cascade_depth_reached": "L2", "reentries": 1},
        "retrieval_trace": {
            "hits": [{"retrieval_level": "L1_cat", "source_turn_ids": ["D1:1"]}],
            "model_calls": [{
                "family": "core",
                "task": "l1_plan",
                "provider": "test",
                "model": "model",
                "provider_calls": 1,
                "tokens_in": 10,
                "tokens_out": 5,
            }],
        },
        "judged": False,
    }]

    operations = summarize(rows)["query_operations"]
    assert operations["latency_s"]["p95"] == 2.0
    assert operations["reentry_rate"] == 1.0
    assert operations["cascade_depth_distribution"] == {"L2": 1}
    assert operations["evidence_recall_at_25_by_hit_level"] == {"L1": 1.0}
    assert operations["model_usage"][0]["tokens_in"] == 10


def test_metrics_delta_and_event_latency_are_deterministic() -> None:
    before = _metrics_snapshot(
        '# TYPE engram_core_model_calls_total counter\n'
        'engram_core_model_calls_total{provider="test",task="extract"} 2\n'
    )
    after = _metrics_snapshot(
        '# TYPE engram_core_model_calls_total counter\n'
        'engram_core_model_calls_total{provider="test",task="extract"} 5\n'
    )
    assert _metrics_delta(before, after) == [{
        "name": "engram_core_model_calls_total",
        "labels": {"provider": "test", "task": "extract"},
        "delta": 3.0,
    }]
    assert _event_latency_seconds({
        "created_at": "2026-08-11 12:00:00",
        "processed_at": "2026-08-11 12:00:03",
    }) == 3.0


def test_exact_event_wait_fails_closed_on_failed_event(monkeypatch) -> None:
    client = EngramClient(base_url="http://example.test", api_key="test")
    monkeypatch.setattr(
        client,
        "event_status",
        lambda _ids: {
            "memory_ready": False,
            "missing_ids": [],
            "failed_count": 1,
            "failures": [{"event_id": "evt-1", "error": "boom"}],
        },
    )
    try:
        with pytest.raises(IngestFailedError, match="evt-1:boom"):
            client.wait_for_events(
                ["evt-1"],
                DrainConfig(max_wait_s=1.0, poll_interval_s=0.0),
            )
    finally:
        client.close()
