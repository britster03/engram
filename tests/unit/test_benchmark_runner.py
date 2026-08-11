from __future__ import annotations

from typing import Any

import pytest

from benchmarks.engram_client import DrainConfig, EngramClient, IngestFailedError
from benchmarks.loader import Conversation, QAProbe, Turn
from benchmarks.run_locomo import (
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


def test_retrieved_turn_ids_are_unique_and_rank_preserving() -> None:
    trace = {
        "hits": [
            {"source_turn_ids": ["D1:2", "D1:1"]},
            {"source_turn_ids": ["D1:2", "D1:3"]},
        ]
    }
    assert _retrieved_turn_ids(trace) == ["D1:2", "D1:1", "D1:3"]


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
