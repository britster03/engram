"""Tests for per-query token accounting."""

from __future__ import annotations

from engram.models.usage import QueryTokenUsage


def test_provider_usage_is_reported_exactly() -> None:
    usage = QueryTokenUsage.from_request("What is my role?", "recent context")
    usage.record_model_call(
        stage="frontier_answer_0",
        provider="openai_responses",
        model="test-model",
        input_tokens=20,
        output_tokens=7,
        input_text="ignored",
        output_text="ignored",
    )

    payload = usage.to_dict()

    assert payload["input_tokens"] == 20
    assert payload["output_tokens"] == 7
    assert payload["total_tokens"] == 27
    assert payload["estimated"] is False
    assert payload["model_calls"] == [
        {
            "stage": "frontier_answer_0",
            "provider": "openai_responses",
            "model": "test-model",
            "input_tokens": 20,
            "output_tokens": 7,
            "total_tokens": 27,
            "estimated": False,
        }
    ]
    assert payload["query_tokens"] > 0
    assert payload["session_context_tokens"] > 0


def test_missing_provider_usage_is_estimated_and_marked() -> None:
    usage = QueryTokenUsage()
    usage.record_model_call(
        stage="l0_plan",
        provider="local",
        model="test-model",
        input_tokens=None,
        output_tokens=None,
        input_text="A short prompt",
        output_text="{}",
    )

    payload = usage.to_dict()

    assert payload["input_tokens"] > 0
    assert payload["output_tokens"] > 0
    assert payload["total_tokens"] == payload["input_tokens"] + payload["output_tokens"]
    assert payload["estimated"] is True
    assert payload["model_calls"][0]["estimated"] is True
