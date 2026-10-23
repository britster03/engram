from types import SimpleNamespace

import pytest

from engram.ingest.user_memory import extract_user_message, gate_user_message


class Model:
    def __init__(self, output):
        self.output = output

    def complete(self, **kwargs):
        assert "Assistant:" not in kwargs["user_prompt"]
        return SimpleNamespace(output=self.output)


def claim(quote, value="Pune"):
    return {
        "subject": "user",
        "relation": "lives_in",
        "object": value,
        "confidence": 0.95,
        "source_quote": quote,
        "subject_id": "model-injected-id",
    }


def test_user_source_span_is_verified_and_model_cannot_inject_ids():
    text = "I live in Pune. What should I visit?"
    result = extract_user_message(Model({"triplets": [claim("I live in Pune.")]}), text)
    assert result["resolved_text"] == text
    assert result["triplets"][0]["source_span"]["quote"] == "I live in Pune."
    assert "subject_id" not in result["triplets"][0]


def test_unsupported_source_or_object_is_rejected():
    with pytest.raises(ValueError, match="supporting source span"):
        extract_user_message(
            Model({"triplets": [claim("Assistant invented Pune")]}), "Where do I live?"
        )
    with pytest.raises(ValueError, match="object is absent"):
        extract_user_message(
            Model({"triplets": [claim("I live in Pune.", "London")]}), "I live in Pune."
        )


def test_negative_assertion_is_not_persisted_as_positive_fact():
    text = "I do not live in Pune."
    assert extract_user_message(Model({"triplets": [claim(text)]}), text)["triplets"] == []


def test_acknowledgments_skip_without_model_call():
    assert gate_user_message(object(), "Thanks!")["store"] is False
