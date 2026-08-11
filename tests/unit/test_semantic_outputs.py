"""Typed semantic-output validation and bounded repair."""

from typing import Any

import pytest

from engram import metrics as metrics_mod
from engram.models.core import CompletionResult, CoreModelError, CoreModelProvider
from engram.models.semantic import (
    ExtractedTriplet,
    GateWriteOutput,
    RetrievalCommand,
    complete_validated,
    validate_frontier_output,
)


class SequenceCore(CoreModelProvider):
    def __init__(self, outputs: list[Any]) -> None:
        self.outputs = outputs
        self.calls: list[dict[str, Any] | None] = []

    def complete(self, **kwargs: Any) -> CompletionResult:
        self.calls.append(kwargs.get("output_schema"))
        return CompletionResult(
            output=self.outputs[min(len(self.calls) - 1, len(self.outputs) - 1)],
            raw_text="redacted",
        )


def test_schema_violation_gets_exactly_one_repair_attempt() -> None:
    core = SequenceCore([{"store": "yes"}, {"store": True, "reason": "fact"}])
    counter = metrics_mod.core_model_calls.labels(task="gate_write", provider="unknown")
    before = counter._value.get()
    validated, _result = complete_validated(
        core,
        task="gate_write",
        schema=GateWriteOutput,
        system_prompt="system",
        user_prompt="user",
    )
    assert validated.store is True
    assert len(core.calls) == 2
    assert core.calls[0] == GateWriteOutput.model_json_schema()
    assert counter._value.get() - before == 2


def test_repeated_schema_violation_fails_without_unbounded_retries() -> None:
    core = SequenceCore([{"store": "yes"}])
    with pytest.raises(CoreModelError, match="schema validation"):
        complete_validated(
            core,
            task="gate_write",
            schema=GateWriteOutput,
            system_prompt="system",
            user_prompt="user",
        )
    assert len(core.calls) == 2


def test_frontier_contract_requires_payload_for_verdict() -> None:
    with pytest.raises(CoreModelError, match="schema validation"):
        validate_frontier_output({"verdict": "ANSWER", "answer": ""})
    valid = validate_frontier_output(
        {"verdict": "NEED_MORE", "reason": "date missing", "suggested_queries": []}
    )
    assert valid.verdict == "NEED_MORE"


def test_retrieval_command_schema_rejects_unregistered_templates() -> None:
    valid = RetrievalCommand.model_validate({
        "template": "t_neighbours_by_relation",
        "params": {"node_uri": "mem://user/entities/alice/alice.md"},
    })
    assert valid.template == "t_neighbours_by_relation"
    with pytest.raises(ValueError):
        RetrievalCommand.model_validate({"template": "t_cat", "params": {}})

    template_schema = RetrievalCommand.model_json_schema()
    assert "t_cat" not in str(template_schema)


def test_extraction_requires_boolean_explicit_correction_evidence() -> None:
    base = {
        "subject": "Alice",
        "relation": "works_at",
        "object": "Meta",
        "confidence": 0.9,
    }
    assert ExtractedTriplet.model_validate(base).explicit_correction is False
    assert (
        ExtractedTriplet.model_validate({**base, "explicit_correction": True})
        .explicit_correction
        is True
    )
    with pytest.raises(ValueError):
        ExtractedTriplet.model_validate({**base, "explicit_correction": "true"})
