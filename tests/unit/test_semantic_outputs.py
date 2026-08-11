"""Typed semantic-output validation and bounded repair."""

from typing import Any

import pytest

from engram.models.core import CompletionResult, CoreModelError, CoreModelProvider
from engram.models.semantic import (
    GateWriteOutput,
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
