"""Token accounting for one retrieval query.

Provider APIs are the source of truth when they return usage metadata.  Some
providers and local/test implementations do not return it, so the ledger
falls back to the repository's tokenizer and marks that call as estimated.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from engram.tokens import count_tokens


def _non_negative(value: int | None) -> int:
    return max(0, int(value or 0))


@dataclass
class ModelTokenCall:
    """Usage for one Core or Frontier model invocation."""

    stage: str
    provider: str
    model: str
    input_tokens: int
    output_tokens: int
    estimated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "provider": self.provider,
            "model": self.model,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.input_tokens + self.output_tokens,
            "estimated": self.estimated,
        }


@dataclass
class QueryTokenUsage:
    """Token usage and useful request-context counts for one query."""

    query_tokens: int = 0
    session_context_tokens: int = 0
    retrieval_context_tokens: int = 0
    model_calls: list[ModelTokenCall] = field(default_factory=list)

    @classmethod
    def from_request(cls, query: str, session_context: str | None) -> QueryTokenUsage:
        return cls(
            query_tokens=count_tokens(query),
            session_context_tokens=count_tokens(session_context or ""),
        )

    def record_model_call(
        self,
        *,
        stage: str,
        provider: str,
        model: str,
        input_tokens: int | None,
        output_tokens: int | None,
        input_text: str,
        output_text: str,
    ) -> None:
        estimated = input_tokens is None or output_tokens is None
        self.model_calls.append(
            ModelTokenCall(
                stage=stage,
                provider=provider,
                model=model,
                input_tokens=(
                    _non_negative(input_tokens)
                    if input_tokens is not None
                    else count_tokens(input_text)
                ),
                output_tokens=(
                    _non_negative(output_tokens)
                    if output_tokens is not None
                    else count_tokens(output_text)
                ),
                estimated=estimated,
            )
        )

    def to_dict(self) -> dict[str, Any]:
        input_tokens = sum(call.input_tokens for call in self.model_calls)
        output_tokens = sum(call.output_tokens for call in self.model_calls)
        return {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
            "estimated": any(call.estimated for call in self.model_calls),
            "query_tokens": self.query_tokens,
            "session_context_tokens": self.session_context_tokens,
            "retrieval_context_tokens": self.retrieval_context_tokens,
            "model_calls": [call.to_dict() for call in self.model_calls],
        }
