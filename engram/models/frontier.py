"""FrontierLLMProvider — final answer generation with NEED_MORE re-entry (§4.3).

Providers expose two methods:

  - `answer(...)` → buffered: used for the first pass, where we must inspect
    the verdict before deciding whether to stream or re-enter.
  - `stream_answer(...)` → generator of text chunks, used after the verdict
    is known to be ANSWER (§4.3.2). The generator yields only the answer
    text; callers concatenate chunks for logging and forward them to the
    client.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Literal


@dataclass
class FrontierVerdict:
    verdict: Literal["ANSWER", "NEED_MORE"]
    answer: str | None = None
    reason: str | None = None
    suggested_queries: list[str] | None = None
    suggested_depth: str | None = None
    tokens_in: int | None = None
    tokens_out: int | None = None
    latency_ms: float | None = None
    provider_calls: int = 1


class FrontierLLMProvider(ABC):
    @abstractmethod
    def answer(
        self,
        *,
        system_prompt: str,
        msc: str,
        user_query: str,
        allow_need_more: bool = True,
    ) -> FrontierVerdict:  # pragma: no cover - interface
        ...

    def stream_answer(
        self,
        *,
        system_prompt: str,
        msc: str,
        user_query: str,
    ) -> Iterator[str]:
        """Stream the final ANSWER verdict as text chunks (§4.3.2).

        Default implementation falls back to a single-shot buffered call;
        concrete providers should override for true streaming when the
        backend supports it.
        """
        verdict = self.answer(
            system_prompt=system_prompt, msc=msc, user_query=user_query,
            allow_need_more=False,
        )
        yield verdict.answer or ""
