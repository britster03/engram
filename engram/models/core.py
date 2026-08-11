"""CoreModelProvider — abstract interface for Core Model tasks (§8.1).

A provider exposes one method: `complete(system_prompt, user_prompt, output_schema)`
returning structured JSON matching the schema. Concrete providers wrap local
inference or remote model APIs.
"""

from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

_JSON_FENCE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)


@dataclass
class CompletionResult:
    output: dict[str, Any] | list[Any]
    raw_text: str
    tokens_in: int | None = None
    tokens_out: int | None = None
    latency_ms: float | None = None
    provider_calls: int = 1


class CoreModelError(RuntimeError):
    """Raised for provider-level failures (malformed output, timeout, API error)."""


class CoreModelProvider(ABC):
    """Contract: produce validated JSON for a task."""

    @abstractmethod
    def complete(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        output_schema: dict[str, Any] | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> CompletionResult:  # pragma: no cover - interface only
        ...

    @staticmethod
    def extract_json(text: str) -> Any:
        """Parse JSON from model output. Tolerates leading prose and ```json fences."""
        # Try a fenced JSON block first
        m = _JSON_FENCE.search(text)
        candidates = [m.group(1)] if m else []
        candidates.append(text)
        # Scan for balanced {...} or [...] blocks as a last resort
        for candidate in candidates:
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass
        # Last resort: find first { ... last }
        start = text.find("{")
        if start == -1:
            start = text.find("[")
        if start != -1:
            end = max(text.rfind("}"), text.rfind("]"))
            if end > start:
                try:
                    return json.loads(text[start : end + 1])
                except json.JSONDecodeError:
                    pass
        raise CoreModelError(f"could not parse JSON from model output: {text[:200]!r}")
