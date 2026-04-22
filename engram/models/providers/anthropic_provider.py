"""Anthropic (Claude) provider — used for both Core Model bootstrap and Frontier LLM.

Structured output is obtained by including the JSON schema in the system
prompt and parsing the response with `CoreModelProvider.extract_json`.

Every call is wrapped with:
  - bounded timeout (client-level via anthropic SDK + wall-clock guard)
  - exponential-backoff retry (3 attempts; retries transient API errors)
  - circuit breaker (5 failures / 30s cooldown) keyed "anthropic"

Retryable errors: anthropic.APIError subclasses that indicate transient
issues (APIConnectionError, APITimeoutError, RateLimitError,
InternalServerError). Client errors (400/401/403/422) are not retried.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Iterator

import anthropic

from engram.config import CoreModelConfig, FrontierLlmConfig
from engram.models.core import CompletionResult, CoreModelError, CoreModelProvider
from engram.models.frontier import FrontierLLMProvider, FrontierVerdict
from engram.resilience import resilient

log = logging.getLogger(__name__)


_RETRYABLE = (
    anthropic.APIConnectionError,
    anthropic.APITimeoutError,
    anthropic.RateLimitError,
    anthropic.InternalServerError,
)


_CORE_SYSTEM_SUFFIX = (
    "\n\nYou must respond with a single JSON object that matches the output schema. "
    "Emit only JSON — no prose, no markdown fences, no commentary."
)

_FRONTIER_SYSTEM = (
    "You are the final-answer generator in the Engram memory system. You receive a "
    "Minimal Sufficient Context (MSC) block and a user query.\n\n"
    "Emit exactly one JSON object, no prose, no markdown fences:\n"
    "{\n"
    '  "verdict": "ANSWER" | "NEED_MORE",\n'
    '  "answer": "final answer text (required when verdict=ANSWER)",\n'
    '  "reason": "why more context is needed (required when verdict=NEED_MORE)",\n'
    '  "suggested_queries": ["focused vector query", "..."],\n'
    '  "suggested_depth": "L1" | "L2" | "L3" | "L4"\n'
    "}\n\n"
    "Use ANSWER when the MSC contains sufficient information. Use NEED_MORE only if "
    "a specific missing fact blocks the answer; cite what is missing in `reason`.\n"
    "Do not fabricate citations — only reference information present in the MSC.\n"
    "Annotations in square brackets like [ACTIVE], [HISTORICAL: ...], "
    "[LOW_CONFIDENCE: 0.45] tell you the status of each retrieved memory; prefer "
    "ACTIVE memories and note uncertainty when using LOW_CONFIDENCE data."
)


class AnthropicCoreProvider(CoreModelProvider):
    def __init__(self, cfg: CoreModelConfig) -> None:
        if not cfg.api_key:
            raise RuntimeError("core_model.api_key is not set")
        self.cfg = cfg
        self._client = anthropic.Anthropic(
            api_key=cfg.api_key,
            timeout=cfg.timeout_seconds,
            max_retries=0,  # we handle retries via @resilient
        )

    @resilient(
        breaker="anthropic_core",
        failure_threshold=5,
        cool_down=30.0,
        max_attempts=3,
        initial_delay=0.5,
        max_delay=8.0,
        retry_on=_RETRYABLE,
        log_context="anthropic_core.complete",
    )
    def _call_messages(
        self, *, system: str, user: str, max_tokens: int, temperature: float
    ) -> anthropic.types.Message:
        return self._client.messages.create(
            model=self.cfg.model_path,
            max_tokens=max_tokens,
            temperature=temperature,
            system=system,
            messages=[{"role": "user", "content": user}],
        )

    def complete(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        output_schema: dict[str, Any] | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> CompletionResult:
        sys_text = system_prompt + _CORE_SYSTEM_SUFFIX
        if output_schema is not None:
            sys_text += "\n\nOutput schema:\n" + json.dumps(output_schema, indent=2)
        started = time.perf_counter()
        try:
            msg = self._call_messages(
                system=sys_text,
                user=user_prompt,
                max_tokens=max_tokens or self.cfg.max_tokens,
                temperature=self.cfg.temperature if temperature is None else temperature,
            )
        except anthropic.APIError as err:
            raise CoreModelError(f"Anthropic API error: {err}") from err
        latency_ms = (time.perf_counter() - started) * 1000
        parts = [blk.text for blk in msg.content if getattr(blk, "type", None) == "text"]
        raw_text = "".join(parts)
        try:
            output = self.extract_json(raw_text)
        except CoreModelError:
            # One retry with an explicit error-correction nudge.
            log.info("anthropic_core: malformed JSON, retrying with correction prompt")
            retry_user = (
                f"{user_prompt}\n\nYour previous response was not valid JSON. "
                f"Respond with ONLY the JSON object, no prose or fences."
            )
            msg = self._call_messages(
                system=sys_text,
                user=retry_user,
                max_tokens=max_tokens or self.cfg.max_tokens,
                temperature=0.0,
            )
            parts = [blk.text for blk in msg.content if getattr(blk, "type", None) == "text"]
            raw_text = "".join(parts)
            output = self.extract_json(raw_text)
            latency_ms = (time.perf_counter() - started) * 1000
        return CompletionResult(
            output=output,
            raw_text=raw_text,
            tokens_in=getattr(msg.usage, "input_tokens", None),
            tokens_out=getattr(msg.usage, "output_tokens", None),
            latency_ms=latency_ms,
        )


class AnthropicFrontierProvider(FrontierLLMProvider):
    def __init__(self, cfg: FrontierLlmConfig) -> None:
        if not cfg.api_key:
            raise RuntimeError("frontier_llm.api_key is not set")
        self.cfg = cfg
        self._client = anthropic.Anthropic(
            api_key=cfg.api_key,
            timeout=60.0,
            max_retries=0,  # we handle retries via @resilient
        )

    @resilient(
        breaker="anthropic_frontier",
        failure_threshold=5,
        cool_down=30.0,
        max_attempts=3,
        initial_delay=0.5,
        max_delay=8.0,
        retry_on=_RETRYABLE,
        log_context="anthropic_frontier.answer",
    )
    def _call_messages(
        self, *, system: str, user: str,
    ) -> anthropic.types.Message:
        return self._client.messages.create(
            model=self.cfg.model_path,
            max_tokens=self.cfg.max_tokens,
            temperature=self.cfg.temperature,
            system=system,
            messages=[{"role": "user", "content": user}],
        )

    def answer(
        self,
        *,
        system_prompt: str,
        msc: str,
        user_query: str,
        allow_need_more: bool = True,
    ) -> FrontierVerdict:
        system_text = (system_prompt or "") + "\n\n" + _FRONTIER_SYSTEM
        if not allow_need_more:
            system_text += (
                "\n\nThis is the final call. You MUST emit ANSWER with a best-effort "
                "answer even if context is incomplete."
            )
        user_text = f"<msc>\n{msc}\n</msc>\n\n<user_query>\n{user_query}\n</user_query>"
        started = time.perf_counter()
        msg = self._call_messages(system=system_text, user=user_text)
        latency_ms = (time.perf_counter() - started) * 1000
        raw_text = "".join(
            blk.text for blk in msg.content if getattr(blk, "type", None) == "text"
        )
        try:
            data = CoreModelProvider.extract_json(raw_text)
        except CoreModelError as err:
            raise CoreModelError(
                f"frontier returned unparseable output: {raw_text[:200]!r}"
            ) from err
        if not isinstance(data, dict) or data.get("verdict") not in {"ANSWER", "NEED_MORE"}:
            raise CoreModelError(f"frontier returned invalid verdict: {raw_text[:200]!r}")
        return FrontierVerdict(
            verdict=data["verdict"],
            answer=data.get("answer"),
            reason=data.get("reason"),
            suggested_queries=data.get("suggested_queries") or [],
            suggested_depth=data.get("suggested_depth"),
            tokens_in=getattr(msg.usage, "input_tokens", None),
            tokens_out=getattr(msg.usage, "output_tokens", None),
            latency_ms=latency_ms,
        )

    def stream_answer(  # type: ignore[override]
        self, *, system_prompt: str, msc: str, user_query: str,
    ) -> Iterator[str]:
        """Stream the final ANSWER as text chunks (§4.3.2).

        Caller must invoke this only after a buffered `answer()` returned
        verdict=ANSWER. We pass a minimal system prompt so Claude emits plain
        text rather than JSON on the second pass.
        """
        system_text = (
            (system_prompt or "") + "\n\nRespond with the final answer text only. "
            "Do not emit JSON; do not prefix with 'ANSWER:'; do not use markdown fences."
        )
        user_text = (
            f"<msc>\n{msc}\n</msc>\n\n<user_query>\n{user_query}\n</user_query>"
        )
        with self._client.messages.stream(
            model=self.cfg.model_path,
            max_tokens=self.cfg.max_tokens,
            temperature=self.cfg.temperature,
            system=system_text,
            messages=[{"role": "user", "content": user_text}],
        ) as stream:
            for text in stream.text_stream:
                yield text


# Historical note: the `build_*` factories used to live here and only knew
# about Anthropic. Dispatch now lives in `engram/models/providers/__init__.py`
# so multiple provider backends can share the entry point.
