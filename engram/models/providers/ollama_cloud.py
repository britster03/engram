"""Ollama Cloud provider using the direct Ollama API.

Ollama documents direct cloud access at ``https://ollama.com/api`` with
``Authorization: Bearer $OLLAMA_API_KEY``. This adapter intentionally does
not reuse the local OpenAI-compatible ``/v1`` path because the cloud API's
native chat endpoint is ``/api/chat``.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import httpx

from engram.config import CoreModelConfig, FrontierLlmConfig
from engram.models.core import CompletionResult, CoreModelError, CoreModelProvider
from engram.models.frontier import FrontierLLMProvider, FrontierVerdict
from engram.models.semantic import validate_frontier_output
from engram.resilience import resilient

log = logging.getLogger(__name__)

_RETRYABLE = (
    httpx.ConnectError,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    httpx.RemoteProtocolError,
)

_CORE_SYSTEM_SUFFIX = (
    "\n\nYou must respond with a single JSON object that matches the output schema. "
    "Emit only JSON, with no prose, markdown fences, or commentary."
)

_FRONTIER_SYSTEM = (
    "You are the final-answer generator in the Engram memory system. You receive a "
    "Minimal Sufficient Context (MSC) block and a user query.\n\n"
    "Emit exactly one JSON object:\n"
    "{\n"
    '  "verdict": "ANSWER" | "NEED_MORE",\n'
    '  "answer": "final answer text",\n'
    '  "reason": "why more context is needed",\n'
    '  "suggested_queries": ["focused vector query"],\n'
    '  "suggested_depth": "L1" | "L2" | "L3" | "L4"\n'
    "}\n"
)


class _OllamaCloudBase:
    def __init__(self, *, api_base: str | None, api_key: str | None, timeout: float) -> None:
        if not api_key:
            raise RuntimeError("OLLAMA_API_KEY is required for provider='ollama_cloud'")
        base = (api_base or "https://ollama.com/api").rstrip("/")
        self._client = httpx.Client(
            base_url=base,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout,
        )

    @resilient(
        breaker="ollama_cloud_chat",
        failure_threshold=5, cool_down=30.0, max_attempts=3,
        initial_delay=0.5, max_delay=8.0,
        retry_on=_RETRYABLE,
        log_context="ollama_cloud.chat",
    )
    def _post_chat(self, payload: dict[str, Any]) -> dict[str, Any]:
        resp = self._client.post("/chat", json=payload)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise CoreModelError("ollama cloud returned a non-object response")
        return data

    @staticmethod
    def _message_content(data: dict[str, Any]) -> str:
        message = data.get("message")
        if isinstance(message, dict):
            content = message.get("content")
            if isinstance(content, str):
                return content
        response = data.get("response")
        if isinstance(response, str):
            return response
        return ""


class OllamaCloudCoreProvider(_OllamaCloudBase, CoreModelProvider):
    """Core Model provider backed by Ollama Cloud's native chat API."""

    def __init__(self, cfg: CoreModelConfig) -> None:
        self.cfg = cfg
        super().__init__(
            api_base=cfg.api_base,
            api_key=cfg.api_key,
            timeout=float(cfg.timeout_seconds),
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
        options: dict[str, float | int] = {
            "temperature": self.cfg.temperature if temperature is None else temperature,
            "num_predict": max_tokens or self.cfg.max_tokens,
        }
        payload: dict[str, Any] = {
            "model": self.cfg.model_path,
            "messages": [
                {"role": "system", "content": sys_text},
                {"role": "user", "content": user_prompt},
            ],
            "stream": False,
            "format": "json",
            "options": options,
        }
        try:
            data = self._post_chat(payload)
        except httpx.HTTPError as err:
            raise CoreModelError(f"ollama cloud API error: {err}") from err
        latency_ms = (time.perf_counter() - started) * 1000
        raw_text = self._message_content(data)
        try:
            output = self.extract_json(raw_text)
        except CoreModelError:
            log.info("ollama_cloud_core: malformed JSON, retrying with correction")
            retry_payload = dict(payload)
            retry_payload["messages"] = [
                {"role": "system", "content": sys_text},
                {
                    "role": "user",
                    "content": (
                        f"{user_prompt}\n\nYour previous response was not valid JSON. "
                        "Respond with ONLY the JSON object."
                    ),
                },
            ]
            retry_payload["options"] = {**payload["options"], "temperature": 0.0}
            data = self._post_chat(retry_payload)
            latency_ms = (time.perf_counter() - started) * 1000
            raw_text = self._message_content(data)
            output = self.extract_json(raw_text)

        return CompletionResult(
            output=output,
            raw_text=raw_text,
            tokens_in=data.get("prompt_eval_count"),
            tokens_out=data.get("eval_count"),
            latency_ms=latency_ms,
        )


class OllamaCloudFrontierProvider(_OllamaCloudBase, FrontierLLMProvider):
    """Frontier provider backed by Ollama Cloud's native chat API."""

    def __init__(self, cfg: FrontierLlmConfig) -> None:
        self.cfg = cfg
        super().__init__(api_base=cfg.api_base, api_key=cfg.api_key, timeout=60.0)

    def answer(
        self,
        *,
        system_prompt: str,
        msc: str,
        user_query: str,
        allow_need_more: bool = True,
    ) -> FrontierVerdict:
        sys_text = (system_prompt or "") + "\n\n" + _FRONTIER_SYSTEM
        if not allow_need_more:
            sys_text += "\n\nThis is the final call. Emit ANSWER with a best-effort answer."
        user_text = f"<msc>\n{msc}\n</msc>\n\n<user_query>\n{user_query}\n</user_query>"
        payload: dict[str, Any] = {
            "model": self.cfg.model_path,
            "messages": [
                {"role": "system", "content": sys_text},
                {"role": "user", "content": user_text},
            ],
            "stream": False,
            "format": "json",
            "options": {
                "temperature": self.cfg.temperature,
                "num_predict": self.cfg.max_tokens,
            },
        }
        started = time.perf_counter()
        parsed = None
        for attempt in range(2):
            try:
                data = self._post_chat(payload)
                raw_text = self._message_content(data)
                parsed = validate_frontier_output(
                    CoreModelProvider.extract_json(raw_text)
                )
                break
            except httpx.HTTPError as err:
                raise CoreModelError(f"ollama cloud API error: {err}") from err
            except CoreModelError:
                if attempt:
                    raise
                payload = {
                    **payload,
                    "messages": [
                        *payload["messages"],
                        {
                            "role": "user",
                            "content": (
                                "The prior response violated the schema. Return one "
                                "corrected JSON object only."
                            ),
                        },
                    ],
                    "options": {**payload["options"], "temperature": 0.0},
                }
        if parsed is None:  # pragma: no cover - loop guarantees a value or raises
            raise CoreModelError("frontier returned no validated output")
        return FrontierVerdict(
            verdict=parsed.verdict,
            answer=parsed.answer,
            reason=parsed.reason,
            suggested_queries=parsed.suggested_queries,
            suggested_depth=parsed.suggested_depth,
            tokens_in=data.get("prompt_eval_count"),
            tokens_out=data.get("eval_count"),
            latency_ms=(time.perf_counter() - started) * 1000,
        )
