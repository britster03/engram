"""OpenAI-compatible provider — dispatch + JSON-mode handling + error mapping."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import httpx
import openai
import pytest

from engram.config import CoreModelConfig, FrontierLlmConfig
from engram.models.providers import (
    _OPENAI_COMPAT_BASES,
    build_core_provider,
    build_frontier_provider,
)


def test_build_core_defaults_to_ollama_cloud_adapter():
    cfg = CoreModelConfig(provider="ollama_cloud", api_key="x")
    p = build_core_provider(cfg)
    from engram.models.providers.ollama_cloud import OllamaCloudCoreProvider
    assert isinstance(p, OllamaCloudCoreProvider)


@pytest.mark.parametrize("provider", list(_OPENAI_COMPAT_BASES.keys()))
def test_build_core_accepts_all_openai_compat_providers(provider: str):
    cfg = CoreModelConfig(
        provider=provider,  # type: ignore[arg-type]
        api_key="sk-test",
        model_path="test-model",
    )
    p = build_core_provider(cfg)
    from engram.models.providers.openai_compat import OpenAICompatCoreProvider
    assert isinstance(p, OpenAICompatCoreProvider)


def test_build_core_rejects_unknown_provider():
    # The Literal prevents this at the Pydantic layer, but the factory
    # should also gate by string name.
    cfg = CoreModelConfig(provider="ollama_cloud", api_key="x")
    cfg.provider = "lolcats"  # type: ignore[assignment]
    with pytest.raises(NotImplementedError):
        build_core_provider(cfg)


def test_build_core_accepts_local_provider_without_loading_model():
    cfg = CoreModelConfig(provider="local", model_path="./models/engram-core-dpo")
    p = build_core_provider(cfg)
    from engram.models.providers.local_provider import LocalCoreProvider

    assert isinstance(p, LocalCoreProvider)
    assert p._model is None


def test_build_core_accepts_ollama_cloud_provider(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("OLLAMA_API_KEY", "ollama-test-key")
    cfg = CoreModelConfig(provider="ollama_cloud", model_path="gpt-oss:20b")
    p = build_core_provider(cfg)
    from engram.models.providers.ollama_cloud import OllamaCloudCoreProvider

    assert isinstance(p, OllamaCloudCoreProvider)
    assert p.cfg.api_base == "https://ollama.com/api"
    assert p.cfg.api_key == "ollama-test-key"


def test_ollama_cloud_core_complete_returns_parsed_json():
    from engram.models.providers.ollama_cloud import OllamaCloudCoreProvider

    cfg = CoreModelConfig(
        provider="ollama_cloud",
        api_base="https://ollama.com/api",
        api_key="ollama-test-key",
        model_path="gpt-oss:20b",
    )
    provider = OllamaCloudCoreProvider(cfg)
    provider._post_chat = MagicMock(return_value={
        "message": {"content": json.dumps({"store": True, "reason": "fact"})},
        "prompt_eval_count": 7,
        "eval_count": 5,
    })

    result = provider.complete(system_prompt="[GATE] ...", user_prompt="...")

    assert result.output == {"store": True, "reason": "fact"}
    assert result.tokens_in == 7
    assert result.tokens_out == 5
    payload = provider._post_chat.call_args.args[0]
    assert payload["model"] == "gpt-oss:20b"
    assert payload["format"] == "json"


def test_openai_compat_complete_returns_parsed_json():
    """The adapter should hand back parsed JSON from the chat completion."""
    from engram.models.providers.openai_compat import OpenAICompatCoreProvider

    cfg = CoreModelConfig(provider="openai", api_key="sk-test", model_path="gpt-4o-mini")
    provider = OpenAICompatCoreProvider(cfg)

    # Monkey-patch the underlying client
    fake_response = MagicMock()
    fake_response.choices = [MagicMock(message=MagicMock(
        content=json.dumps({"store": True, "reason": "fact"}),
    ))]
    fake_response.usage = MagicMock(prompt_tokens=20, completion_tokens=10)
    provider._client.chat.completions.create = MagicMock(return_value=fake_response)

    result = provider.complete(
        system_prompt="[GATE] ...",
        user_prompt="...",
    )
    assert isinstance(result.output, dict)
    assert result.output["store"] is True
    assert result.tokens_in == 20
    assert result.tokens_out == 10


def test_openai_compat_retries_on_malformed_json():
    """First response is garbage; retry fires with a correction prompt."""
    from engram.models.providers.openai_compat import OpenAICompatCoreProvider

    cfg = CoreModelConfig(provider="openai", api_key="sk", model_path="gpt-4o-mini")
    provider = OpenAICompatCoreProvider(cfg)

    bad = MagicMock()
    bad.choices = [MagicMock(message=MagicMock(content="I think the answer is yes"))]
    bad.usage = MagicMock(prompt_tokens=5, completion_tokens=5)
    good = MagicMock()
    good.choices = [MagicMock(message=MagicMock(
        content=json.dumps({"store": False, "reason": "pleasantry"}),
    ))]
    good.usage = MagicMock(prompt_tokens=6, completion_tokens=6)
    provider._client.chat.completions.create = MagicMock(side_effect=[bad, good])

    result = provider.complete(system_prompt="[GATE] ...", user_prompt="...")
    assert result.output == {"store": False, "reason": "pleasantry"}
    assert provider._client.chat.completions.create.call_count == 2


def test_openai_compat_disables_json_mode_for_ollama():
    """Ollama doesn't reliably honour response_format; our adapter should skip it."""
    from engram.models.providers.openai_compat import OpenAICompatCoreProvider

    cfg = CoreModelConfig(
        provider="ollama", api_key="sk",
        api_base="http://localhost:11434/v1",
        model_path="qwen2.5:7b",
    )
    provider = OpenAICompatCoreProvider(cfg)
    assert provider._use_json_mode is False


def test_build_frontier_works_for_openai():
    cfg = FrontierLlmConfig(provider="openai", api_key="sk", model_path="gpt-4o-mini")
    p = build_frontier_provider(cfg)
    from engram.models.providers.openai_compat import OpenAICompatFrontierProvider
    assert isinstance(p, OpenAICompatFrontierProvider)


def test_openai_compat_frontier_maps_api_errors_to_core_model_error():
    from engram.models.core import CoreModelError
    from engram.models.providers.openai_compat import OpenAICompatFrontierProvider

    cfg = FrontierLlmConfig(
        provider="openai",
        api_key="sk",
        model_path="gpt-4o-mini",
    )
    provider = OpenAICompatFrontierProvider(cfg)
    request = httpx.Request("POST", "https://example.test/v1/chat/completions")
    provider._call_chat = MagicMock(  # type: ignore[method-assign]
        side_effect=openai.APIConnectionError(request=request)
    )

    with pytest.raises(CoreModelError, match="frontier API error"):
        provider.answer(system_prompt="", msc="", user_query="test")
