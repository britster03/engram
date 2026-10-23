"""Provider dispatcher.

`build_providers(core_cfg, frontier_cfg)` returns a (core, frontier) pair
built from the configured provider name. Supported:

  - "local"           → engram.models.providers.local_provider (Transformers)
  - "openai"          → engram.models.providers.openai_compat (api.openai.com)
  - "openai_compat"   → engram.models.providers.openai_compat (any base URL)
  - "openai_responses" → OpenAI Responses API adapter (any compatible base URL)
  - "ollama"          → openai_compat with base URL http://localhost:11434/v1
  - "groq"            → openai_compat with base URL https://api.groq.com/openai/v1
  - "gemini"          → openai_compat with base URL https://generativelanguage.googleapis.com/v1beta/openai
  - "openrouter"      → openai_compat with base URL https://openrouter.ai/api/v1
  - "together"        → openai_compat with base URL https://api.together.xyz/v1
  - "deepseek"        → openai_compat with base URL https://api.deepseek.com

For providers that expose the OpenAI Chat Completions API, this sugar
lets the operator just set `provider: "groq"` in config.yaml without
having to remember the base URL.
"""

from __future__ import annotations

import os

from engram.config import CoreModelConfig, FrontierLlmConfig
from engram.models.core import CoreModelProvider
from engram.models.frontier import FrontierLLMProvider

_OPENAI_COMPAT_BASES: dict[str, str | None] = {
    "openai": None,  # default client endpoint
    "openai_compat": None,  # requires explicit api_base in config
    "ollama": "http://localhost:11434/v1",
    "groq": "https://api.groq.com/openai/v1",
    "gemini": "https://generativelanguage.googleapis.com/v1beta/openai",
    "openrouter": "https://openrouter.ai/api/v1",
    "together": "https://api.together.xyz/v1",
    "deepseek": "https://api.deepseek.com",
}

_MUSE_SPARK_MODEL = "muse-spark-1.3-contributor"
_OPENCODE_GO_BASE = "https://opencode.ai/zen/go/v1"


def _resolve_base(provider: str, configured_base: str | None) -> str | None:
    """If the config has an explicit api_base, always honour it. Otherwise
    use the canonical base for the named provider."""
    if configured_base:
        return configured_base
    return _OPENAI_COMPAT_BASES.get(provider)


def build_core_provider(cfg: CoreModelConfig) -> CoreModelProvider:
    provider = cfg.provider.lower()
    if provider == "local":
        from engram.models.providers.local_provider import LocalCoreProvider

        return LocalCoreProvider(cfg)
    if provider == "openai_responses":
        from engram.models.providers.openai_compat import OpenAIResponsesCoreProvider

        return OpenAIResponsesCoreProvider(_patch_responses_core(cfg))
    if provider in _OPENAI_COMPAT_BASES:
        from engram.models.providers.openai_compat import OpenAICompatCoreProvider

        # Swap in the resolved base URL without mutating the caller's config.
        patched = cfg.model_copy(update={"api_base": _resolve_base(provider, cfg.api_base)})
        return OpenAICompatCoreProvider(patched)
    raise NotImplementedError(
        f"core_model.provider={cfg.provider!r} is not wired. "
        f"Supported: local, openai_responses, {', '.join(_OPENAI_COMPAT_BASES)}."
    )


def build_frontier_provider(cfg: FrontierLlmConfig) -> FrontierLLMProvider:
    provider = cfg.provider.lower()
    if provider == "openai_responses":
        from engram.models.providers.openai_compat import OpenAIResponsesFrontierProvider

        return OpenAIResponsesFrontierProvider(_patch_responses_frontier(cfg))
    if provider in _OPENAI_COMPAT_BASES:
        from engram.models.providers.openai_compat import OpenAICompatFrontierProvider

        configured_base = getattr(cfg, "api_base", None)
        base = _resolve_base(provider, configured_base)
        patched = cfg.model_copy(update={"api_base": base})
        return OpenAICompatFrontierProvider(patched)
    raise NotImplementedError(
        f"frontier_llm.provider={cfg.provider!r} is not wired. "
        f"Supported: openai_responses, {', '.join(_OPENAI_COMPAT_BASES)}."
    )


def build_providers(
    core_cfg: CoreModelConfig,
    frontier_cfg: FrontierLlmConfig,
) -> tuple[CoreModelProvider, FrontierLLMProvider]:
    """Single-call helper used by `engram.deps.build_state`."""
    return build_core_provider(core_cfg), build_frontier_provider(frontier_cfg)


def _responses_connection(
    model: str,
    configured_base: str | None,
    configured_key: str | None,
) -> tuple[str | None, str | None]:
    """Resolve the canonical Muse Spark endpoint without breaking custom gateways."""
    if model != _MUSE_SPARK_MODEL:
        return configured_base, configured_key
    return (
        configured_base or _OPENCODE_GO_BASE,
        configured_key or os.environ.get("OPENCODE_GO_API_KEY"),
    )


def _patch_responses_core(cfg: CoreModelConfig) -> CoreModelConfig:
    api_base, api_key = _responses_connection(cfg.model_path, cfg.api_base, cfg.api_key)
    return cfg.model_copy(update={"api_base": api_base, "api_key": api_key})


def _patch_responses_frontier(cfg: FrontierLlmConfig) -> FrontierLlmConfig:
    api_base, api_key = _responses_connection(cfg.model_path, cfg.api_base, cfg.api_key)
    return cfg.model_copy(update={"api_base": api_base, "api_key": api_key})


__all__ = [
    "build_core_provider",
    "build_frontier_provider",
    "build_providers",
]
