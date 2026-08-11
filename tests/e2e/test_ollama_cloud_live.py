"""Live Ollama Cloud smoke test.

Skipped unless OLLAMA_API_KEY is present. The key is read only from the
environment and is never printed.
"""

from __future__ import annotations

import os

import pytest

from engram.config import CoreModelConfig
from engram.models.providers.ollama_cloud import OllamaCloudCoreProvider


@pytest.mark.e2e
def test_ollama_cloud_core_json_smoke():
    api_key = os.environ.get("OLLAMA_API_KEY")
    if not api_key:
        pytest.skip("OLLAMA_API_KEY is not set")
    provider = OllamaCloudCoreProvider(
        CoreModelConfig(
            provider="ollama_cloud",
            model_path="gemma4:31b",
            api_base="https://ollama.com/api",
            api_key=api_key,
            max_tokens=128,
            timeout_seconds=60,
        )
    )
    result = provider.complete(
        system_prompt="[GATE] Decide whether to store a memory.",
        user_prompt='Return {"store": true, "reason": "live smoke"} as JSON.',
        temperature=0.0,
        max_tokens=128,
    )
    assert isinstance(result.output, dict)
    assert "store" in result.output
