"""Live Muse Spark/OpenCode Go smoke test.

Skipped unless OPENCODE_GO_API_KEY is present. The key is read only from the
environment and is never printed.
"""

from __future__ import annotations

import os

import pytest

from engram.config import CoreModelConfig
from engram.models.providers.openai_compat import OpenAIResponsesCoreProvider


@pytest.mark.e2e
def test_muse_spark_core_json_smoke():
    api_key = os.environ.get("OPENCODE_GO_API_KEY")
    if not api_key:
        pytest.skip("OPENCODE_GO_API_KEY is not set")
    provider = OpenAIResponsesCoreProvider(
        CoreModelConfig(
            provider="openai_responses",
            model_path="muse-spark-1.3-contributor",
            api_base="https://opencode.ai/zen/go/v1",
            api_key=api_key,
            max_tokens=1024,
            timeout_seconds=60,
        )
    )
    result = provider.complete(
        system_prompt="[GATE] Decide whether to store a memory.",
        user_prompt='Return {"store": true, "reason": "live smoke"} as JSON.',
        temperature=1.0,
        max_tokens=1024,
    )
    assert isinstance(result.output, dict)
    assert "store" in result.output
