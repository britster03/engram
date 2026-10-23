"""Explicit live regression; uses only an isolated test schema and source records."""

import json
import os
from pathlib import Path

import pytest

from engram.config import SessionCacheConfig
from engram.retrieval.orchestrator import run_query
from engram.storage.memory_repository import MemoryRepository
from engram.storage.redis_cache import SessionCache
from tests.postgres_support import PostgresTestStore
from tests.unit.test_locomo_activity_regression import NOISE, ORIGINALS, QUESTION


@pytest.mark.skipif(
    os.environ.get("ENGRAM_TEST_LIVE_MODELS") != "1"
    or not os.environ.get("LOCOMO_PROJECT", "").startswith("engram-lifecycle-"),
    reason="explicit isolated live-model regression only",
)
def test_live_supported_activity_list():
    from engram.deps import get_state, make_orchestrator_context

    state = get_state()
    store = PostgresTestStore()
    try:
        ctx = make_orchestrator_context(state)
        ctx.control_plane = store
        ctx.memory_repository = MemoryRepository(store)
        ctx.session_cache = SessionCache(
            SessionCacheConfig(backend="memory"), default_ttl_seconds=3600
        )
        for identity, session, text in [*ORIGINALS, ("D19:1", "s019", NOISE)]:
            store.record_event(
                pair_id=identity,
                session_id=session,
                event_type="INGEST",
                initial_status="COMPLETE",
                source="live-regression",
                payload={
                    "turn_pair": {
                        "user": {"content": text, "turn_idx": 0},
                        "assistant": {"content": ""},
                    },
                    "session_id": session,
                },
                tenant_id="_default",
            )
        ctx.session_cache.set(
            "s019",
            {
                "session_id": "s019",
                "compacted": NOISE,
                "turns": [
                    {"role": "user", "content": NOISE, "turn_idx": 0, "timestamp": "2023-10-22"}
                ],
            },
            ttl_seconds=3600,
        )
        result = run_query(ctx, session_id="s019", query=QUESTION, max_depth="L2")
        artifact = {
            "answer": result.answer,
            "answerability": result.answerability,
            "metadata": result.retrieval_metadata.to_dict(),
        }
        Path("/results/live_activity_regression.json").write_text(
            json.dumps(artifact, indent=2, default=str)
        )
        assert all(
            word in result.answer.lower() for word in ["pottery", "camping", "painting", "swimming"]
        )
        assert "L2" in result.retrieval_metadata.levels_visited
    finally:
        store.close()
