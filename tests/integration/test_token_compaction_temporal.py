"""Run explicitly against the isolated lifecycle stack, never production."""
import asyncio
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from types import SimpleNamespace

import pytest

from engram.config import SessionCacheConfig
from engram.models.core import CoreModelError
from engram.storage.redis_cache import SessionCache
from tests.integration.test_token_compaction import manager as manager
from tests.unit.test_token_compaction import Core


@pytest.mark.skipif(not os.environ.get("LOCOMO_PROJECT", "").startswith("engram-lifecycle-"),
                    reason="requires the isolated LoCoMo Temporal stack")
def test_real_temporal_retry_survives_worker_restart(manager, monkeypatch):
    from temporalio.client import Client
    from temporalio.worker import Worker

    from engram.config import get_config
    from engram.temporal import activities
    from engram.temporal.workflows import ConsolidationWorkflow

    cfg = get_config()
    state = manager.create()
    manager.append_turn_pair(state.session_id, " a" * 3000, "reply")
    task = manager.control_plane.get_conn().execute(
        "SELECT * FROM consolidation_tasks WHERE task_type='SESSION_COMPACT'").fetchone()
    failed = threading.Event()
    class Unavailable(Core):
        def complete(self, **kwargs):
            failed.set()
            raise CoreModelError("transient compaction provider failure")
    runtime = SimpleNamespace(cfg=cfg.model_copy(update={"session": manager.session_config}),
        control_plane=manager.control_plane, session_cache=manager.cache,
        core=Unavailable(), fs=None, neo4j=None, embed=None, overview_cache=None)
    monkeypatch.setattr(activities, "get_state", lambda: runtime)

    async def exercise():
        client = await Client.connect(cfg.temporal.address, namespace=cfg.temporal.namespace)
        queue = "compaction-validation-" + uuid.uuid4().hex
        identity = "compaction-validation-" + uuid.uuid4().hex
        with ThreadPoolExecutor(max_workers=2) as executor:
            async with Worker(client, task_queue=queue, workflows=[ConsolidationWorkflow],
                              activities=[activities.process_consolidation_activity],
                              activity_executor=executor):
                handle = await client.start_workflow(ConsolidationWorkflow.run, task["task_id"],
                    id=identity, task_queue=queue, execution_timeout=timedelta(seconds=90))
                assert await asyncio.to_thread(failed.wait, 20)
            runtime.core = Core()
            runtime.session_cache = SessionCache(SessionCacheConfig(backend="memory"),
                                                  default_ttl_seconds=3600)
            async with Worker(client, task_queue=queue, workflows=[ConsolidationWorkflow],
                              activities=[activities.process_consolidation_activity],
                              activity_executor=executor):
                assert await asyncio.wait_for(handle.result(), 60) == "COMPLETE"
    asyncio.run(exercise())
    snapshot = manager.control_plane.get_session_state(state.session_id, tenant_id="_default")
    assert snapshot["compacted"] and snapshot["compacted_turns_idx_upper_bound"] == 2
    assert len(snapshot["turns"]) == 2 and len(runtime.core.calls) == 1
