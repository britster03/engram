"""Opt-in real Temporal delivery/retry against explicitly isolated services."""

import asyncio
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from types import SimpleNamespace

import pytest
from temporalio.client import Client
from temporalio.worker import Worker

from engram.config import EngramConfig
from engram.consolidation.canonical import overview_generation_policy
from engram.storage.memory_repository import MemoryRepository
from engram.temporal import activities, dispatcher
from engram.temporal.workflows import ConsolidationWorkflow
from engram.tenancy import current_tenant_id
from tests.postgres_support import PostgresTestStore


@pytest.mark.skipif(not os.environ.get("ENGRAM_TEST_TEMPORAL_ADDRESS"),
                    reason="requires explicitly isolated Temporal and PostgreSQL")
def test_outbox_delivery_retries_changed_sources_and_preserves_tenant(monkeypatch):
    tenant = "canonical-temporal-test"
    queue = "canonical-test-" + uuid.uuid4().hex
    store = PostgresTestStore()
    store.consolidation_task_queue = queue
    store.overview_debounce_seconds = 0
    repo = MemoryRepository(store)
    repo.ensure_schema()
    episode = repo.create_memory(memory_type="EPISODE", canonical_name="Conversation",
                                emit_projection=False, tenant_id=tenant)
    person = repo.create_memory(memory_type="ENTITY", canonical_name="Alice",
                               emit_projection=False, tenant_id=tenant)
    repo.add_hierarchy(parent_id=episode.id, child_id=person.id, emit_projection=False, tenant_id=tenant)
    claim = repo.add_claim(subject_id=person.id, predicate="HAS_ROLE", object_value="Engineer",
                          object_type="ROLE", emit_projection=False, tenant_id=tenant)
    cfg = EngramConfig.model_validate({
        "api": {"api_key": "test"}, "event_ledger": {"dsn": store.dsn},
        "canonical_memory": {"enabled": True, "allow_filesystem_runtime": False},
        "temporal": {"enabled": True, "address": os.environ["ENGRAM_TEST_TEMPORAL_ADDRESS"],
            "namespace": os.environ.get("ENGRAM_TEST_TEMPORAL_NAMESPACE", "engram-test"),
            "consolidation_task_queue": queue, "dispatch_interval_seconds": .1},
        "consolidation": {"quality_mode": "enforce", "overview_debounce_seconds": 0},
    })

    class Model:
        calls = 0

        def complete(self, **kwargs):
            assert current_tenant_id() == tenant
            self.calls += 1
            if self.calls == 1:
                # The real activity must reject stale publication, then
                # Temporal retries it using the now-current source snapshot.
                repo.append_version(person.id, body="Concurrent source change", tenant_id=tenant)
            return SimpleNamespace(output={"overview": "", "claim_ids": [str(claim.id)]}, raw_text="")

    model = Model()
    state = SimpleNamespace(cfg=cfg, control_plane=store, memory_repository=repo,
        core=model, fs=None, neo4j=None, embed=None, overview_cache=None, session_cache=None)
    monkeypatch.setattr(activities, "get_state", lambda: state)
    monkeypatch.setattr(dispatcher, "get_config", lambda: cfg)
    task = store.get_conn().execute(
        "SELECT * FROM consolidation_tasks WHERE tenant_id=? AND node_id=?", (tenant, str(episode.id))).fetchone()
    delivery = store.get_dispatch_for_aggregate("CONSOLIDATION", task["task_id"])

    async def exercise():
        client = await Client.connect(cfg.temporal.address, namespace=cfg.temporal.namespace)
        with ThreadPoolExecutor(max_workers=2) as executor:
            async with Worker(client, task_queue=queue, workflows=[ConsolidationWorkflow],
                activities=[activities.process_consolidation_activity, activities.mark_task_failed_activity],
                activity_executor=executor):
                dispatch_task = asyncio.create_task(dispatcher.run())
                try:
                    for _ in range(300):
                        row = store.get_conn().execute(
                            "SELECT status FROM consolidation_tasks WHERE task_id=?", (task["task_id"],)).fetchone()
                        if row["status"] == "COMPLETE":
                            break
                        if dispatch_task.done():
                            await dispatch_task
                        await asyncio.sleep(.1)
                    else:
                        raise AssertionError("Canonical workflow did not complete")
                    assert await asyncio.wait_for(client.get_workflow_handle(delivery["workflow_id"]).result(), 5) == "COMPLETE"
                finally:
                    dispatch_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await dispatch_task

    try:
        asyncio.run(exercise())
        assert model.calls == 2
        overview = repo.get_overview(episode.id, tenant_id=tenant, require_current=True,
            generation_policy=overview_generation_policy(cfg.consolidation, cfg.core_model))
        assert overview is not None and "Engineer" in overview.content
        assert repo.get_overview(episode.id, tenant_id="other", require_current=True) is None
        assert store.get_dispatch(delivery["dispatch_id"])["status"] == "COMPLETE"
    finally:
        store.close()
