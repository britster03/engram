import pytest

from engram.config import EngramConfig, SessionCacheConfig, SessionConfig
from engram.consolidation.worker import ConsolidationContext, _dispatch
from engram.session.manager import SessionManager, compact_session
from engram.storage.chat_runs import accept_message, finish_response
from engram.storage.redis_cache import SessionCache
from engram.tenancy import Tenant, TenantQuotas, set_current_tenant
from tests.postgres_support import PostgresTestStore
from tests.unit.test_token_compaction import Core


@pytest.fixture
def manager():
    store = PostgresTestStore()
    cache = SessionCache(SessionCacheConfig(backend="memory"), default_ttl_seconds=3600)
    cfg = SessionConfig(compaction_trigger_tokens=2048, compaction_summary_tokens=64,
                        compaction_input_tokens=4096)
    result = SessionManager(cache, session_config=cfg, session_ttl_minutes=60, control_plane=store)
    yield result
    store.close()


def tasks(manager):
    return manager.control_plane.get_conn().execute("SELECT * FROM consolidation_tasks WHERE task_type='SESSION_COMPACT'").fetchall()


def test_acceptance_enqueues_atomically_and_coalesces(manager):
    state = manager.create()
    manager.append_turn_pair(state.session_id, " a" * 3000, "reply")
    manager.append_turn_pair(state.session_id, "new", "reply")
    assert len(tasks(manager)) == 1
    task = tasks(manager)[0]
    assert task["requested_generation"] == 2
    dispatch = manager.control_plane.get_dispatch_for_aggregate("CONSOLIDATION", task["task_id"])
    assert dispatch["status"] == "PENDING"


def test_outbox_failure_rolls_back_accepted_turns(manager, monkeypatch):
    state = manager.create()
    monkeypatch.setattr(manager.control_plane, "enqueue_task", lambda **k: (_ for _ in ()).throw(RuntimeError("outbox failed")))
    with pytest.raises(RuntimeError, match="outbox failed"):
        manager.append_turn_pair(state.session_id, " a" * 3000, "reply")
    assert manager.get(state.session_id).turns == []


def test_worker_handles_session_task_in_canonical_mode_and_retries(manager):
    state = manager.create()
    manager.append_turn_pair(state.session_id, " a" * 3000, "reply")
    task = dict(tasks(manager)[0])
    cfg = EngramConfig(api={"api_key": "test-key"}, event_ledger={"dsn": "postgresql://test:test/test"},
        session=manager.session_config, session_cache={"backend": "memory"})
    cfg.canonical_memory.enabled = True
    ctx = ConsolidationContext(cfg, manager.control_plane, None, None, Core(), None,
                               session_cache=manager.cache)
    _dispatch(ctx, task)
    current = manager.get(state.session_id)
    assert current.compacted and current.compacted_turns_idx_upper_bound == 2
    assert len(current.turns) == 2
    _dispatch(ctx, task)
    assert current.compacted == manager.get(state.session_id).compacted


def test_running_exchange_is_not_compacted_until_response_finishes(manager):
    state = manager.create()
    run = accept_message(manager, state.session_id, " a" * 3000, tenant="_default",
        client_message_id="one", input_hash="one", automatic_memory=True)
    compact_session(manager, manager.get(state.session_id), Core(), manager.control_plane)
    assert manager.get(state.session_id).compacted_turns_idx_upper_bound == 0
    finish_response(manager, run["run_id"], {"answer": "reply"}, tenant="_default")
    compact_session(manager, manager.get(state.session_id), Core(), manager.control_plane)
    assert manager.get(state.session_id).compacted_turns_idx_upper_bound == 2


def test_identical_session_ids_are_tenant_isolated(manager):
    for tid in ["first", "second"]:
        set_current_tenant(Tenant(tid, tid, [], TenantQuotas(), "ACTIVE"))
        manager.append_turn_pair("shared", " a" * 3000, "reply")
    set_current_tenant(Tenant("first", "first", [], TenantQuotas(), "ACTIVE"))
    compact_session(manager, manager.get("shared"), Core(), manager.control_plane)
    first = manager.get("shared")
    set_current_tenant(Tenant("second", "second", [], TenantQuotas(), "ACTIVE"))
    assert first.compacted and manager.get("shared").compacted is None

def test_temporal_compaction_retry_recovers_after_worker_restart(manager, monkeypatch):
    from types import SimpleNamespace

    from engram.models.core import CoreModelError
    from engram.temporal import activities
    state = manager.create()
    manager.append_turn_pair(state.session_id, " a" * 3000, "reply")
    task = dict(tasks(manager)[0])
    cfg = EngramConfig(api={"api_key": "test-key"}, event_ledger={"dsn": "postgresql://test:test/test"},
        session=manager.session_config, session_cache={"backend": "memory"})
    class Unavailable(Core):
        def complete(self, **kwargs):
            raise CoreModelError("temporary provider failure")
    runtime = SimpleNamespace(cfg=cfg, control_plane=manager.control_plane,
        session_cache=manager.cache, core=Unavailable(), fs=None, neo4j=None,
        embed=None, overview_cache=None)
    monkeypatch.setattr(activities, "get_state", lambda: runtime)
    with pytest.raises(CoreModelError, match="temporary"):
        activities.process_consolidation_activity(task["task_id"])
    assert manager.get(state.session_id).compacted is None
    runtime.session_cache = SessionCache(cfg.session_cache, default_ttl_seconds=3600)
    runtime.core = Core()
    assert activities.process_consolidation_activity(task["task_id"]) == "COMPLETE"
    assert activities.process_consolidation_activity(task["task_id"]) == "ALREADY_PROCESSED"
    assert len(runtime.core.calls) == 1
    assert manager.control_plane.get_session_state(state.session_id, tenant_id="_default")["compacted"]


def test_database_concurrent_append_survives_compaction(manager):
    state = manager.create()
    manager.append_turn_pair(state.session_id, " a" * 3000, "reply")
    core = Core(callback=lambda: manager.append_turn_pair(state.session_id, "concurrent", "reply"))
    result = compact_session(manager, manager.get(state.session_id), core)
    assert result.turns[-2].content == "concurrent"
    persisted = manager.control_plane.get_session_state(state.session_id, tenant_id="_default")
    assert len(persisted["turns"]) == 4
    assert persisted["compacted_turns_idx_upper_bound"] == 2


def test_database_ingestion_handoff_failure_rolls_back_summary(manager, monkeypatch):
    state = manager.create()
    manager.append_turn_pair(state.session_id, " a" * 3000, "reply")
    from engram.session import manager as module
    def fail(*args, **kwargs):
        raise RuntimeError("handoff unavailable")
    monkeypatch.setattr(module, "_reenqueue_for_ingest", fail)
    with pytest.raises(RuntimeError, match="handoff"):
        compact_session(manager, manager.get(state.session_id), Core())
    persisted = manager.control_plane.get_session_state(state.session_id, tenant_id="_default")
    assert persisted["compacted"] is None and persisted["compacted_turns_idx_upper_bound"] == 0
def test_worker_uses_durable_snapshot_when_cache_is_stale(manager):
    state = manager.create()
    stale = state.to_payload()
    manager.append_turn_pair(state.session_id, " a" * 3000, "reply")
    task = dict(tasks(manager)[0])
    # A reader that started before the append can fill Redis after invalidation.
    manager.cache.set(state.session_id, stale, ttl_seconds=3600)
    cfg = EngramConfig(api={"api_key": "test-key"}, event_ledger={"dsn": "postgresql://test:test/test"},
        session=manager.session_config, session_cache={"backend": "memory"})
    ctx = ConsolidationContext(cfg, manager.control_plane, None, None, Core(), None,
                               session_cache=manager.cache)
    _dispatch(ctx, task)
    snapshot = manager.control_plane.get_session_state(state.session_id, tenant_id="_default")
    assert snapshot["compacted"] and snapshot["compacted_turns_idx_upper_bound"] == 2
