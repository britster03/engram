from concurrent.futures import ThreadPoolExecutor

import pytest

from engram.config import SessionCacheConfig
from engram.session.manager import SessionManager
from engram.storage.chat_runs import (
    ChatConflictError,
    accept_message,
    find_run,
    find_session_run,
    finish_response,
    terminate_run,
)
from engram.storage.redis_cache import SessionCache
from engram.tenancy import set_current_tenant
from tests.postgres_support import PostgresTestStore


@pytest.fixture
def manager():
    set_current_tenant(None)
    store = PostgresTestStore()
    result = SessionManager(
        SessionCache(SessionCacheConfig(backend="memory"), default_ttl_seconds=1800),
        window_threshold_ratio=0.4,
        max_turns_before_window=50,
        session_ttl_minutes=30,
        control_plane=store,
    )
    yield result
    store.close()
    set_current_tenant(None)


def accept(manager, session_id, key):
    return accept_message(
        manager,
        session_id,
        "I live in Pune",
        tenant="_default",
        client_message_id=key,
        input_hash=key,
        automatic_memory=True,
    )


def test_concurrent_acceptance_has_one_active_response_and_one_user_turn(manager):
    session = manager.create()

    def attempt(key):
        try:
            return accept(manager, session.session_id, key)
        except ChatConflictError:
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(attempt, ["one", "two"]))
    assert sum(result is not None for result in results) == 1
    assert len(manager.get(session.session_id).turns) == 1
    assert (
        manager.control_plane.get_conn()
        .execute("SELECT count(*) AS n FROM workflow_dispatches")
        .fetchone()["n"]
        == 1
    )


def test_cancellation_fences_late_answer_and_preserves_ingestion(manager):
    session = manager.create()
    run = accept(manager, session.session_id, "one")
    terminate_run(manager.control_plane, run["run_id"], tenant="_default", cancelled=True)
    with pytest.raises(ChatConflictError):
        finish_response(manager, run["run_id"], {"answer": "Late answer"}, tenant="_default")
    assert len(manager.get(session.session_id).turns) == 1
    assert (
        manager.control_plane.get_dispatch_for_aggregate("INGEST", run["input_event_id"])["status"]
        == "PENDING"
    )
    assert (
        find_run(manager.control_plane, "_default", run_id=run["run_id"])["status"] == "CANCELLED"
    )


def test_stale_session_update_cannot_erase_a_newly_accepted_turn(manager):
    stale = manager.create()
    accept(manager, stale.session_id, "one")
    stale.status = "COMMITTED"
    manager._persist(stale)
    recovered = manager.get(stale.session_id)
    assert recovered.status == "COMMITTED"
    assert len(recovered.turns) == 1


def test_dead_worker_run_expires_and_new_message_can_proceed(manager):
    session = manager.create()
    run = accept(manager, session.session_id, "one")
    manager.control_plane.get_conn().execute(
        "UPDATE chat_runs SET deadline = CURRENT_TIMESTAMP - INTERVAL '1 second'"
    )
    assert find_run(manager.control_plane, "_default", run_id=run["run_id"])["status"] == "FAILED"
    newer = accept(manager, session.session_id, "two")
    assert newer["run_id"] != run["run_id"]
    assert len(manager.get(session.session_id).turns) == 2


def test_session_discovery_expires_a_dead_worker_and_fences_late_answers(manager):
    session = manager.create()
    run = accept(manager, session.session_id, "one")
    manager.control_plane.get_conn().execute(
        "UPDATE chat_runs SET deadline = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE run_id = ?",
        (run["run_id"],),
    )
    with pytest.raises(ChatConflictError):
        finish_response(manager, run["run_id"], {"answer": "Late answer"}, tenant="_default")
    assert find_session_run(manager.control_plane, "other", session.session_id) is None
    expired = find_session_run(manager.control_plane, "_default", session.session_id)
    assert expired["status"] == "FAILED"
    assert expired["error_code"] == "deadline_exceeded"
    newer = accept(manager, session.session_id, "two")
    assert (
        find_session_run(manager.control_plane, "_default", session.session_id)["run_id"]
        == newer["run_id"]
    )
    assert (
        find_session_run(
            manager.control_plane, "_default", session.session_id, client_message_id="one"
        )["run_id"]
        == run["run_id"]
    )


def test_same_message_race_returns_existing_run_without_another_acceptance(manager):
    session = manager.create()

    def attempt(_):
        try:
            return accept(manager, session.session_id, "same")
        except ChatConflictError as error:
            assert error.run is not None
            return error.run

    with ThreadPoolExecutor(max_workers=2) as executor:
        runs = list(executor.map(attempt, range(2)))
    assert runs[0]["run_id"] == runs[1]["run_id"]
    assert len(manager.get(session.session_id).turns) == 1


def test_pool_bounds_connections_under_parallel_reads_and_clears_tenant_scope(manager):
    from engram.storage.memory_repository import MemoryRepository

    store = manager.control_plane

    def read(_):
        return (
            store.get_conn()
            .execute("SELECT pg_backend_pid() AS pid, pg_sleep(0.01)")
            .fetchone()["pid"]
        )

    with ThreadPoolExecutor(max_workers=24) as executor:
        backends = set(executor.map(read, range(48)))
    assert len(backends) <= 12
    repo = MemoryRepository(store)
    repo.ensure_schema()
    repo.create_memory(memory_type="ENTITY", canonical_name="Private", tenant_id="tenant-one")
    assert (
        repo._tenant_connection("tenant-two")
        .execute("SELECT current_setting('app.tenant_id', true) AS tenant")
        .fetchone()["tenant"]
        == "tenant-two"
    )
    assert (
        not store.get_conn()
        .execute("SELECT current_setting('app.tenant_id', true) AS tenant")
        .fetchone()["tenant"]
    )
