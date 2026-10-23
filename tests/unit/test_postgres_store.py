from pathlib import Path

import pytest

from engram.storage.idempotency import IdempotencyConflictError
from tests.postgres_support import PostgresTestStore


def test_record_event_is_idempotent(tmp_path: Path):
    store = PostgresTestStore()
    eid1, is_new1 = store.record_event(
        pair_id="pair-1",
        session_id="sess-a",
        source="client",
        event_type="INGEST",
        payload={"hello": "world"},
    )
    eid2, is_new2 = store.record_event(
        pair_id="pair-1",
        session_id="sess-a",
        source="client",
        event_type="INGEST",
        payload={"hello": "world"},
    )
    assert is_new1 is True
    assert is_new2 is False
    assert eid1 == eid2


def test_record_event_idempotency_is_tenant_scoped(tmp_path: Path):
    store = PostgresTestStore()
    eid_a, is_new_a = store.record_event(
        pair_id="same-session-turns",
        session_id="shared-session",
        source="client",
        event_type="INGEST",
        payload={"tenant": "a"},
        tenant_id="tenant-a",
    )
    eid_b, is_new_b = store.record_event(
        pair_id="same-session-turns",
        session_id="shared-session",
        source="client",
        event_type="INGEST",
        payload={"tenant": "b"},
        tenant_id="tenant-b",
    )
    eid_a_dup, is_new_a_dup = store.record_event(
        pair_id="same-session-turns",
        session_id="shared-session",
        source="client",
        event_type="INGEST",
        payload={"tenant": "a"},
        tenant_id="tenant-a",
    )

    assert is_new_a is True
    assert is_new_b is True
    assert eid_a != eid_b
    assert is_new_a_dup is False
    assert eid_a_dup == eid_a
    assert store.get_event(eid_a, tenant_id="tenant-b") is None


def test_strict_retry_rejects_changed_payload_and_remains_tenant_scoped():
    store = PostgresTestStore()
    args = dict(
        pair_id="key", session_id=None, source="client", event_type="INGEST",
        require_matching_payload=True,
    )
    first, _ = store.record_event(**args, payload={"fact": "Alice"}, tenant_id="a")
    with pytest.raises(IdempotencyConflictError):
        store.record_event(**args, payload={"fact": "Bob"}, tenant_id="a")
    second, is_new = store.record_event(**args, payload={"fact": "Bob"}, tenant_id="b")
    assert is_new and second != first
    assert store.get_event(first)["payload"] == {"fact": "Alice"}


def test_concurrent_keyed_requests_create_one_event_and_dispatch():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    store = PostgresTestStore()
    barrier = Barrier(6)

    def submit(_):
        barrier.wait(timeout=10)
        return store.record_event(
            pair_id="concurrent-key", session_id=None, source="client", event_type="INGEST",
            payload={"fact": "Alice"}, require_matching_payload=True,
        )

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(submit, range(6)))
    assert len({event_id for event_id, _ in results}) == 1
    assert sum(is_new for _, is_new in results) == 1
    assert len(store.get_conn().execute("SELECT * FROM workflow_dispatches").fetchall()) == 1


def test_concurrent_key_reuse_with_different_payload_rejects_losing_request():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    store = PostgresTestStore()
    barrier = Barrier(2)

    def submit(fact):
        barrier.wait(timeout=10)
        try:
            event_id, _ = store.record_event(
                pair_id="concurrent-key", session_id=None, source="client", event_type="INGEST",
                payload={"fact": fact}, require_matching_payload=True,
            )
            return event_id, fact
        except IdempotencyConflictError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(submit, ["Alice", "Bob"]))
    accepted = [result for result in results if result is not None]
    assert len(accepted) == 1
    event_id, fact = accepted[0]
    assert store.get_event(event_id)["payload"] == {"fact": fact}
    assert len(store.get_conn().execute("SELECT * FROM workflow_dispatches").fetchall()) == 1


def test_event_lifecycle(tmp_path: Path):
    store = PostgresTestStore()
    eid, _ = store.record_event(
        pair_id="pair-2",
        session_id="sess-a",
        source="client",
        event_type="INGEST",
        payload={},
    )
    pending = store.claim_pending_events()
    assert len(pending) == 1
    claimed = store.get_event(eid)
    assert claimed is not None
    assert claimed["status"] == "PROCESSING"
    store.set_event_status(eid, "GATED_SKIP")
    ev = store.get_event(eid)
    assert ev is not None
    assert ev["status"] == "GATED_SKIP"
    assert ev["processed_at"] is not None


def test_claim_pending_events_is_atomic_in_store(tmp_path: Path):
    store = PostgresTestStore()
    for idx in range(2):
        store.record_event(
            pair_id=f"pair-{idx}",
            session_id="sess-a",
            source="client",
            event_type="INGEST",
            payload={},
        )
    first = store.claim_pending_events(limit=2)
    second = store.claim_pending_events(limit=2)
    assert len(first) == 2
    assert second == []


def test_enqueue_task_is_deduped(tmp_path: Path):
    store = PostgresTestStore()
    t1 = store.enqueue_task(node_id="mem://user/x", task_type="CONSOLIDATE_OVERVIEW")
    t2 = store.enqueue_task(node_id="mem://user/x", task_type="CONSOLIDATE_OVERVIEW")
    assert t1 is not None
    assert t2 is None
    assert store.queue_depth() == 1


def test_extraction_roundtrip(tmp_path: Path):
    store = PostgresTestStore()
    eid, _ = store.record_event(
        pair_id="p", session_id="s", source="client", event_type="INGEST", payload={}
    )
    store.save_extraction(
        eid,
        resolved_text="Alice works at Meta.",
        triplets=[
            {"subject": "Alice", "relation": "works_at", "object": "Meta", "confidence": 0.9}
        ],
        l0_abstract="Alice works at Meta.",
    )
    got = store.get_extraction(eid)
    assert got is not None
    assert got["triplets"][0]["subject"] == "Alice"
