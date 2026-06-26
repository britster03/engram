from pathlib import Path

from engram.storage.sqlite import SqliteStore


def test_record_event_is_idempotent(tmp_path: Path):
    store = SqliteStore(tmp_path / "ev.db")
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
    store = SqliteStore(tmp_path / "ev.db")
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


def test_event_lifecycle(tmp_path: Path):
    store = SqliteStore(tmp_path / "ev.db")
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
    store = SqliteStore(tmp_path / "ev.db")
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
    store = SqliteStore(tmp_path / "cons.db")
    t1 = store.enqueue_task(node_id="mem://user/x", task_type="CONSOLIDATE_OVERVIEW")
    t2 = store.enqueue_task(node_id="mem://user/x", task_type="CONSOLIDATE_OVERVIEW")
    assert t1 is not None
    assert t2 is None
    assert store.queue_depth() == 1


def test_extraction_roundtrip(tmp_path: Path):
    store = SqliteStore(tmp_path / "ev.db")
    eid, _ = store.record_event(
        pair_id="p", session_id="s", source="client", event_type="INGEST", payload={}
    )
    store.save_extraction(
        eid,
        resolved_text="Alice works at Meta.",
        triplets=[{"subject": "Alice", "relation": "works_at", "object": "Meta", "confidence": 0.9}],
        l0_abstract="Alice works at Meta.",
    )
    got = store.get_extraction(eid)
    assert got is not None
    assert got["triplets"][0]["subject"] == "Alice"
