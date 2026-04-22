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
    store.set_event_status(eid, "GATED_SKIP")
    ev = store.get_event(eid)
    assert ev is not None
    assert ev["status"] == "GATED_SKIP"
    assert ev["processed_at"] is not None


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
