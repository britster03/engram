import json
from pathlib import Path

import pytest

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


def test_event_status_counts_can_be_tenant_scoped(tmp_path: Path):
    store = SqliteStore(tmp_path / "ev.db")
    for tenant_id in ("tenant-a", "tenant-b"):
        event_id, _ = store.record_event(
            pair_id=f"failed-{tenant_id}",
            session_id="s",
            source="test",
            event_type="INGEST",
            payload={},
            tenant_id=tenant_id,
        )
        store.set_event_status(event_id, "FAILED", tenant_id=tenant_id)
    assert store.count_events_by_status("FAILED") == 2
    assert store.count_events_by_status("FAILED", tenant_id="tenant-a") == 1


def test_get_event_readiness_is_exact_ordered_and_tenant_scoped(tmp_path: Path):
    store = SqliteStore(tmp_path / "ev.db")
    eid_a, _ = store.record_event(
        pair_id="a",
        session_id="s",
        source="test",
        event_type="INGEST",
        payload={},
        tenant_id="tenant-a",
    )
    eid_b, _ = store.record_event(
        pair_id="b",
        session_id="s",
        source="test",
        event_type="INGEST",
        payload={},
        tenant_id="tenant-b",
    )
    store.fs_outbox_write(eid_a, "mem://user/episodes/a.md", tenant_id="tenant-a")
    store.fs_outbox_mark(eid_a, "INDEXED")
    store.set_event_status(eid_a, "COMPLETE", tenant_id="tenant-a")

    rows = store.get_event_readiness(["missing", eid_a, eid_b], tenant_id="tenant-a")
    assert [row["event_id"] for row in rows] == [eid_a]
    assert rows[0]["outbox_state"] == "INDEXED"
    assert rows[0]["source_uri"] == "mem://user/episodes/a.md"


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


def test_directory_refresh_coalesces_by_child_generation(tmp_path: Path):
    store = SqliteStore(tmp_path / "cons.db")
    first = store.enqueue_directory_refresh(
        node_id="mem://user/entities", child_signature="sig-a"
    )
    duplicate = store.enqueue_directory_refresh(
        node_id="mem://user/entities", child_signature="sig-a"
    )
    changed = store.enqueue_directory_refresh(
        node_id="mem://user/entities", child_signature="sig-b"
    )
    assert first is not None and duplicate == first and changed == first
    rows = store.get_conn().execute(
        "SELECT generation, child_signature FROM consolidation_tasks "
        "WHERE task_type = 'REFRESH_DIRECTORY'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["generation"] == 2
    assert rows[0]["child_signature"] == "sig-b"


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


def test_ingest_stage_is_monotonic_and_persists_outputs(tmp_path: Path):
    store = SqliteStore(tmp_path / "ev.db")
    eid, _ = store.record_event(
        pair_id="stage", session_id="s", source="test", event_type="INGEST", payload={}
    )
    store.advance_event_stage(
        eid,
        "GATED",
        tenant_id="_default",
        gate_output={"store": True, "reason": "fact"},
    )
    store.advance_event_stage(
        eid,
        "LINKED",
        tenant_id="_default",
        link_output=[{"slug": "alice", "display_name": "Alice", "matched_uri": None}],
    )
    stage = store.get_event_stage(eid, tenant_id="_default")
    assert stage["completed_stage"] == "LINKED"
    assert stage["gate_output"]["store"] is True
    assert stage["link_output"][0]["slug"] == "alice"
    with pytest.raises(RuntimeError, match="stage regression"):
        store.advance_event_stage(eid, "EXTRACTED", tenant_id="_default")


def test_artifact_identity_is_immutable_and_readiness_is_aggregated(tmp_path: Path):
    store = SqliteStore(tmp_path / "ev.db")
    eid, _ = store.record_event(
        pair_id="artifacts", session_id="s", source="test", event_type="INGEST", payload={}
    )
    store.upsert_ingest_artifact(
        event_id=eid,
        tenant_id="_default",
        artifact_type="DOCUMENT",
        source_uri="mem://user/episodes/e.md",
        artifact_id="stable-id",
        content_hash="abc",
        source_session_id="session-1",
        source_turn_ids=["D1:1", "D1:2"],
        confidence=0.9,
        extractor_version="core-v1",
    )
    store.mark_event_artifacts_kg(eid, "COMMITTED")
    rows = store.get_event_readiness([eid], tenant_id="_default")
    assert rows[0]["artifact_count"] == 1
    assert rows[0]["filesystem_ready_count"] == 1
    assert rows[0]["kg_ready_count"] == 1
    artifact = store.list_ingest_artifacts(eid, tenant_id="_default")[0]
    assert json.loads(artifact["source_turn_ids"]) == ["D1:1", "D1:2"]
    assert artifact["extractor_version"] == "core-v1"
    with pytest.raises(RuntimeError, match="identity changed"):
        store.upsert_ingest_artifact(
            event_id=eid,
            tenant_id="_default",
            artifact_type="DOCUMENT",
            source_uri="mem://user/episodes/e.md",
            artifact_id="different-id",
            content_hash="abc",
        )
