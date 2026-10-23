from __future__ import annotations

from contextlib import contextmanager

from engram.storage.postgres import PostgresStore


class _Cursor:
    rowcount = 1

    def fetchone(self):
        return {"dispatch_id": "dsp-test"}


class _RecordingConnection:
    def __init__(self):
        self.sql = ""
        self.params = None

    def execute(self, sql, params):
        self.sql = sql
        self.params = params
        return _Cursor()

    def executemany(self, sql, params):
        self.sql = sql
        self.params = params
        return _Cursor()


class _RecordingTransactionStore:
    def __init__(self):
        self.connection = _RecordingConnection()

    @contextmanager
    def transaction(self):
        yield self.connection


def test_dispatch_uses_configured_temporal_task_queues():
    store = object.__new__(PostgresStore)
    store.ingest_task_queue = "staging-ingest"
    store.consolidation_task_queue = "staging-consolidation"

    ingest_conn = _RecordingConnection()
    store._enqueue_dispatch_in_tx(
        ingest_conn,
        "INGEST",
        "evt-1",
        "acme",
    )
    assert ingest_conn.params[5] == "staging-ingest"

    consolidation_conn = _RecordingConnection()
    store._enqueue_dispatch_in_tx(
        consolidation_conn,
        "CONSOLIDATION",
        "task-1",
        "acme",
    )
    assert consolidation_conn.params[5] == "staging-consolidation"


def test_release_dispatch_does_not_reopen_completed_work():
    store = object.__new__(PostgresStore)
    transaction_store = _RecordingTransactionStore()
    store.transaction = transaction_store.transaction

    store.release_dispatch("dsp-1", "start response timed out", claim_token="claim-1")

    assert "status = 'DISPATCHING'" in transaction_store.connection.sql
    assert "claim_token = ?" in transaction_store.connection.sql
    assert transaction_store.connection.params == ("start response timed out", "dsp-1", "claim-1")


def test_linked_entities_use_postgres_upsert():
    store = object.__new__(PostgresStore)
    transaction_store = _RecordingTransactionStore()
    store.transaction = transaction_store.transaction
    rows = [("evt-1", "acme", 0, "mem://alice", "mem://bob")]

    store.save_linked_entities(rows)

    assert "INSERT OR REPLACE" not in transaction_store.connection.sql
    assert "ON CONFLICT (event_id, triplet_idx) DO UPDATE" in (transaction_store.connection.sql)
    assert transaction_store.connection.params == rows
