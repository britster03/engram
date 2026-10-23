from types import SimpleNamespace

import pytest

from engram.search_backfill import backfill_search
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from engram.storage.memory_repository import MemoryRepository
from engram.temporal.activities import _publish_memory_projection
from tests.postgres_support import PostgresTestStore


def test_search_backfill_is_scoped_resumable_and_idempotent():
    store = PostgresTestStore()
    try:
        repo = MemoryRepository(store)
        repo.ensure_schema()
        for i in range(3):
            repo.create_memory(memory_type="ENTITY", canonical_name=f"Alice {i}", tenant_id="one")
        repo.create_memory(memory_type="ENTITY", canonical_name="Foreign", tenant_id="two")
        first = backfill_search(repo, tenant_id="one", limit=2)
        assert first["submitted"] == 2 and first["next_cursor"]
        second = backfill_search(repo, tenant_id="one", cursor=first["next_cursor"], limit=2)
        assert second["submitted"] == 1 and second["next_cursor"] is None
        backfill_search(repo, tenant_id="one", limit=2)
        rows = (
            store.get_conn()
            .execute(
                "SELECT tenant_id, id FROM canonical_mutations WHERE mutation_type = 'PROJECTION_REINDEX_BM25_V1'"
            )
            .fetchall()
        )
        assert len(rows) == 3 and {r["tenant_id"] for r in rows} == {"one"}
    finally:
        store.close()


def test_embedding_outage_still_publishes_corrected_bm25_text():
    class FailingEmbed:
        def embed(self, text):
            raise ConnectionError("embedding unavailable")

    graph = InMemoryKnowledgeGraph()
    projection = {
        "node_properties": {"canonical_name": "Alice", "status": "ACTIVE"},
        "current_version": {"body": "Alice lives in Bengaluru"},
        "claims": [],
    }
    with pytest.raises(ConnectionError):
        _publish_memory_projection(
            graph,
            projection,
            SimpleNamespace(embed=FailingEmbed()),
            memory_id="alice",
            tenant_id="one",
            revision=2,
        )
    assert graph.bm25_search("Bengaluru", tenant_id="one")[0]["memory_id"] == "alice"
