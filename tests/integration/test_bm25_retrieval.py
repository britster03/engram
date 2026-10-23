"""Actual Lucene execution; explicitly isolated Neo4j only."""

import os
import uuid

import pytest
from neo4j import GraphDatabase

from engram.config import KnowledgeGraphConfig
from engram.storage.lexical import BM25_INDEX
from engram.storage.neo4j_store import Neo4jStore


@pytest.fixture
def lexical_graph():
    uri = os.environ.get("ENGRAM_TEST_NEO4J_URI")
    if not uri:
        pytest.skip("requires isolated ENGRAM_TEST_NEO4J_URI")
    driver = GraphDatabase.driver(uri, auth=None)
    store = Neo4jStore(KnowledgeGraphConfig(uri=uri))
    store._writer = store._reader = driver
    tenant = "bm25-test-" + uuid.uuid4().hex
    store.ensure_indexes()
    with driver.session() as session:
        session.run("CALL db.awaitIndex($name, 30)", name=BM25_INDEX).consume()
    yield store, tenant
    with driver.session() as session:
        session.run(
            "MATCH (n:Node) WHERE n.tenant_id IN $tenants DETACH DELETE n",
            tenants=[tenant, tenant + "other"],
        ).consume()
    driver.close()


def test_lucene_bm25_scopes_before_top_k_and_ranks_text(lexical_graph):
    store, tenant = lexical_graph
    for i in range(25):
        store.upsert_memory_projection(
            memory_id=f"foreign-{i}",
            revision=1,
            tenant_id=tenant + "other",
            properties={"search_text": "quasar " * 20},
        )
    for identity, text in [("short", "quasar"), ("long", "quasar " + "ordinary " * 60)]:
        store.upsert_memory_projection(
            memory_id=identity, revision=1, tenant_id=tenant, properties={"search_text": text}
        )
    rows = store.bm25_search("quasar", k=1, tenant_id=tenant)
    assert rows[0]["memory_id"] == "short" and rows[0]["score"] > 0
    assert all(
        r["tenant_id"] == tenant
        for r in store.bm25_search("quasar OR search_tenant:*", tenant_id=tenant)
    )


def test_lucene_revisioned_updates_and_delete_remove_old_terms(lexical_graph):
    store, tenant = lexical_graph
    store.upsert_memory_projection(
        memory_id="location", revision=1, tenant_id=tenant, properties={"search_text": "Pune"}
    )
    store.upsert_memory_projection(
        memory_id="location", revision=2, tenant_id=tenant, properties={"search_text": "Bengaluru"}
    )
    store.upsert_memory_projection(
        memory_id="location", revision=1, tenant_id=tenant, properties={"search_text": "Pune"}
    )
    assert not store.bm25_search("Pune", tenant_id=tenant)
    assert store.bm25_search("Bengaluru", tenant_id=tenant)
    store.delete_memory_projection(memory_id="location", revision=3, tenant_id=tenant)
    assert not store.bm25_search("Bengaluru", tenant_id=tenant)
