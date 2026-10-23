"""Exercise the actual Cypher against an explicitly configured isolated Neo4j."""

import os
import uuid

import pytest
from neo4j import GraphDatabase, Query

from engram.retrieval.templates import run_template


@pytest.fixture
def graph():
    uri = os.environ.get("ENGRAM_TEST_NEO4J_URI")
    if not uri:
        pytest.skip("ENGRAM_TEST_NEO4J_URI must point to an isolated test Neo4j")
    driver = GraphDatabase.driver(uri, auth=None)
    tenant = "cascade-test-" + uuid.uuid4().hex

    class Graph:
        def run_template(self, cypher, params, timeout_s=None):
            with driver.session() as session:
                return session.run(Query(cypher, timeout=timeout_s), params).data()

    instance = Graph()
    instance.run_template(
        "CREATE (a:Node {tenant_id: $tenant, memory_id: 'alice', status: 'ACTIVE'}), "
        "(b:Node {tenant_id: $tenant, memory_id: 'atlas', status: 'ACTIVE'}), "
        "(c:Node {tenant_id: $tenant, memory_id: 'priya', status: 'ACTIVE'}), "
        "(x:Node {tenant_id: $other, memory_id: 'secret', status: 'ACTIVE'}), "
        "(a)-[:RELATES_TO {tenant_id: $tenant, claim_id: 'joined', status: 'ACTIVE', "
        "projected_revision: 1}]->(b), "
        "(c)-[:RELATES_TO {tenant_id: $tenant, claim_id: 'leads', status: 'ACTIVE', "
        "projected_revision: 1}]->(b), "
        "(b)-[:RELATES_TO {tenant_id: $tenant, claim_id: 'foreign-node', status: 'ACTIVE'}]->(x), "
        "(a)-[:RELATES_TO {tenant_id: $other, claim_id: 'foreign-edge', status: 'ACTIVE'}]->(c)",
        {"tenant": tenant, "other": tenant + "-other"},
    )
    yield instance, tenant
    instance.run_template(
        "MATCH (n) WHERE n.tenant_id IN $tenants DETACH DELETE n",
        {"tenants": [tenant, tenant + "-other"]},
    )
    driver.close()


def test_actual_cypher_finds_supporting_path_and_rejects_foreign_nodes_and_edges(graph):
    store, tenant = graph
    rows = run_template(
        store,
        "t_canonical_paths",
        {
            "tenant_id": tenant,
            "memory_ids": ["alice"],
            "hops": 2,
        },
        timeout_s=3,
    )
    assert any(
        {claim["claim_id"] for claim in row["claims"]} == {"joined", "leads"} for row in rows
    )
    assert all(claim["claim_id"] in {"joined", "leads"} for row in rows for claim in row["claims"])
    assert all(claim["memory_id"] in {"alice", "priya"} for row in rows for claim in row["claims"])


def test_actual_cypher_obeys_hop_and_row_limits_and_parameterizes_ids(graph):
    store, tenant = graph
    rows = run_template(
        store,
        "t_canonical_paths",
        {
            "tenant_id": tenant,
            "memory_ids": ["alice"],
            "hops": 1,
            "limit": 1,
        },
        timeout_s=3,
    )
    assert len(rows) == 1 and len(rows[0]["claims"]) == 1
    rows = run_template(
        store,
        "t_canonical_paths",
        {
            "tenant_id": tenant,
            "memory_ids": ["alice' MATCH (n) RETURN n //"],
        },
        timeout_s=3,
    )
    assert rows == []
