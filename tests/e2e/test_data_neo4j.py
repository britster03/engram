import os
import uuid

import pytest
from neo4j import Driver, GraphDatabase

NEO4J_PASSWORD = os.environ.get("NEO4J_ADMIN_PASSWORD", "engram-admin")
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="module")
def neo4j_driver() -> Driver:
    driver = GraphDatabase.driver(NEO4J_URI, auth=("neo4j", NEO4J_PASSWORD))
    try:
        with driver.session() as session:
            result = session.run("RETURN 1 AS test")
            assert result.single()["test"] == 1
    except Exception as err:
        driver.close()
        pytest.skip(f"Neo4j connection unavailable: {err}")
    yield driver
    driver.close()


@pytest.fixture
def qa_run_id(neo4j_driver: Driver) -> str:
    run_id = f"qa-{uuid.uuid4().hex}"
    try:
        yield run_id
    finally:
        with neo4j_driver.session() as session:
            session.run(
                "MATCH (n:Node {qa_run_id: $run_id}) DETACH DELETE n",
                run_id=run_id,
            )


class TestNeo4jData:
    def test_neo4j_module_importable(self):
        assert GraphDatabase is not None

    def test_neo4j_connection(self, neo4j_driver: Driver):
        with neo4j_driver.session() as session:
            result = session.run("RETURN 1 AS test")
            assert result.single()["test"] == 1

    def test_tenant_scoped_memuri_nodes(self, neo4j_driver: Driver, qa_run_id: str):
        """Same tenant-relative mem:// URI can exist independently per tenant."""
        source_uri = "mem://qa/shared-memory"
        with neo4j_driver.session() as session:
            session.run(
                """
                CREATE (:Node {
                    qa_run_id: $run_id,
                    tenant_id: 'tenant-a',
                    source_uri: $source_uri,
                    uri: $source_uri,
                    node_type: 'FACT',
                    status: 'ACTIVE',
                    l0_abstract: 'tenant a fact'
                })
                CREATE (:Node {
                    qa_run_id: $run_id,
                    tenant_id: 'tenant-b',
                    source_uri: $source_uri,
                    uri: $source_uri,
                    node_type: 'FACT',
                    status: 'ACTIVE',
                    l0_abstract: 'tenant b fact'
                })
                """,
                run_id=qa_run_id,
                source_uri=source_uri,
            )
            both = session.run(
                """
                MATCH (n:Node {qa_run_id: $run_id, source_uri: $source_uri})
                RETURN count(n) AS count
                """,
                run_id=qa_run_id,
                source_uri=source_uri,
            ).single()["count"]
            tenant_a = session.run(
                """
                MATCH (n:Node {
                    qa_run_id: $run_id,
                    tenant_id: 'tenant-a',
                    source_uri: $source_uri
                })
                RETURN count(n) AS count, collect(n.l0_abstract) AS abstracts
                """,
                run_id=qa_run_id,
                source_uri=source_uri,
            ).single()
            tenant_b = session.run(
                """
                MATCH (n:Node {
                    qa_run_id: $run_id,
                    tenant_id: 'tenant-b',
                    source_uri: $source_uri
                })
                RETURN count(n) AS count, collect(n.l0_abstract) AS abstracts
                """,
                run_id=qa_run_id,
                source_uri=source_uri,
            ).single()

        assert both == 2
        assert tenant_a["count"] == 1
        assert tenant_a["abstracts"] == ["tenant a fact"]
        assert tenant_b["count"] == 1
        assert tenant_b["abstracts"] == ["tenant b fact"]

    def test_tenant_scoped_edges(self, neo4j_driver: Driver, qa_run_id: str):
        """Relationships created for one tenant remain tenant-labelled."""
        with neo4j_driver.session() as session:
            session.run(
                """
                CREATE (a:Node {
                    qa_run_id: $run_id,
                    tenant_id: 'tenant-a',
                    source_uri: 'mem://qa/a',
                    node_type: 'FACT',
                    status: 'ACTIVE'
                })
                CREATE (b:Node {
                    qa_run_id: $run_id,
                    tenant_id: 'tenant-a',
                    source_uri: 'mem://qa/b',
                    node_type: 'FACT',
                    status: 'ACTIVE'
                })
                CREATE (a)-[:RELATED_TO {
                    qa_run_id: $run_id,
                    tenant_id: 'tenant-a',
                    relation_label: 'related'
                }]->(b)
                """,
                run_id=qa_run_id,
            )
            tenant_a_edges = session.run(
                """
                MATCH (:Node {qa_run_id: $run_id, tenant_id: 'tenant-a'})
                    -[r {qa_run_id: $run_id, tenant_id: 'tenant-a'}]->
                    (:Node {qa_run_id: $run_id, tenant_id: 'tenant-a'})
                RETURN count(r) AS count
                """,
                run_id=qa_run_id,
            ).single()["count"]
            tenant_b_edges = session.run(
                """
                MATCH (:Node {qa_run_id: $run_id, tenant_id: 'tenant-b'})
                    -[r {qa_run_id: $run_id}]->
                    (:Node {qa_run_id: $run_id, tenant_id: 'tenant-b'})
                RETURN count(r) AS count
                """,
                run_id=qa_run_id,
            ).single()["count"]

        assert tenant_a_edges == 1
        assert tenant_b_edges == 0
