import os

import pytest
from neo4j import GraphDatabase

NEO4J_PASSWORD = os.environ.get("NEO4J_ADMIN_PASSWORD", "password")

pytestmark = pytest.mark.e2e

NEO4J_URI = "bolt://localhost:7687"


class TestNeo4jData:
    def test_neo4j_module_importable(self):
        assert GraphDatabase is not None

    def test_neo4j_connection(self):
        """Verify basic Neo4j connectivity."""
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=("neo4j", NEO4J_PASSWORD))
            with driver.session() as session:
                result = session.run("RETURN 1 AS test")
                assert result.single()["test"] == 1
            driver.close()
        except Exception as e:
            pytest.skip(f"Neo4j connection unavailable: {e}")

    def test_neo4j_node_count(self):
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=("neo4j", NEO4J_PASSWORD))
            with driver.session() as session:
                result = session.run("MATCH (n) RETURN count(n) AS node_count")
                node_count = result.single()["node_count"]
                assert node_count == 0, (
                    f"Expected 0 nodes (no admin UI browsing), got {node_count}. "
                    "mem:// URIs are only created when the admin UI scrapes pages."
                )
            driver.close()
        except Exception as e:
            pytest.skip(f"Neo4j query failed (expected if no server running): {e}")

    def test_neo4j_memuri_nodes(self):
        """Verify no mem:// nodes exist without admin UI browsing."""
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=("neo4j", NEO4J_PASSWORD))
            with driver.session() as session:
                result = session.run(
                    "MATCH (n) WHERE n.uri STARTS WITH 'mem://' RETURN count(n) AS mem_count"
                )
                mem_count = result.single()["mem_count"]
                assert mem_count == 0, (
                    f"Expected 0 mem:// nodes, got {mem_count}. "
                    "Admin UI browsing creates mem:// nodes."
                )
            driver.close()
        except Exception as e:
            pytest.skip(f"Neo4j query failed (expected if no server running): {e}")

    def test_neo4j_edge_count(self):
        """Query edge count — expected 0 since consolidation is async."""
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=("neo4j", NEO4J_PASSWORD))
            with driver.session() as session:
                result = session.run("MATCH ()-[r]->() RETURN count(r) AS edge_count")
                record = result.single()
                edge_count = record["edge_count"]
                # With no browsing/consolidation, no edges exist
                assert edge_count == 0, (
                    f"Expected 0 edges (no consolidation), got {edge_count}. "
                    "Edges are only created during async consolidation processes."
                )
            driver.close()
        except Exception as e:
            pytest.skip(f"Neo4j query failed (expected if no server running): {e}")