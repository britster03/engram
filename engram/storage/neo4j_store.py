"""Neo4j knowledge graph store (§6).

Separate writer and reader drivers per §12.3 (on Enterprise; Community
collapses to one admin user). Every node carries a `tenant_id` property
and every read path filters by the ambient tenant context — tenants
cannot accidentally observe each other's data.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from neo4j import Driver, GraphDatabase

from engram.config import KnowledgeGraphConfig
from engram.tenancy import current_tenant_id

log = logging.getLogger(__name__)


INDEX_STATEMENTS = [
    # L0 embedding vector index (§6.6). 384 dims for BGE-Small.
    """
    CREATE VECTOR INDEX l0_idx IF NOT EXISTS
    FOR (n:Node) ON (n.l0_embedding)
    OPTIONS {
      indexConfig: {
        `vector.dimensions`: 384,
        `vector.similarity_function`: 'cosine'
      }
    }
    """,
    # BM25 fulltext index over l0_abstract (§6.6)
    """
    CREATE FULLTEXT INDEX l0_text_idx IF NOT EXISTS
    FOR (n:Node) ON EACH [n.l0_abstract]
    """,
    # URI prefix index for filesystem-style navigation
    """
    CREATE INDEX uri_prefix_idx IF NOT EXISTS
    FOR (n:Node) ON (n.source_uri)
    """,
    # Status index for ACTIVE-only default filters
    """
    CREATE INDEX status_idx IF NOT EXISTS
    FOR (n:Node) ON (n.status)
    """,
    # Retrieval-weight index for decay-filtered queries
    """
    CREATE INDEX weight_idx IF NOT EXISTS
    FOR (n:Node) ON (n.retrieval_weight)
    """,
    # Tenant index — most read-path queries filter by tenant_id
    """
    CREATE INDEX tenant_idx IF NOT EXISTS
    FOR (n:Node) ON (n.tenant_id)
    """,
    # Composite: tenant + status (fast ACTIVE-within-tenant)
    """
    CREATE INDEX tenant_status_idx IF NOT EXISTS
    FOR (n:Node) ON (n.tenant_id, n.status)
    """,
    # Uniqueness: (tenant_id, source_uri) — same URI can reappear across tenants
    """
    CREATE CONSTRAINT node_tenant_source_uri_unique IF NOT EXISTS
    FOR (n:Node) REQUIRE (n.tenant_id, n.source_uri) IS UNIQUE
    """,
]


class Neo4jStore:
    def __init__(self, cfg: KnowledgeGraphConfig) -> None:
        self.cfg = cfg
        self._writer: Driver | None = None
        self._reader: Driver | None = None

    # ------------------------------------------------------------------
    # Drivers
    # ------------------------------------------------------------------

    def writer(self) -> Driver:
        if self._writer is None:
            self._writer = GraphDatabase.driver(
                self.cfg.uri,
                auth=(self.cfg.writer_username, self.cfg.writer_password or ""),
            )
        return self._writer

    def reader(self) -> Driver:
        if self._reader is None:
            self._reader = GraphDatabase.driver(
                self.cfg.uri,
                auth=(self.cfg.reader_username, self.cfg.reader_password or ""),
            )
        return self._reader

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
        if self._reader is not None:
            self._reader.close()

    # ------------------------------------------------------------------
    # Index / schema bootstrap
    # ------------------------------------------------------------------

    def ensure_indexes(self) -> None:
        with self.writer().session() as session:
            for stmt in INDEX_STATEMENTS:
                try:
                    session.run(stmt)
                except Exception as err:
                    # Old Neo4j versions may not support particular index syntaxes.
                    # We surface the error as a warning; `engram init` reports it.
                    log.warning("index statement failed: %s", err)

    def ping(self) -> bool:
        try:
            with self.reader().session() as session:
                session.run("RETURN 1").consume()
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Node upsert — writes after filesystem commit (§5.4.6)
    # ------------------------------------------------------------------

    def merge_node(
        self,
        *,
        source_uri: str,
        properties: dict[str, Any],
        parent_uri: str | None = None,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        """MERGE on (tenant_id, source_uri); SET all properties; create CONTAINS edge from parent."""
        tid = tenant_id or current_tenant_id()
        props = {**properties, "tenant_id": tid}
        query = (
            "MERGE (n:Node {tenant_id: $tenant_id, source_uri: $source_uri}) "
            "SET n += $props "
            "RETURN n"
        )
        with self.writer().session() as session:
            result = session.run(
                query, tenant_id=tid, source_uri=source_uri, props=props,
            ).single()
            node = dict(result["n"]) if result else {}
            if parent_uri:
                session.run(
                    "MERGE (p:Node {tenant_id: $tenant_id, source_uri: $parent_uri}) "
                    "ON CREATE SET p.tenant_id = $tenant_id, p.status = 'ACTIVE' "
                    "MERGE (c:Node {tenant_id: $tenant_id, source_uri: $child_uri}) "
                    "MERGE (p)-[:CONTAINS]->(c)",
                    tenant_id=tid,
                    parent_uri=parent_uri,
                    child_uri=source_uri,
                )
        return node

    def merge_edge(
        self,
        *,
        subject_uri: str,
        object_uri: str,
        relation_label: str,
        edge_type: str = "RELATES_TO",
        properties: dict[str, Any] | None = None,
        tenant_id: str | None = None,
    ) -> None:
        tid = tenant_id or current_tenant_id()
        props = dict(properties or {})
        props.setdefault("relation_label", relation_label)
        props.setdefault("status", "ACTIVE")
        props["tenant_id"] = tid
        query = (
            "MATCH (s:Node {tenant_id: $tenant_id, source_uri: $s_uri}) "
            "MATCH (o:Node {tenant_id: $tenant_id, source_uri: $o_uri}) "
            f"MERGE (s)-[r:{edge_type} {{relation_label: $relation_label}}]->(o) "
            "SET r += $props "
        )
        with self.writer().session() as session:
            session.run(
                query,
                tenant_id=tid,
                s_uri=subject_uri,
                o_uri=object_uri,
                relation_label=relation_label,
                props=props,
            )

    # ------------------------------------------------------------------
    # Vector search (§3.2 L1) — tenant-scoped
    # ------------------------------------------------------------------

    def vector_search(
        self,
        query_embedding: list[float],
        k: int = 10,
        *,
        uri_prefix: str | None = None,
        dormant_floor: float = 0.05,
        tenant_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Top-K tenant-scoped nodes by cosine similarity on l0_embedding.

        Per §10.4, over-fetch by 1.5x then filter by retrieval_weight.
        """
        tid = tenant_id or current_tenant_id()
        over_k = max(k + 5, int(k * 1.5))
        cypher = (
            "CALL db.index.vector.queryNodes('l0_idx', $k, $vec) YIELD node, score "
            "WHERE node.tenant_id = $tenant_id "
            "AND node.status = 'ACTIVE' "
            "AND coalesce(node.retrieval_weight, 1.0) >= $floor "
        )
        if uri_prefix:
            cypher += "AND node.source_uri STARTS WITH $prefix "
        cypher += (
            "RETURN node.source_uri AS source_uri, node.l0_abstract AS l0_abstract, "
            "score, node.id AS id, node.node_type AS node_type, "
            "coalesce(node.source_turn_ids, []) AS source_turn_ids ORDER BY score DESC"
        )
        params: dict[str, Any] = {
            "k": over_k, "vec": query_embedding, "floor": dormant_floor,
            "tenant_id": tid,
        }
        if uri_prefix:
            params["prefix"] = uri_prefix
        with self.reader().session() as session:
            result = session.run(cypher, **params)
            rows = [dict(r) for r in result]
        return rows[:k]

    # ------------------------------------------------------------------
    # Read-only template runner (§8.5)
    # ------------------------------------------------------------------

    @contextmanager
    def read_session(self) -> Iterator[Any]:
        with self.reader().session() as session:
            yield session

    def run_template(
        self,
        cypher: str,
        params: dict[str, Any],
        timeout_s: int | None = None,
        *,
        tenant_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a parameterised Cypher template under the tenant's scope.

        Template authors reference `$tenant_id` in the WHERE clauses of node
        MATCH patterns. The runner injects the ambient tenant automatically
        so templates stay tenant-aware without per-callsite plumbing.
        """
        tid = tenant_id or current_tenant_id()
        params = {**params, "tenant_id": tid}
        with self.reader().session() as session:
            tx = session.begin_transaction(timeout=timeout_s)
            try:
                result = tx.run(cypher, **params)
                rows = [dict(r) for r in result]
                tx.commit()
                return rows
            except Exception:
                tx.rollback()
                raise

    def graph(
        self,
        *,
        root_uri: str | None = None,
        depth: int = 1,
        limit: int = 100,
        node_type: str | None = None,
        tenant_id: str | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Return a bounded tenant-scoped graph view for admin visualization."""
        tid = tenant_id or current_tenant_id()
        depth = max(0, min(int(depth), 4))
        limit = max(1, min(int(limit), 500))
        params: dict[str, Any]
        if root_uri:
            cypher = (
                f"MATCH p=(root:Node {{tenant_id: $tenant_id, source_uri: $root_uri}})"
                f"-[*0..{depth}]-(n:Node) "
                "WHERE n.tenant_id = $tenant_id "
                "AND ($node_type IS NULL OR n.node_type = $node_type) "
                "AND all(rel IN relationships(p) WHERE coalesce(rel.tenant_id, $tenant_id) = $tenant_id) "
                "WITH collect(DISTINCT n)[0..$limit] AS nodes "
                "UNWIND nodes AS n WITH collect(DISTINCT n) AS nodes "
                "OPTIONAL MATCH (a)-[r]-(b) "
                "WHERE a IN nodes AND b IN nodes "
                "AND coalesce(r.tenant_id, $tenant_id) = $tenant_id "
                "RETURN "
                "[node IN nodes | {id: node.source_uri, label: node.source_uri, "
                "source_uri: node.source_uri, node_type: node.node_type, "
                "status: node.status, l0_abstract: node.l0_abstract}] AS nodes, "
                "[rel IN collect(DISTINCT r) WHERE rel IS NOT NULL | "
                "{source: startNode(rel).source_uri, target: endNode(rel).source_uri, "
                "type: type(rel), label: coalesce(rel.relation_label, type(rel)), "
                "status: rel.status}] AS edges"
            )
            params = {
                "tenant_id": tid,
                "root_uri": root_uri,
                "node_type": node_type,
                "limit": limit,
            }
        else:
            cypher = (
                "MATCH (n:Node) WHERE n.tenant_id = $tenant_id "
                "AND ($node_type IS NULL OR n.node_type = $node_type) "
                "WITH n ORDER BY coalesce(n.created_at, '') DESC LIMIT $limit "
                "WITH collect(n) AS nodes "
                "UNWIND nodes AS n WITH collect(DISTINCT n) AS nodes "
                "OPTIONAL MATCH (a)-[r]-(b) "
                "WHERE a IN nodes AND b IN nodes "
                "AND coalesce(r.tenant_id, $tenant_id) = $tenant_id "
                "RETURN "
                "[node IN nodes | {id: node.source_uri, label: node.source_uri, "
                "source_uri: node.source_uri, node_type: node.node_type, "
                "status: node.status, l0_abstract: node.l0_abstract}] AS nodes, "
                "[rel IN collect(DISTINCT r) WHERE rel IS NOT NULL | "
                "{source: startNode(rel).source_uri, target: endNode(rel).source_uri, "
                "type: type(rel), label: coalesce(rel.relation_label, type(rel)), "
                "status: rel.status}] AS edges"
            )
            params = {"tenant_id": tid, "node_type": node_type, "limit": limit}
        with self.reader().session() as session:
            row = session.run(cypher, **params).single()
        if not row:
            return {"nodes": [], "edges": []}
        return {
            "nodes": [dict(n) for n in (row.get("nodes") or [])],
            "edges": [dict(e) for e in (row.get("edges") or [])],
        }
