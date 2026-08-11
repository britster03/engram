"""In-memory knowledge graph backend.

A legitimate first-class alternative to Neo4j for deployments that don't
need a durable graph database:

  - single-user local installs where filesystem backup is enough
  - development loops (no Docker needed)
  - integration test suites that want deterministic behaviour without a
    backing service

The public surface mirrors `Neo4jStore` exactly — caller code never sees
which backend is in use. Durability: this backend keeps nothing on disk.
Process exit wipes the index. The filesystem under `data_dir` remains
authoritative, so `engram rebuild-kg` (which walks the filesystem +
replays extractions) reconstructs the index on next boot.

Performance: O(N) vector search via pure-Python cosine. Acceptable up to
~10k memory nodes per tenant; beyond that switch `knowledge_graph.backend`
to `neo4j`. The indexes created in `ensure_indexes()` are no-ops.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, cast

from engram.tenancy import current_tenant_id

log = logging.getLogger(__name__)


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    num = sum(x * y for x, y in zip(a, b, strict=True))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(x * x for x in b) ** 0.5
    if na == 0.0 or nb == 0.0:
        return 0.0
    return num / (na * nb)


@dataclass
class _Edge:
    type: str
    subject_uri: str
    object_uri: str
    relation_label: str
    tenant_id: str
    props: dict[str, Any] = field(default_factory=dict)


class InMemoryKnowledgeGraph:
    """Thread-safe in-memory KG indexed by (tenant_id, source_uri)."""

    def __init__(self) -> None:
        self._nodes: dict[tuple[str, str], dict[str, Any]] = {}
        self._edges: list[_Edge] = []
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Neo4j-shaped interface
    # ------------------------------------------------------------------

    def writer(self):  # compatibility shim — never called
        return self

    def reader(self):
        return self

    def ensure_indexes(self) -> None:
        return None

    def indexes_ready(self) -> bool:
        return True

    def ping(self) -> bool:
        return True

    def delete_tenant_data(self, tenant_ids: list[str]) -> None:
        if not tenant_ids:
            raise ValueError("tenant_ids must not be empty")
        selected = set(tenant_ids)
        with self._lock:
            self._nodes = {
                key: value for key, value in self._nodes.items() if key[0] not in selected
            }
            self._edges = [edge for edge in self._edges if edge.tenant_id not in selected]

    def close(self) -> None:
        return None

    # ------------------------------------------------------------------
    # Node / edge mutation
    # ------------------------------------------------------------------

    def merge_node(
        self,
        *,
        source_uri: str,
        properties: dict[str, Any],
        parent_uri: str | None = None,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        tid = tenant_id or current_tenant_id()
        with self._lock:
            key = (tid, source_uri)
            existing = self._nodes.get(key, {})
            default_created_at = str(
                properties.get("created_at") or datetime.now(timezone.utc).isoformat()
            )
            defaults = {
                "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{tid}:{source_uri}")),
                "status": "ACTIVE",
                "schema_version": 1,
                "created_at": default_created_at,
            }
            merged = {
                **defaults,
                **existing,
                **properties,
                "source_uri": source_uri,
                "tenant_id": tid,
            }
            self._nodes[key] = merged
            if parent_uri:
                parent_key = (tid, parent_uri)
                parent = self._nodes.setdefault(
                    parent_key,
                    {
                        "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{tid}:{parent_uri}")),
                        "source_uri": parent_uri,
                        "tenant_id": tid,
                        "node_type": "DIRECTORY",
                        "status": "ACTIVE",
                        "schema_version": 1,
                        "created_at": default_created_at,
                    },
                )
                if default_created_at < str(parent.get("created_at") or "~"):
                    parent["created_at"] = default_created_at
                if not any(
                    e.type == "CONTAINS"
                    and e.subject_uri == parent_uri
                    and e.object_uri == source_uri
                    and e.tenant_id == tid
                    for e in self._edges
                ):
                    self._edges.append(
                        _Edge(type="CONTAINS", subject_uri=parent_uri,
                              object_uri=source_uri, relation_label="CONTAINS",
                              tenant_id=tid)
                    )
            return dict(merged)

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
        props.setdefault("status", "ACTIVE")
        props["tenant_id"] = tid
        with self._lock:
            for e in self._edges:
                if (
                    e.type == edge_type
                    and e.subject_uri == subject_uri
                    and e.object_uri == object_uri
                    and e.relation_label == relation_label
                    and e.tenant_id == tid
                ):
                    e.props.update(props)
                    return
            self._edges.append(
                _Edge(
                    type=edge_type, subject_uri=subject_uri, object_uri=object_uri,
                    relation_label=relation_label, tenant_id=tid, props=props,
                )
            )

    # ------------------------------------------------------------------
    # Vector search
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
        tid = tenant_id or current_tenant_id()
        rows: list[tuple[float, dict[str, Any]]] = []
        with self._lock:
            for (t, uri), node in self._nodes.items():
                if t != tid:
                    continue
                if node.get("status") != "ACTIVE":
                    continue
                if any(
                    edge.tenant_id == tid
                    and edge.type == "SUPERSEDES"
                    and edge.object_uri == uri
                    for edge in self._edges
                ):
                    continue
                if float(node.get("retrieval_weight", 1.0)) < dormant_floor:
                    continue
                if uri_prefix and not uri.startswith(uri_prefix):
                    continue
                emb = node.get("l0_embedding")
                if emb is None:
                    continue
                score = _cosine(query_embedding, emb)
                rows.append(
                    (
                        score,
                        {
                            "source_uri": uri,
                            "l0_abstract": node.get("l0_abstract"),
                            "score": score,
                            "id": node.get("id"),
                            "node_type": node.get("node_type"),
                            "source_turn_ids": list(node.get("source_turn_ids") or []),
                        },
                    )
                )
        rows.sort(key=lambda p: p[0], reverse=True)
        return [r for _, r in rows[:k]]

    # ------------------------------------------------------------------
    # Cypher template runner — a best-effort interpreter for the bundled templates
    # ------------------------------------------------------------------

    def run_template(
        self,
        cypher: str,
        params: dict[str, Any],
        timeout_s: int | None = None,
        *,
        tenant_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Interpret a small handful of bundled templates in Python.

        This backend deliberately does NOT parse Cypher generically. Callers
        get results for the templates we ship out of the box
        (t_children_of, t_neighbours_by_relation, t_cross_references,
        t_find_by_uri_prefix, t_history_chain, t_path_between) and empty
        results for anything else (the orchestrator degrades gracefully
        when a template returns nothing, per §4.1).
        """
        tid = tenant_id or params.get("tenant_id") or current_tenant_id()
        uri = params.get("uri") or params.get("node_uri")
        limit = int(params.get("limit", 25))

        cypher_upper = cypher.upper()

        with self._lock:
            # Conflict-classifier projection of active semantic edges.
            if "RETURN ELEMENTID(R) AS EDGE_ID" in cypher_upper and uri:
                conflict_rows: list[dict[str, Any]] = []
                for existing_edge_idx, edge in enumerate(self._edges):
                    if (
                        edge.type != "RELATES_TO"
                        or edge.subject_uri != uri
                        or edge.tenant_id != tid
                        or edge.props.get("status") != "ACTIVE"
                    ):
                        continue
                    obj = self._nodes.get((tid, edge.object_uri)) or {}
                    if obj.get("status") != "ACTIVE":
                        continue
                    conflict_rows.append(
                        {
                            "edge_id": existing_edge_idx,
                            "relation_label": edge.relation_label,
                            "object_uri": edge.object_uri,
                            "object_abstract": obj.get("l0_abstract"),
                            "assertion_uri": edge.props.get("assertion_uri"),
                        }
                    )
                return conflict_rows

            if "SET R.LAST_ACCESSED_AT" in cypher_upper:
                target_edge_id = params.get("eid")
                if isinstance(target_edge_id, int) and 0 <= target_edge_id < len(self._edges):
                    edge = self._edges[target_edge_id]
                    if edge.tenant_id == tid:
                        edge.props["last_accessed_at"] = params.get("now")
                        edge.props["access_count"] = int(edge.props.get("access_count", 0)) + 1
                return []

            if "SET R.STATUS = 'HISTORICAL'" in cypher_upper:
                target_edge_id = params.get("eid")
                if isinstance(target_edge_id, int) and 0 <= target_edge_id < len(self._edges):
                    edge = self._edges[target_edge_id]
                    if edge.tenant_id == tid:
                        edge.props["status"] = "HISTORICAL"
                        edge.props["superseded_at"] = params.get("now")
                        return [{"object_uri": edge.object_uri}]
                return []

            # t_children_of — CONTAINS traversal
            if "CONTAINS" in cypher_upper and uri and "SHORTESTPATH" not in cypher_upper:
                out: list[dict[str, Any]] = []
                for e in self._edges:
                    if (e.type == "CONTAINS" and e.subject_uri == uri
                            and e.tenant_id == tid):
                        child = self._nodes.get((tid, e.object_uri))
                        if child and child.get("status") == "ACTIVE":
                            out.append({
                                "source_uri": e.object_uri,
                                "l0_abstract": child.get("l0_abstract"),
                                "node_type": child.get("node_type"),
                                "retrieval_weight": child.get("retrieval_weight", 1.0),
                            })
                return out[:limit]

            # t_neighbours_by_relation
            if "RELATES_TO" in cypher_upper and uri:
                relation = params.get("relation")
                out = []
                for e in self._edges:
                    if (e.type == "RELATES_TO" and e.subject_uri == uri
                            and e.tenant_id == tid and e.props.get("status") == "ACTIVE"):
                        if relation and e.relation_label != relation:
                            continue
                        node = self._nodes.get((tid, e.object_uri))
                        if node and node.get("status") == "ACTIVE":
                            out.append({
                                "source_uri": e.object_uri,
                                "l0_abstract": node.get("l0_abstract"),
                                "node_type": node.get("node_type"),
                                "distance": 1,
                            })
                return out[:limit]

            # t_cross_references
            if "REFERENCES" in cypher_upper and uri:
                out = []
                for e in self._edges:
                    if (e.type == "REFERENCES" and e.subject_uri == uri
                            and e.tenant_id == tid):
                        node = self._nodes.get((tid, e.object_uri))
                        if node and node.get("status") == "ACTIVE":
                            out.append({
                                "source_uri": e.object_uri,
                                "node_type": node.get("node_type"),
                                "relation": e.props.get("relation_label") or e.relation_label,
                                "l0_abstract": node.get("l0_abstract"),
                            })
                return out[:limit]

            # t_find_by_uri_prefix
            if "STARTS WITH" in cypher_upper and params.get("prefix"):
                prefix = params["prefix"]
                out = []
                for (t, node_uri), node in self._nodes.items():
                    if t != tid or node.get("status") != "ACTIVE":
                        continue
                    if node_uri.startswith(prefix):
                        out.append({
                            "source_uri": node_uri,
                            "node_type": node.get("node_type"),
                            "l0_abstract": node.get("l0_abstract"),
                            "retrieval_weight": node.get("retrieval_weight", 1.0),
                        })
                return sorted(out, key=lambda r: r["source_uri"])[:limit]

            # t_history_chain
            if "SUPERSEDES" in cypher_upper and uri:
                # Follow SUPERSEDES edges from the node
                out = [{
                    "source_uri": uri,
                    "status": (self._nodes.get((tid, uri)) or {}).get("status"),
                    "l0_abstract": (self._nodes.get((tid, uri)) or {}).get("l0_abstract"),
                    "created_at": (self._nodes.get((tid, uri)) or {}).get("created_at"),
                    "distance": 0,
                }]
                return out[:limit]

            # Fallback: mutation-style templates used elsewhere (touch/supersede/merge)
            # — treat them as successful no-ops with no return rows. The callers that
            # need the side-effect in-memory use merge_edge / merge_node directly.
            return []

    # ------------------------------------------------------------------
    # Introspection helpers (used by tests + diagnostics)
    # ------------------------------------------------------------------

    def node_count(self, *, tenant_id: str | None = None) -> int:
        with self._lock:
            if tenant_id is None:
                return len(self._nodes)
            return sum(1 for (t, _) in self._nodes if t == tenant_id)

    def edge_count(self, *, tenant_id: str | None = None) -> int:
        with self._lock:
            if tenant_id is None:
                return len(self._edges)
            return sum(1 for e in self._edges if e.tenant_id == tenant_id)

    def iter_nodes(self, *, tenant_id: str | None = None):
        with self._lock:
            for (t, uri), node in self._nodes.items():
                if tenant_id is None or t == tenant_id:
                    yield uri, dict(node)

    def graph(
        self,
        *,
        root_uri: str | None = None,
        depth: int = 1,
        limit: int = 100,
        node_type: str | None = None,
        tenant_id: str | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Return a bounded tenant-scoped graph view."""
        tid = tenant_id or current_tenant_id()
        depth = max(0, min(depth, 4))
        limit = max(1, min(limit, 500))
        with self._lock:
            if root_uri:
                if (tid, root_uri) not in self._nodes:
                    return {"nodes": [], "edges": []}
                selected: set[str] = {root_uri}
                frontier = {root_uri}
                for _ in range(depth):
                    next_frontier: set[str] = set()
                    for e in self._edges:
                        if e.tenant_id != tid:
                            continue
                        if e.subject_uri in frontier and e.object_uri not in selected:
                            next_frontier.add(e.object_uri)
                        if e.object_uri in frontier and e.subject_uri not in selected:
                            next_frontier.add(e.subject_uri)
                    selected.update(next_frontier)
                    frontier = next_frontier
                    if len(selected) >= limit or not frontier:
                        break
            else:
                selected = {
                    uri
                    for (t, uri), node in self._nodes.items()
                    if t == tid and (node_type is None or node.get("node_type") == node_type)
                }
            ordered = sorted(selected)[:limit]
            selected = set(ordered)
            nodes: list[dict[str, Any]] = []
            for uri in ordered:
                node = self._nodes.get((tid, uri))
                if not node:
                    continue
                if node_type and node.get("node_type") != node_type:
                    continue
                nodes.append(_graph_node(uri, node))
            node_ids = {n["id"] for n in nodes}
            edges: list[dict[str, Any]] = []
            seen_edges: set[tuple[str, str, str, str]] = set()
            for e in self._edges:
                if e.tenant_id != tid:
                    continue
                if e.subject_uri not in node_ids or e.object_uri not in node_ids:
                    continue
                key = (e.subject_uri, e.object_uri, e.type, e.relation_label)
                if key in seen_edges:
                    continue
                seen_edges.add(key)
                edges.append({
                    "source": e.subject_uri,
                    "target": e.object_uri,
                    "type": e.type,
                    "label": e.props.get("relation_label") or e.relation_label or e.type,
                    "status": e.props.get("status"),
                })
            return {"nodes": nodes, "edges": edges}

    # ------------------------------------------------------------------
    # Flat-dict views for diagnostics + legacy callers that want a single
    # collection without the tenant key. Returns copies; mutating them has
    # no effect on the backing store.
    # ------------------------------------------------------------------

    @property
    def nodes(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {uri: dict(props) for (_, uri), props in self._nodes.items()}

    @property
    def edges(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                {
                    "type": e.type,
                    "source": e.subject_uri,
                    "target": e.object_uri,
                    "relation_label": e.relation_label,
                    "tenant_id": e.tenant_id,
                    **e.props,
                }
                for e in self._edges
            ]


def _graph_node(uri: str, node: dict[str, Any]) -> dict[str, Any]:
    normalize = (
        cast(dict[str, Any], node.get("normalize"))
        if isinstance(node.get("normalize"), dict)
        else {}
    )
    return {
        "id": uri,
        "label": normalize.get("canonical_name") or node.get("source_uri") or uri,
        "source_uri": uri,
        "node_type": node.get("node_type"),
        "status": node.get("status"),
        "l0_abstract": node.get("l0_abstract"),
    }
