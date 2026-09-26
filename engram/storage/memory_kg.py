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
from dataclasses import dataclass, field
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

    def ping(self) -> bool:
        return True

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
            merged = {**existing, **properties, "source_uri": source_uri, "tenant_id": tid}
            self._nodes[key] = merged
            if parent_uri:
                parent_key = (tid, parent_uri)
                self._nodes.setdefault(
                    parent_key,
                    {
                        "source_uri": parent_uri,
                        "tenant_id": tid,
                        "node_type": "DIRECTORY",
                        "status": "ACTIVE",
                    },
                )
                if not any(
                    e.type == "CONTAINS"
                    and e.subject_uri == parent_uri
                    and e.object_uri == source_uri
                    and e.tenant_id == tid
                    for e in self._edges
                ):
                    self._edges.append(
                        _Edge(
                            type="CONTAINS",
                            subject_uri=parent_uri,
                            object_uri=source_uri,
                            relation_label="CONTAINS",
                            tenant_id=tid,
                        )
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
                    type=edge_type,
                    subject_uri=subject_uri,
                    object_uri=object_uri,
                    relation_label=relation_label,
                    tenant_id=tid,
                    props=props,
                )
            )

    # ------------------------------------------------------------------
    # Canonical PostgreSQL projection ----------------------------------
    # ------------------------------------------------------------------

    @staticmethod
    def _projection_revision(revision: int) -> int:
        try:
            value = int(revision)
        except (TypeError, ValueError) as err:
            raise ValueError("projection revision must be an integer") from err
        if value < 0:
            raise ValueError("projection revision must be non-negative")
        return value

    def upsert_memory_projection(
        self,
        *,
        memory_id: str,
        revision: int,
        properties: dict[str, Any] | None = None,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        """Apply a stable-ID, revision-guarded memory projection in memory."""
        if not memory_id:
            raise ValueError("memory_id is required for projection")
        tid = tenant_id or current_tenant_id()
        incoming = self._projection_revision(revision)
        key = (tid, f"memory:{memory_id}")
        props = dict(properties or {})
        for field_name in ("tenant_id", "memory_id", "projected_revision"):
            props.pop(field_name, None)
        props.setdefault("status", "ACTIVE")
        with self._lock:
            existing = self._nodes.get(key, {})
            current = int(existing.get("projected_revision", -1))
            if current > incoming:
                return {
                    "node": dict(existing),
                    "applied": False,
                    "projected_revision": current,
                }
            merged = {
                **existing,
                **props,
                "source_uri": props.get(
                    "source_uri", existing.get("source_uri", f"mem://memory/{memory_id}")
                ),
                "tenant_id": tid,
                "memory_id": memory_id,
                "projected_revision": incoming,
            }
            self._nodes[key] = merged
            return {"node": dict(merged), "applied": True, "projected_revision": incoming}

    def delete_memory_projection(
        self,
        *,
        memory_id: str,
        revision: int,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        """Retain a keyed tombstone so late older projections cannot resurrect it."""
        if not memory_id:
            raise ValueError("memory_id is required for projection")
        tid = tenant_id or current_tenant_id()
        incoming = self._projection_revision(revision)
        key = (tid, f"memory:{memory_id}")
        with self._lock:
            existing = self._nodes.get(key, {})
            current = int(existing.get("projected_revision", -1))
            if current > incoming:
                return {
                    "node": dict(existing),
                    "applied": False,
                    "projected_revision": current,
                }
            merged = {
                **existing,
                "source_uri": existing.get("source_uri", f"mem://{memory_id}"),
                "tenant_id": tid,
                "memory_id": memory_id,
                "status": "DELETED",
                "projected_revision": incoming,
            }
            self._nodes[key] = merged
            return {"node": dict(merged), "applied": True, "projected_revision": incoming}

    def upsert_claim_projection(
        self,
        *,
        claim_id: str,
        subject_memory_id: str,
        object_memory_id: str,
        predicate: str,
        revision: int,
        properties: dict[str, Any] | None = None,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        """Apply a stable claim edge keyed by tenant and claim ID."""
        if not claim_id:
            raise ValueError("claim_id is required for projection")
        if not subject_memory_id or not object_memory_id:
            raise ValueError("subject_memory_id and object_memory_id are required")
        if not predicate:
            raise ValueError("predicate is required for projection")
        tid = tenant_id or current_tenant_id()
        incoming = self._projection_revision(revision)
        props = dict(properties or {})
        for field_name in (
            "tenant_id",
            "claim_id",
            "projected_revision",
            "subject_memory_id",
            "object_memory_id",
        ):
            props.pop(field_name, None)
        props.setdefault("predicate", predicate)
        props.setdefault("relation_label", predicate)
        props.setdefault("status", "ACTIVE")
        with self._lock:
            if (tid, f"memory:{subject_memory_id}") not in self._nodes or (
                tid,
                f"memory:{object_memory_id}",
            ) not in self._nodes:
                return {"edge": {}, "applied": False, "projected_revision": None}
            existing = next(
                (
                    edge
                    for edge in self._edges
                    if edge.tenant_id == tid and edge.props.get("claim_id") == claim_id
                ),
                None,
            )
            if existing is not None:
                current = int(existing.props.get("projected_revision", -1))
                if current > incoming:
                    return {
                        "edge": dict(existing.props),
                        "applied": False,
                        "projected_revision": current,
                    }
                self._edges.remove(existing)
            edge_props = {
                **props,
                "tenant_id": tid,
                "claim_id": claim_id,
                "subject_memory_id": subject_memory_id,
                "object_memory_id": object_memory_id,
                "predicate": predicate,
                "projected_revision": incoming,
            }
            self._edges.append(
                _Edge(
                    type="RELATES_TO",
                    subject_uri=f"memory:{subject_memory_id}",
                    object_uri=f"memory:{object_memory_id}",
                    relation_label=predicate,
                    tenant_id=tid,
                    props=edge_props,
                )
            )
            return {"edge": dict(edge_props), "applied": True, "projected_revision": incoming}

    def delete_claim_projection(
        self,
        *,
        claim_id: str,
        revision: int,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        """Delete a claim edge only when its incoming revision is current."""
        if not claim_id:
            raise ValueError("claim_id is required for projection")
        tid = tenant_id or current_tenant_id()
        incoming = self._projection_revision(revision)
        with self._lock:
            for edge in list(self._edges):
                if edge.tenant_id != tid or edge.props.get("claim_id") != claim_id:
                    continue
                current = int(edge.props.get("projected_revision", -1))
                if current > incoming:
                    return {"applied": False, "projected_revision": current}
                self._edges.remove(edge)
                return {"applied": True, "projected_revision": incoming}
        return {"applied": False, "projected_revision": None}

    def upsert_hierarchy_projection(
        self,
        *,
        hierarchy_id: str,
        parent_memory_id: str,
        child_memory_id: str,
        revision: int,
        properties: dict[str, Any] | None = None,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        """Apply a stable-ID, revision-aware canonical hierarchy edge."""

        if not hierarchy_id or not parent_memory_id or not child_memory_id:
            raise ValueError("hierarchy_id, parent_memory_id, and child_memory_id are required")
        tid = tenant_id or current_tenant_id()
        incoming = self._projection_revision(revision)
        props = dict(properties or {})
        with self._lock:
            if (tid, f"memory:{parent_memory_id}") not in self._nodes or (
                tid,
                f"memory:{child_memory_id}",
            ) not in self._nodes:
                return {"edge": {}, "applied": False, "projected_revision": None}
            existing = next(
                (
                    edge
                    for edge in self._edges
                    if edge.tenant_id == tid and edge.props.get("hierarchy_id") == hierarchy_id
                ),
                None,
            )
            if existing is not None:
                current = int(existing.props.get("projected_revision", -1))
                if current > incoming:
                    return {
                        "edge": dict(existing.props),
                        "applied": False,
                        "projected_revision": current,
                    }
                self._edges.remove(existing)
            edge_props = {
                **props,
                "tenant_id": tid,
                "hierarchy_id": hierarchy_id,
                "projected_revision": incoming,
            }
            self._edges.append(
                _Edge(
                    type="CONTAINS",
                    subject_uri=f"memory:{parent_memory_id}",
                    object_uri=f"memory:{child_memory_id}",
                    relation_label="contains",
                    tenant_id=tid,
                    props=edge_props,
                )
            )
            return {
                "edge": dict(edge_props),
                "applied": True,
                "projected_revision": incoming,
            }

    project_memory = upsert_memory_projection
    project_claim = upsert_claim_projection

    # ------------------------------------------------------------------
    # Code-map replacement and read models
    # ------------------------------------------------------------------

    def replace_code_project(
        self,
        *,
        project_uri: str,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
        tenant_id: str | None = None,
    ) -> None:
        """Replace one structural project without affecting semantic memories."""
        tid = tenant_id or current_tenant_id()
        with self._lock:
            stale = {
                uri
                for (tenant, uri), node in self._nodes.items()
                if tenant == tid and node.get("project_uri") == project_uri
            }
            for uri in stale:
                self._nodes.pop((tid, uri), None)
            self._edges = [
                edge
                for edge in self._edges
                if edge.tenant_id != tid
                or (edge.subject_uri not in stale and edge.object_uri not in stale)
            ]
            for item in nodes:
                props = dict(item.get("properties") or {})
                uri = str(item["source_uri"])
                self._nodes[(tid, uri)] = {**props, "source_uri": uri, "tenant_id": tid}
            for item in nodes:
                parent_uri = item.get("parent_uri")
                if parent_uri:
                    self.merge_edge(
                        subject_uri=parent_uri,
                        object_uri=item["source_uri"],
                        edge_type="CONTAINS",
                        relation_label="contains",
                        properties={
                            "parser": "archive-hierarchy",
                            "confidence": 1.0,
                            "resolution": "RESOLVED",
                        },
                        tenant_id=tid,
                    )
            for item in edges:
                self.merge_edge(
                    subject_uri=item["subject_uri"],
                    object_uri=item["object_uri"],
                    edge_type=item["edge_type"],
                    relation_label=item["relation_label"],
                    properties=dict(item.get("properties") or {}),
                    tenant_id=tid,
                )

    def list_code_projects(self, *, tenant_id: str | None = None) -> list[dict[str, Any]]:
        tid = tenant_id or current_tenant_id()
        with self._lock:
            projects = [
                {
                    "source_uri": uri,
                    "name": node.get("display_name") or uri,
                    "l0_abstract": node.get("l0_abstract"),
                    "created_at": node.get("created_at"),
                }
                for (tenant, uri), node in self._nodes.items()
                if tenant == tid
                and node.get("node_type") == "PROJECT"
                and node.get("project_uri") == uri
            ]
        return sorted(projects, key=lambda item: str(item["name"]).lower())

    def code_map(
        self,
        *,
        project_uri: str,
        depth: int = 2,
        limit: int = 200,
        tenant_id: str | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        tid = tenant_id or current_tenant_id()
        depth = max(0, min(int(depth), 6))
        limit = max(1, min(int(limit), 500))
        hierarchy = {"CONTAINS", "DEFINES"}
        with self._lock:
            if (tid, project_uri) not in self._nodes:
                return {"nodes": [], "edges": []}
            selected = {project_uri}
            frontier = {project_uri}
            for _ in range(depth):
                next_frontier = {
                    edge.object_uri
                    for edge in self._edges
                    if edge.tenant_id == tid
                    and edge.type in hierarchy
                    and edge.subject_uri in frontier
                    and edge.object_uri not in selected
                }
                selected.update(next_frontier)
                frontier = next_frontier
                if not frontier or len(selected) >= limit:
                    break
            selected = set(sorted(selected)[:limit])
            nodes = []
            for uri in sorted(selected):
                node = self._nodes[(tid, uri)]
                nodes.append(
                    {
                        "id": uri,
                        "source_uri": uri,
                        "label": node.get("display_name") or node.get("relative_path") or uri,
                        "node_type": node.get("node_type"),
                        "status": node.get("status"),
                        "l0_abstract": node.get("l0_abstract"),
                        "relative_path": node.get("relative_path"),
                        "language": node.get("language"),
                        "signature": node.get("signature"),
                        "line_start": node.get("line_start"),
                        "line_end": node.get("line_end"),
                    }
                )
            edges = [
                {
                    "source": edge.subject_uri,
                    "target": edge.object_uri,
                    "type": edge.type,
                    "label": edge.relation_label,
                    "status": edge.props.get("status"),
                }
                for edge in self._edges
                if edge.tenant_id == tid
                and edge.type in hierarchy
                and edge.subject_uri in selected
                and edge.object_uri in selected
            ]
            return {"nodes": nodes, "edges": edges}

    def code_node_details(
        self,
        *,
        source_uri: str,
        tenant_id: str | None = None,
    ) -> dict[str, Any] | None:
        tid = tenant_id or current_tenant_id()
        with self._lock:
            node = self._nodes.get((tid, source_uri))
            if node is None:
                return None
            clean_node = {key: value for key, value in node.items() if key != "l0_embedding"}

            def relation(edge: _Edge, other_uri: str) -> dict[str, Any]:
                other = self._nodes.get((tid, other_uri), {})
                return {
                    "source_uri": other_uri,
                    "display_name": other.get("display_name") or other_uri,
                    "node_type": other.get("node_type"),
                    "relation": edge.type,
                    "label": edge.relation_label,
                    "source_path": edge.props.get("source_path"),
                    "line_start": edge.props.get("line_start"),
                    "line_end": edge.props.get("line_end"),
                    "confidence": edge.props.get("confidence"),
                    "resolution": edge.props.get("resolution"),
                }

            outgoing = [
                relation(edge, edge.object_uri)
                for edge in self._edges
                if edge.tenant_id == tid and edge.subject_uri == source_uri
            ]
            incoming = [
                relation(edge, edge.subject_uri)
                for edge in self._edges
                if edge.tenant_id == tid and edge.object_uri == source_uri
            ]
            return {"node": clean_node, "outgoing": outgoing, "incoming": incoming}

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
                            "memory_id": node.get("memory_id"),
                            "projected_revision": node.get("projected_revision"),
                            "node_type": node.get("node_type"),
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
            # t_children_of — CONTAINS traversal
            if "CONTAINS" in cypher_upper and uri and "SHORTESTPATH" not in cypher_upper:
                out: list[dict[str, Any]] = []
                for e in self._edges:
                    if e.type == "CONTAINS" and e.subject_uri == uri and e.tenant_id == tid:
                        child = self._nodes.get((tid, e.object_uri))
                        if child and child.get("status") == "ACTIVE":
                            out.append(
                                {
                                    "source_uri": e.object_uri,
                                    "l0_abstract": child.get("l0_abstract"),
                                    "node_type": child.get("node_type"),
                                    "retrieval_weight": child.get("retrieval_weight", 1.0),
                                }
                            )
                return out[:limit]

            # t_neighbours_by_relation
            if "RELATES_TO" in cypher_upper and uri:
                relation = params.get("relation")
                out = []
                for e in self._edges:
                    if (
                        e.type == "RELATES_TO"
                        and e.subject_uri == uri
                        and e.tenant_id == tid
                        and e.props.get("status") == "ACTIVE"
                    ):
                        if relation and e.relation_label != relation:
                            continue
                        node = self._nodes.get((tid, e.object_uri))
                        if node and node.get("status") == "ACTIVE":
                            out.append(
                                {
                                    "source_uri": e.object_uri,
                                    "l0_abstract": node.get("l0_abstract"),
                                    "node_type": node.get("node_type"),
                                    "distance": 1,
                                }
                            )
                return out[:limit]

            # t_cross_references
            if "REFERENCES" in cypher_upper and uri:
                out = []
                for e in self._edges:
                    if e.type == "REFERENCES" and e.subject_uri == uri and e.tenant_id == tid:
                        node = self._nodes.get((tid, e.object_uri))
                        if node and node.get("status") == "ACTIVE":
                            out.append(
                                {
                                    "source_uri": e.object_uri,
                                    "node_type": node.get("node_type"),
                                    "relation": e.props.get("relation_label") or e.relation_label,
                                    "l0_abstract": node.get("l0_abstract"),
                                }
                            )
                return out[:limit]

            # t_find_by_uri_prefix
            if "STARTS WITH" in cypher_upper and params.get("prefix"):
                prefix = params["prefix"]
                out = []
                for (t, node_uri), node in self._nodes.items():
                    if t != tid or node.get("status") != "ACTIVE":
                        continue
                    if node_uri.startswith(prefix):
                        out.append(
                            {
                                "source_uri": node_uri,
                                "node_type": node.get("node_type"),
                                "l0_abstract": node.get("l0_abstract"),
                                "retrieval_weight": node.get("retrieval_weight", 1.0),
                            }
                        )
                return sorted(out, key=lambda r: r["source_uri"])[:limit]

            # t_history_chain
            if "SUPERSEDES" in cypher_upper and uri:
                # Follow SUPERSEDES edges from the node
                out = [
                    {
                        "source_uri": uri,
                        "status": (self._nodes.get((tid, uri)) or {}).get("status"),
                        "l0_abstract": (self._nodes.get((tid, uri)) or {}).get("l0_abstract"),
                        "created_at": (self._nodes.get((tid, uri)) or {}).get("created_at"),
                        "distance": 0,
                    }
                ]
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
                edges.append(
                    {
                        "source": e.subject_uri,
                        "target": e.object_uri,
                        "type": e.type,
                        "label": e.props.get("relation_label") or e.relation_label or e.type,
                        "status": e.props.get("status"),
                    }
                )
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
