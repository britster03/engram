"""`engram rebuild-kg` — reconstruct Neo4j from the filesystem + extractions (§2.3).

The filesystem holds the canonical node content; the SQLite `extractions`
table holds the source-of-truth for semantic edges (RELATES_TO). Together
they fully reconstruct the KG. This routine:

  1. Wipes every :Node in Neo4j.
  2. Walks the filesystem for every .md, re-inserts the node + CONTAINS edges.
  3. Replays the extractions table, re-adding RELATES_TO and REFERENCES edges
     via the linked_entities mapping that was written at ingest time.

Disaster recovery: lose Neo4j → run this → retrieval works again.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from engram import frontmatter
from engram import uri as uri_mod
from engram.config import EngramConfig
from engram.models.embeddings import EmbeddingService
from engram.storage.filesystem import FilesystemStore, is_generated_memory_path
from engram.storage.neo4j_store import Neo4jStore
from engram.storage.sqlite import SqliteStore
from engram.tenancy import DEFAULT_TENANT_ID

log = logging.getLogger(__name__)


def rebuild(cfg: EngramConfig) -> dict[str, int]:
    fs = FilesystemStore(cfg.filesystem.data_dir)
    neo4j = Neo4jStore(cfg.knowledge_graph)
    neo4j.ensure_indexes()
    embed = EmbeddingService.get(cfg.gating)
    sqlite = SqliteStore(cfg.event_ledger.path)
    stats = {"nodes_written": 0, "edges_written": 0, "failed": 0}

    # Wipe existing :Node entries
    with neo4j.writer().session() as session:
        session.run("MATCH (n:Node) DETACH DELETE n")

    # Pass 1: nodes + CONTAINS, walking each tenant under its own URI scope.
    for tenant_id, tenant_root in _tenant_roots(fs, sqlite):
        for path in sorted(tenant_root.rglob("*.md")):
            if is_generated_memory_path(path):
                continue
            try:
                text = path.read_text(encoding="utf-8")
                mf = frontmatter.parse(text)
            except Exception:
                log.warning("skipping unreadable file: %s", path)
                stats["failed"] += 1
                continue
            source_uri = uri_mod.path_to_uri(path, tenant_root)
            parent = uri_mod.parent_uri(source_uri)
            body = mf.body.strip()
            l0 = body.splitlines()[0] if body else ""
            emb = embed.embed(l0 or path.name)
            now = datetime.now(timezone.utc).isoformat()
            neo4j.merge_node(
                source_uri=source_uri,
                parent_uri=parent,
                tenant_id=tenant_id,
                properties={
                    "id": mf.frontmatter.get("id") or "",
                    "node_type": mf.frontmatter.get("node_type", "DOCUMENT"),
                    "status": mf.frontmatter.get("status", "ACTIVE"),
                    "l0_abstract": l0 or path.name,
                    "l0_embedding": emb,
                    "retrieval_weight": 1.0,
                    "created_at": mf.frontmatter.get("created_at", now),
                    "last_accessed_at": now,
                    "access_count": 0,
                    "schema_version": int(mf.frontmatter.get("schema_version", 1)),
                },
            )
            stats["nodes_written"] += 1

    # Pass 2: semantic edges from extractions + linked_entities
    conn = sqlite.get_conn()
    rows = conn.execute(
        "SELECT e.event_id, COALESCE(e.tenant_id, x.tenant_id, ?) AS tenant_id, "
        "x.triplets, x.l0_abstract "
        "FROM events e JOIN extractions x ON x.event_id = e.event_id "
        "WHERE e.status IN ('INDEXED', 'COMPLETE')"
        ,
        (DEFAULT_TENANT_ID,),
    ).fetchall()
    for row in rows:
        tenant_id = row["tenant_id"] or DEFAULT_TENANT_ID
        try:
            triplets = json.loads(row["triplets"])
        except Exception:
            continue
        link_rows = conn.execute(
            "SELECT triplet_idx, subject_node_id, object_node_id "
            "FROM linked_entities WHERE event_id = ? AND tenant_id = ?",
            (row["event_id"], tenant_id),
        ).fetchall()
        if not link_rows:
            link_rows = conn.execute(
                "SELECT triplet_idx, subject_node_id, object_node_id "
                "FROM linked_entities WHERE event_id = ?",
                (row["event_id"],),
            ).fetchall()
        links = {int(r["triplet_idx"]): (r["subject_node_id"], r["object_node_id"])
                 for r in link_rows}
        for idx, trip in enumerate(triplets):
            rel = trip.get("relation")
            conf = float(trip.get("confidence", 0.0))
            if not rel or conf < 0.3:
                continue
            s_uri, o_uri = links.get(idx, (None, None))
            if not (s_uri and o_uri):
                continue
            now = datetime.now(timezone.utc).isoformat()
            neo4j.merge_edge(
                subject_uri=s_uri,
                object_uri=o_uri,
                relation_label=rel,
                edge_type="RELATES_TO",
                tenant_id=tenant_id,
                properties={
                    "confidence": conf,
                    "status": "ACTIVE",
                    "created_at": now,
                    "ingest_event_id": row["event_id"],
                    "source": "rebuild_kg",
                },
            )
            stats["edges_written"] += 1
    neo4j.close()
    return stats


def _tenant_roots(fs: FilesystemStore, sqlite: SqliteStore) -> list[tuple[str, Path]]:
    """Return (tenant_id, root_path) pairs to rebuild.

    Modern stores use `{data_dir}/{tenant_id}/...`. If no tenant roots are
    present, treat the data directory as a legacy single-tenant root.
    """
    data_dir = Path(fs.data_dir)
    tenant_ids = _known_tenant_ids(sqlite)
    roots: list[tuple[str, Path]] = []
    for tenant_id in sorted(tenant_ids):
        root = data_dir / tenant_id
        if root.exists() and root.is_dir():
            roots.append((tenant_id, root))
    if data_dir.exists():
        known = {tenant_id for tenant_id, _ in roots}
        for child in sorted(data_dir.iterdir()):
            if not child.is_dir() or child.name in known or child.name.startswith("."):
                continue
            if child.name in {"user", "system", "org", "project", "projects"}:
                continue
            if any(child.rglob("*.md")):
                roots.append((child.name, child))
    if roots:
        return roots
    return [(DEFAULT_TENANT_ID, data_dir)]


def _known_tenant_ids(sqlite: SqliteStore) -> set[str]:
    conn = sqlite.get_conn()
    tables = {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    tenant_ids = {DEFAULT_TENANT_ID}
    for table in (
        "tenants",
        "events",
        "extractions",
        "linked_entities",
        "fs_outbox",
        "consolidation_tasks",
    ):
        if table not in tables:
            continue
        try:
            rows = conn.execute(
                f"SELECT DISTINCT tenant_id FROM {table} WHERE tenant_id IS NOT NULL"
            ).fetchall()
        except Exception:
            continue
        tenant_ids.update(str(row["tenant_id"]) for row in rows if row["tenant_id"])
    return tenant_ids
