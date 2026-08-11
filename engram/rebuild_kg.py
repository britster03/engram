"""Safely reconstruct tenant-scoped graph projections from authoritative data."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from engram import frontmatter
from engram import uri as uri_mod
from engram.config import EngramConfig
from engram.ingest.conflict import apply_decision, classify, restore_decision
from engram.ingest.facts import fact_uri
from engram.models.embeddings import EmbeddingService
from engram.storage.filesystem import FilesystemStore, is_generated_memory_path
from engram.storage.graph_projection import memory_l0_abstract, project_memory_node
from engram.storage.neo4j_store import Neo4jStore
from engram.storage.sqlite import SqliteStore
from engram.tenancy import (
    DEFAULT_TENANT_ID,
    Tenant,
    TenantQuotas,
    get_current_tenant,
    set_current_tenant,
)

log = logging.getLogger(__name__)


def rebuild(
    cfg: EngramConfig,
    *,
    tenant_ids: list[str] | None = None,
    dry_run: bool = True,
    allow_global: bool = False,
    graph: Any | None = None,
    embed: Any | None = None,
) -> dict[str, Any]:
    """Plan or rebuild only explicitly resolved tenants.

    Passing no ``tenant_ids`` is accepted only with ``allow_global=True``.
    ``dry_run`` performs validation and reports the exact scope without opening
    Neo4j or deleting anything.
    """
    fs = FilesystemStore(cfg.filesystem.data_dir)
    sqlite = SqliteStore(cfg.event_ledger.path)
    available = dict(_tenant_roots(fs, sqlite))
    if tenant_ids:
        requested = list(dict.fromkeys(tenant_ids))
    elif allow_global:
        requested = sorted(available)
    else:
        raise ValueError("rebuild requires explicit tenant_ids or allow_global=True")
    missing = [tenant_id for tenant_id in requested if tenant_id not in available]
    if missing:
        raise ValueError(f"unknown or empty tenant scope(s): {', '.join(missing)}")

    validated: list[tuple[str, Path, str, frontmatter.MemoryFile]] = []
    failed = 0
    for tenant_id in requested:
        root = available[tenant_id]
        for path in sorted(root.rglob("*.md")):
            if is_generated_memory_path(path):
                continue
            try:
                memory = frontmatter.parse(path.read_text(encoding="utf-8"))
                frontmatter.validate_required_keys(memory.frontmatter)
                frontmatter.validate_metadata(memory.frontmatter)
                validated.append((tenant_id, root, uri_mod.path_to_uri(path, root), memory))
            except Exception:
                log.warning("rebuild validation rejected %s", path, exc_info=True)
                failed += 1

    placeholders = ",".join("?" for _ in requested)
    event_count = int(
        sqlite.get_conn()
        .execute(
            "SELECT COUNT(*) AS c FROM events WHERE tenant_id IN ("
            + placeholders
            + ") AND status IN ('INDEXED', 'COMPLETE')",
            tuple(requested),
        )
        .fetchone()["c"]
    )
    stats: dict[str, Any] = {
        "dry_run": dry_run,
        "tenants": requested,
        "nodes_planned": len(validated),
        "events_planned": event_count,
        "nodes_written": 0,
        "edges_written": 0,
        "failed": failed,
    }
    if dry_run:
        return stats
    if failed:
        raise RuntimeError(
            f"rebuild validation failed for {failed} source file(s); no graph data was deleted"
        )

    owned_graph = graph is None
    graph = graph or Neo4jStore(cfg.knowledge_graph)
    embed = embed or EmbeddingService.get(cfg.gating)
    graph.ensure_indexes()
    graph.delete_tenant_data(requested)
    try:
        for tenant_id, _root, source_uri, memory in validated:
            abstract = memory_l0_abstract(memory, fallback=Path(source_uri).name)
            graph.merge_node(
                source_uri=source_uri,
                parent_uri=uri_mod.parent_uri(source_uri),
                tenant_id=tenant_id,
                properties=project_memory_node(
                    memory,
                    l0_abstract=abstract,
                    l0_embedding=embed.embed(abstract),
                ),
            )
            stats["nodes_written"] += 1

        stats["edges_written"] += _rebuild_fact_references(graph, validated)
        stats["edges_written"] += _rebuild_assertions(
            graph=graph,
            embed=embed,
            sqlite=sqlite,
            tenant_ids=requested,
            assertion_uris={
                (tenant_id, source_uri)
                for tenant_id, _root, source_uri, memory in validated
                if memory.frontmatter.get("node_type") == "FACT"
            },
            conflict_decisions={
                (tenant_id, source_uri): dict(memory.frontmatter["conflict"])
                for tenant_id, _root, source_uri, memory in validated
                if memory.frontmatter.get("node_type") == "FACT"
                and isinstance(memory.frontmatter.get("conflict"), dict)
            },
        )
    finally:
        if owned_graph:
            graph.close()
    return stats


def _rebuild_fact_references(
    graph: Any,
    memories: list[tuple[str, Path, str, frontmatter.MemoryFile]],
) -> int:
    written = 0
    for tenant_id, _root, source_uri, memory in memories:
        fm = memory.frontmatter
        fact = fm.get("fact")
        if fm.get("node_type") != "FACT" or not isinstance(fact, dict):
            continue
        event_id = str((fm.get("provenance") or {}).get("ingest_event_id") or "")
        source_turn_ids = list(fm.get("source_turn_ids") or [])
        confidence = float((fm.get("provenance") or {}).get("confidence") or 0.0)
        episode_uri = str(
            fm.get("source_episode_uri")
            or (f"mem://user/episodes/{event_id}.md" if event_id else "")
        )
        if episode_uri:
            graph.merge_edge(
                subject_uri=episode_uri,
                object_uri=source_uri,
                relation_label="assertion",
                edge_type="REFERENCES",
                tenant_id=tenant_id,
                properties={
                    "created_at": str(fm.get("created_at") or ""),
                    "ingest_event_id": event_id,
                    "confidence": confidence,
                    "source_turn_ids": source_turn_ids,
                },
            )
            written += 1
        for role in ("subject", "object"):
            target = fact.get(f"{role}_uri")
            if not target:
                continue
            graph.merge_edge(
                subject_uri=source_uri,
                object_uri=str(target),
                relation_label=role,
                edge_type="REFERENCES",
                tenant_id=tenant_id,
                properties={
                    "created_at": str(fm.get("created_at") or ""),
                    "ingest_event_id": event_id,
                    "confidence": confidence,
                    "source_turn_ids": source_turn_ids,
                },
            )
            written += 1
    return written


def _rebuild_assertions(
    *,
    graph: Any,
    embed: Any,
    sqlite: SqliteStore,
    tenant_ids: list[str],
    assertion_uris: set[tuple[str, str]],
    conflict_decisions: dict[tuple[str, str], dict[str, Any]],
) -> int:
    placeholders = ",".join("?" for _ in tenant_ids)
    rows = (
        sqlite.get_conn()
        .execute(
            "SELECT e.event_id, e.tenant_id, e.created_at, e.payload, x.triplets "
            "FROM events e JOIN extractions x ON x.event_id = e.event_id "
            "WHERE e.tenant_id IN ("
            + placeholders
            + ") AND e.status IN ('INDEXED', 'COMPLETE') ORDER BY e.created_at, e.event_id",
            tuple(tenant_ids),
        )
        .fetchall()
    )
    written = 0
    previous_tenant = get_current_tenant()
    try:
        for row in rows:
            event_id = str(row["event_id"])
            tenant_id = str(row["tenant_id"] or DEFAULT_TENANT_ID)
            set_current_tenant(
                Tenant(tenant_id=tenant_id, display_name=tenant_id, quotas=TenantQuotas())
            )
            try:
                triplets = json.loads(row["triplets"])
                payload = json.loads(row["payload"])
            except Exception:
                continue
            link_rows = (
                sqlite.get_conn()
                .execute(
                    "SELECT triplet_idx, subject_node_id, object_node_id FROM linked_entities "
                    "WHERE event_id = ? AND tenant_id = ?",
                    (event_id, tenant_id),
                )
                .fetchall()
            )
            links = {
                int(link["triplet_idx"]): (link["subject_node_id"], link["object_node_id"])
                for link in link_rows
            }
            source_turn_ids = _source_turn_ids(payload)
            episode_uri = f"mem://user/episodes/{event_id}.md"
            for idx, trip in enumerate(triplets):
                relation = trip.get("relation_canonical") or trip.get("relation")
                confidence = float(trip.get("confidence", 0.0))
                if not relation or confidence < 0.6:
                    continue
                subject_uri, object_uri = links.get(idx, (None, None))
                if not subject_uri or not object_uri:
                    continue
                candidate_assertion_uri = fact_uri(event_id, idx, trip)
                assertion_uri = (
                    candidate_assertion_uri
                    if (tenant_id, candidate_assertion_uri) in assertion_uris
                    else None
                )
                persisted = conflict_decisions.get((tenant_id, candidate_assertion_uri))
                if persisted is not None:
                    decision = restore_decision(
                        neo4j=graph,
                        subject_uri=str(subject_uri),
                        persisted=persisted,
                    )
                else:
                    decision = classify(
                        neo4j=graph,
                        embed=embed,
                        subject_uri=str(subject_uri),
                        relation_label=str(relation),
                        object_uri=str(object_uri),
                        object_abstract=str(trip.get("object") or ""),
                        core=None,
                        incoming_confidence=confidence,
                        allow_contradiction=trip.get("explicit_correction") is True,
                    )
                apply_decision(
                    neo4j=graph,
                    decision=decision,
                    subject_uri=str(subject_uri),
                    object_uri=str(object_uri),
                    relation_label=str(relation),
                    properties={
                        "confidence": confidence,
                        "created_at": str(row["created_at"]),
                        "ingest_event_id": event_id,
                        "source_turn_ids": source_turn_ids,
                        **({"assertion_uri": assertion_uri} if assertion_uri is not None else {}),
                    },
                    incoming_assertion_uri=assertion_uri,
                )
                if decision.case != "DUPLICATE":
                    written += 1
                graph.merge_edge(
                    subject_uri=episode_uri,
                    object_uri=str(subject_uri),
                    relation_label="mentions",
                    edge_type="REFERENCES",
                    tenant_id=tenant_id,
                    properties={
                        "created_at": str(row["created_at"]),
                        "ingest_event_id": event_id,
                        "source_turn_ids": source_turn_ids,
                    },
                )
                written += 1
    finally:
        set_current_tenant(previous_tenant)
    return written


def _source_turn_ids(payload: dict[str, Any]) -> list[str]:
    pair = payload.get("turn_pair") or payload.get("turn_group") or payload
    if not isinstance(pair, dict):
        return []
    values: list[str] = []
    for role in ("user", "assistant"):
        turn = pair.get(role)
        if isinstance(turn, dict) and turn.get("external_id"):
            value = str(turn["external_id"])
            if value not in values:
                values.append(value)
    return values


def _tenant_roots(fs: FilesystemStore, sqlite: SqliteStore) -> list[tuple[str, Path]]:
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
    if any((data_dir / namespace).exists() for namespace in ("user", "system", "org")):
        return [(DEFAULT_TENANT_ID, data_dir)]
    return []


def _known_tenant_ids(sqlite: SqliteStore) -> set[str]:
    conn = sqlite.get_conn()
    tables = {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }
    tenant_ids = {DEFAULT_TENANT_ID}
    for table in (
        "tenants",
        "events",
        "extractions",
        "linked_entities",
        "fs_outbox",
        "ingest_artifacts",
        "consolidation_tasks",
    ):
        if table not in tables:
            continue
        rows = conn.execute(
            f"SELECT DISTINCT tenant_id FROM {table} WHERE tenant_id IS NOT NULL"
        ).fetchall()
        tenant_ids.update(str(row["tenant_id"]) for row in rows if row["tenant_id"])
    return tenant_ids
