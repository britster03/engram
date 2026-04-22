"""Unmerge operation (§8.6).

Splits a merged ENTITY node back into its contributing source extractions.
The merged node becomes HISTORICAL with `superseded_by` pointing to the list
of newly-created splits.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from slugify import slugify

from engram import frontmatter, prompts
from engram.models.core import CoreModelProvider
from engram.models.embeddings import EmbeddingService
from engram.storage.filesystem import FilesystemStore
from engram.storage.neo4j_store import Neo4jStore
from engram.storage.sqlite import SqliteStore

log = logging.getLogger(__name__)


@dataclass
class UnmergeResult:
    merged_uri: str
    split_uris: list[str]


def unmerge(
    *,
    fs: FilesystemStore,
    neo4j: Neo4jStore,
    sqlite: SqliteStore,
    core: CoreModelProvider,
    embed: EmbeddingService,
    merged_uri: str,
) -> UnmergeResult:
    """Split a merged ENTITY back into per-source splits."""
    if not fs.exists(merged_uri):
        raise FileNotFoundError(merged_uri)
    raw = fs.read(merged_uri)
    mf = frontmatter.parse(raw)
    merged_abstract = mf.body.strip().splitlines()[0] if mf.body.strip() else ""
    extractions = _contributing_extractions(sqlite, merged_uri)
    if not extractions:
        # Nothing to split from — degenerate to retire.
        _retire_node(fs, neo4j, merged_uri)
        return UnmergeResult(merged_uri=merged_uri, split_uris=[])

    prompt = prompts.render(
        "unmerge",
        merged_node={"source_uri": merged_uri, "l0_abstract": merged_abstract},
        source_extractions=extractions,
    )
    result = core.complete(
        system_prompt=prompt,
        user_prompt="Return the split JSON.",
    )
    out = result.output if isinstance(result.output, dict) else {}
    splits = out.get("splits") or []
    if not splits:
        # Disambiguation returned nothing — keep original but flag for review.
        return UnmergeResult(merged_uri=merged_uri, split_uris=[])

    now = datetime.now(timezone.utc).isoformat()
    new_uris: list[str] = []
    for split in splits:
        name = str(split.get("name", "")).strip()
        if not name:
            continue
        l0 = str(split.get("l0_abstract", "")).strip() or f"{name} (entity)."
        triplets = split.get("triplets") or []
        slug = f"{slugify(name, separator='-', lowercase=True)}-split-{uuid.uuid4().hex[:6]}"
        split_uri = f"mem://user/entities/{slug}/{slug}.md"
        fm_ = {
            "id": str(uuid.uuid4()),
            "node_type": "ENTITY",
            "status": "ACTIVE",
            "created_at": now,
            "schema_version": 1,
            "normalize": {"canonical_name": name, "aliases": [name]},
            "provenance": {
                "extractor": "unmerge_v1",
                "confidence": 0.85,
                "superseded_from": merged_uri,
            },
        }
        mf_ = frontmatter.MemoryFile(frontmatter=fm_, body=f"{l0}\n")
        fs.write_atomic(split_uri, mf_.serialize())
        emb = embed.embed(name)
        neo4j.merge_node(
            source_uri=split_uri,
            parent_uri=f"mem://user/entities/{slug}",
            properties={
                "id": fm_["id"],
                "node_type": "ENTITY",
                "status": "ACTIVE",
                "l0_abstract": l0,
                "l0_embedding": emb,
                "retrieval_weight": 1.0,
                "created_at": now,
                "last_accessed_at": now,
                "access_count": 0,
                "schema_version": 1,
            },
        )
        # Replay triplets from this split as new ACTIVE edges.
        for trip in triplets:
            s = trip.get("subject")
            o = trip.get("object")
            rel = trip.get("relation")
            if not (s and o and rel):
                continue
            o_slug = slugify(str(o), separator="-", lowercase=True)
            o_uri = f"mem://user/entities/{o_slug}/{o_slug}.md"
            neo4j.merge_edge(
                subject_uri=split_uri,
                object_uri=o_uri,
                relation_label=str(rel),
                edge_type="RELATES_TO",
                properties={
                    "confidence": float(trip.get("confidence", 0.7)),
                    "status": "ACTIVE",
                    "created_at": now,
                    "source": "unmerge",
                },
            )
        new_uris.append(split_uri)

    # Mark merged node HISTORICAL and point SUPERSEDES edges at the splits.
    mf.frontmatter["status"] = "HISTORICAL"
    mf.frontmatter.setdefault("provenance", {})["unmerge_at"] = now
    mf.frontmatter["provenance"]["superseded_by"] = new_uris
    fs.write_atomic(merged_uri, mf.serialize())
    try:
        neo4j.run_template(
            "MATCH (n:Node {source_uri: $uri}) SET n.status = 'HISTORICAL', "
            "n.superseded_at = $now",
            {"uri": merged_uri, "now": now},
        )
        for new_uri in new_uris:
            neo4j.merge_edge(
                subject_uri=new_uri,
                object_uri=merged_uri,
                relation_label="supersedes",
                edge_type="SUPERSEDES",
                properties={"created_at": now, "source": "unmerge"},
            )
    except Exception:
        log.exception("KG update failed during unmerge; filesystem is authoritative")

    return UnmergeResult(merged_uri=merged_uri, split_uris=new_uris)


def _contributing_extractions(sqlite: SqliteStore, merged_uri: str) -> list[dict[str, Any]]:
    """Fetch every extraction triplet that linked to `merged_uri` on either side."""
    conn = sqlite.get_conn()
    rows = conn.execute(
        "SELECT le.event_id, le.triplet_idx FROM linked_entities le "
        "WHERE le.subject_node_id = ? OR le.object_node_id = ?",
        (merged_uri, merged_uri),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        extraction = None
        ext_row = conn.execute(
            "SELECT * FROM extractions WHERE event_id = ?", (r["event_id"],)
        ).fetchone()
        if ext_row is None:
            continue
        import json
        triplets = json.loads(ext_row["triplets"])
        idx = int(r["triplet_idx"])
        if 0 <= idx < len(triplets):
            trip = triplets[idx]
            out.append({
                "event_id": r["event_id"],
                "subject": trip.get("subject"),
                "relation": trip.get("relation"),
                "object": trip.get("object"),
                "confidence": trip.get("confidence"),
                "l0_abstract": ext_row["l0_abstract"],
            })
    return out


def _retire_node(fs: FilesystemStore, neo4j: Neo4jStore, source_uri: str) -> None:
    raw = fs.read(source_uri)
    mf = frontmatter.parse(raw)
    mf.frontmatter["status"] = "HISTORICAL"
    fs.write_atomic(source_uri, mf.serialize())
    try:
        neo4j.run_template(
            "MATCH (n:Node {source_uri: $uri}) SET n.status = 'HISTORICAL'",
            {"uri": source_uri},
        )
    except Exception:
        log.exception("failed to retire node: %s", source_uri)
