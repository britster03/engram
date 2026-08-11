"""Consolidation task handlers (§7.3).

Implements every task type from the SDD:
  - CONSOLIDATE_OVERVIEW: regenerate overview.md for a directory via Core Model.
  - REGENERATE_MANIFEST: rebuild the .manifest file from current children.
  - PROPAGATE_OVERVIEW: enqueue CONSOLIDATE_OVERVIEW for each ancestor up to root.
  - ATOMIZE: split compound triplets stored for an event into single-claim triplets.
  - NORMALIZE: canonicalise entity names and relation labels.
  - TEMPORALIZE: attach temporal scope (valid_from/valid_until/phrase) to nodes.
  - INTEGRATE: replay atomized+normalized+temporalized facts back into the KG.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from engram import frontmatter, prompts
from engram import uri as uri_mod
from engram.config import ConsolidationConfig
from engram.models.core import CoreModelProvider
from engram.models.embeddings import EmbeddingService
from engram.storage.filesystem import FilesystemStore
from engram.storage.neo4j_store import Neo4jStore
from engram.storage.sqlite import SqliteStore

log = logging.getLogger(__name__)

TaskHandler = Callable[..., None]


def handle_regenerate_manifest(
    *,
    node_id: str,
    fs: FilesystemStore,
    cfg: ConsolidationConfig,
) -> None:
    """Rebuild .manifest for a directory URI."""
    dir_uri = node_id
    if not dir_uri.endswith("/"):
        # Treat the node_id as a directory URI; strip trailing filename if any.
        base = fs.path_for(dir_uri)
        if base.is_file():
            dir_uri = uri_mod.parent_uri(dir_uri) or dir_uri
    children_uris = fs.list_children(dir_uri)
    lines: list[str] = []
    for child_uri in children_uris:
        path = fs.path_for(child_uri)
        rel = path.name
        if path.is_dir():
            abstract = _directory_summary(fs, child_uri)
            lines.append(f"{rel}/ | {abstract}")
        else:
            abstract = _file_abstract(path)
            lines.append(f"{rel} | {abstract}")
    content = "\n".join(lines) + ("\n" if lines else "")
    fs.write_manifest(dir_uri, content)


def handle_consolidate_overview(
    *,
    node_id: str,
    fs: FilesystemStore,
    neo4j: Neo4jStore,
    core: CoreModelProvider,
    cfg: ConsolidationConfig,
    overview_cache: Any | None = None,
) -> None:
    """Regenerate overview.md for a directory via the Core Model (§7.3).

    After writing the new overview, invalidate any distributed cache entry
    so L3 reads pick up the fresh copy on the next query. The cache is
    optional — when None, behaviour is unchanged.
    """
    from engram.tenancy import current_tenant_id

    dir_uri = node_id
    if not fs.path_for(dir_uri).is_dir():
        return
    children_uris = fs.list_children(dir_uri)
    children_abstracts: list[dict] = []
    for curi in children_uris:
        path = fs.path_for(curi)
        abs_ = _file_abstract(path) if path.is_file() else _directory_summary(fs, curi)
        children_abstracts.append({"source_uri": curi, "abstract": abs_})
    children_relations = _collect_relations(neo4j, children_uris)

    text: str
    if len(children_abstracts) <= 1:
        if children_abstracts:
            child = children_abstracts[0]
            text = f"# Overview\n\n- {child['source_uri']}: {child['abstract']}\n"
        else:
            text = "# Overview\n\nThis directory contains no memory documents.\n"
    else:
        prompt = prompts.render(
            "overview",
            directory_uri=dir_uri,
            children_abstracts=children_abstracts,
            children_relations=children_relations,
            overview_max_tokens=cfg.overview_max_tokens,
        )
        result = core.complete(
            system_prompt=prompt,
            user_prompt="Return the overview as Markdown.",
        )
        if isinstance(result.output, dict) and "overview" in result.output:
            text = str(result.output["overview"])
        elif isinstance(result.output, str):
            text = result.output
        else:
            text = result.raw_text
    fs.write_atomic(
        f"{dir_uri.rstrip('/')}/overview.md",
        text if text.endswith("\n") else text + "\n",
    )
    # Invalidate any cached view of this overview so L3 reads the fresh version.
    if overview_cache is not None:
        try:
            overview_cache.invalidate(current_tenant_id(), dir_uri)
        except Exception:
            log.debug("overview cache invalidation failed", exc_info=True)
    # Update overview_generated_at on the KG directory node.
    now = datetime.now(timezone.utc).isoformat()
    neo4j.merge_node(
        source_uri=dir_uri,
        properties={"overview_generated_at": now},
    )


def handle_propagate_overview(
    *,
    node_id: str,
    sqlite: SqliteStore,
    cfg: ConsolidationConfig,
    tenant_id: str | None = None,
) -> None:
    """Enqueue CONSOLIDATE_OVERVIEW for each ancestor up to the root, deduped."""
    ancestor = uri_mod.parent_uri(node_id)
    while ancestor is not None:
        sqlite.enqueue_task(
            node_id=ancestor,
            task_type="CONSOLIDATE_OVERVIEW",
            priority=5,
            tenant_id=tenant_id or "_default",
        )
        ancestor = uri_mod.parent_uri(ancestor)


# ----------------------------------------------------------------------
# ATOMIZE / NORMALIZE / TEMPORALIZE / INTEGRATE (§7.3)
# ----------------------------------------------------------------------

_COMPOUND_TOKEN = re.compile(r"\s+and\s+|\s+&\s+", re.IGNORECASE)


def handle_atomize(
    *,
    node_id: str,
    sqlite: SqliteStore,
    cfg: ConsolidationConfig,
) -> None:
    """Split compound objects/subjects in an extraction into independent triplets.

    `node_id` may be either an event_id or a mem:// URI. For event_id form we
    rewrite `extractions.triplets`; for URI form we walk `linked_entities` back
    to events and atomize each.
    """
    event_ids = _event_ids_for(sqlite, node_id)
    if not event_ids:
        return
    for event_id in event_ids:
        row = sqlite.get_conn().execute(
            "SELECT triplets FROM extractions WHERE event_id = ?", (event_id,)
        ).fetchone()
        if row is None:
            continue
        triplets: list[dict[str, Any]] = json.loads(row["triplets"])
        atomic: list[dict[str, Any]] = []
        for trip in triplets:
            pieces = _COMPOUND_TOKEN.split(str(trip.get("object", "")))
            if len(pieces) <= 1:
                atomic.append(trip)
                continue
            for piece in pieces:
                piece = piece.strip(" ,.;:")
                if piece:
                    atomic.append({**trip, "object": piece})
        if len(atomic) != len(triplets):
            with sqlite.transaction() as conn:
                conn.execute(
                    "UPDATE extractions SET triplets = ? WHERE event_id = ?",
                    (json.dumps(atomic), event_id),
                )


def handle_normalize(
    *,
    node_id: str,
    sqlite: SqliteStore,
    neo4j: Neo4jStore,
    embed: EmbeddingService,
    cfg: ConsolidationConfig,
) -> None:
    """Canonicalise entity names AND relation labels (§6.4.2).

    For each distinct subject/object string across the relevant extractions,
    find the nearest existing canonical entity name in the KG by cosine. For
    each relation label, look up the controlled vocabulary at
    `prompts/relations.yaml` (exact match → alias map → cosine ≥ 0.85).
    Writes the canonicalised forms back into `extractions.triplets` so a
    subsequent INTEGRATE replays with the normalised labels.
    """
    from engram.relations import vocabulary

    vocab = vocabulary()
    event_ids = _event_ids_for(sqlite, node_id)
    for event_id in event_ids:
        row = sqlite.get_conn().execute(
            "SELECT triplets FROM extractions WHERE event_id = ?", (event_id,)
        ).fetchone()
        if row is None:
            continue
        triplets: list[dict[str, Any]] = json.loads(row["triplets"])
        for trip in triplets:
            for key in ("subject", "object"):
                raw = trip.get(key)
                if not raw or not isinstance(raw, str):
                    continue
                normalized = _canonicalise(neo4j, embed, raw)
                if normalized and normalized != raw:
                    trip[f"{key}_canonical"] = normalized
            rel = trip.get("relation")
            if isinstance(rel, str):
                canon_rel = vocab.canonicalise(rel, embed)
                if canon_rel and canon_rel != rel:
                    trip["relation_canonical"] = canon_rel
        with sqlite.transaction() as conn:
            conn.execute(
                "UPDATE extractions SET triplets = ? WHERE event_id = ?",
                (json.dumps(triplets), event_id),
            )


def handle_temporalize(
    *,
    node_id: str,
    fs: FilesystemStore,
    cfg: ConsolidationConfig,
) -> None:
    """Attach temporal metadata to a memory file when a date is parseable from
    the body. Very conservative — only writes when a single unambiguous date
    appears in the opening sentence.
    """
    if not fs.exists(node_id):
        return
    raw = fs.read(node_id)
    mf = frontmatter.parse(raw)
    if "temporal" in mf.frontmatter and mf.frontmatter["temporal"].get("valid_from"):
        return  # already temporalised
    body_head = mf.body.splitlines()[0] if mf.body.strip() else ""
    iso = _extract_iso_date(body_head)
    if iso is None:
        return
    mf.frontmatter.setdefault("temporal", {})
    mf.frontmatter["temporal"].update(
        {
            "asserted_at": datetime.now(timezone.utc).isoformat(),
            "valid_from": iso,
            "valid_until": None,
            "phrase": body_head,
        }
    )
    fs.write_atomic(node_id, mf.serialize())


def handle_integrate(
    *,
    node_id: str,
    sqlite: SqliteStore,
    cfg: ConsolidationConfig,
) -> None:
    """Mark an event as re-runnable so the next reconciliation pass re-indexes.

    ATOMIZE/NORMALIZE/TEMPORALIZE mutate SQLite-side data; INTEGRATE reruns
    step 6 (KG index update) for each affected event.
    """
    event_ids = _event_ids_for(sqlite, node_id)
    for event_id in event_ids:
        with sqlite.transaction() as conn:
            conn.execute(
                "UPDATE events SET status = 'GATED_STORE', processed_at = NULL "
                "WHERE event_id = ?",
                (event_id,),
            )


def handle_unmerge(
    *,
    node_id: str,
    fs: FilesystemStore,
    neo4j: Neo4jStore,
    sqlite: SqliteStore,
    core: CoreModelProvider,
    embed: EmbeddingService,
    cfg: ConsolidationConfig,
    tenant_id: str | None = None,
) -> None:
    """Split a merged ENTITY back into its contributing sources (§8.6).

    Driven off the consolidation queue so the LLM call never blocks an API
    request. Also enqueues overview + manifest regen for touched ancestors.
    """
    from engram.ingest.unmerge import unmerge as do_unmerge

    result = do_unmerge(
        fs=fs, neo4j=neo4j, sqlite=sqlite, core=core, embed=embed,
        merged_uri=node_id,
    )
    for uri in [result.merged_uri, *result.split_uris]:
        parent = uri_mod.parent_uri(uri)
        if parent:
            sqlite.enqueue_task(
                node_id=parent,
                task_type="CONSOLIDATE_OVERVIEW",
                priority=5,
                tenant_id=tenant_id or "_default",
            )
            sqlite.enqueue_task(
                node_id=parent,
                task_type="REGENERATE_MANIFEST",
                priority=5,
                tenant_id=tenant_id or "_default",
            )


# ----------------------------------------------------------------------
# Helpers for atomize / normalize / integrate
# ----------------------------------------------------------------------

def _event_ids_for(sqlite: SqliteStore, node_id: str) -> list[str]:
    """Resolve `node_id` to the list of contributing event_ids."""
    if node_id.startswith("evt-"):
        return [node_id]
    conn = sqlite.get_conn()
    rows = conn.execute(
        "SELECT DISTINCT event_id FROM linked_entities "
        "WHERE subject_node_id = ? OR object_node_id = ?",
        (node_id, node_id),
    ).fetchall()
    return [r["event_id"] for r in rows]


def _canonicalise(neo4j: Neo4jStore, embed: EmbeddingService, name: str) -> str | None:
    vec = embed.embed(name)
    try:
        hits = neo4j.vector_search(
            vec, k=3, uri_prefix="mem://user/entities/", dormant_floor=0.0,
        )
    except Exception:
        return None
    for hit in hits:
        if float(hit.get("score", 0.0)) >= 0.85:
            abstract = str(hit.get("l0_abstract") or "")
            first = abstract.split(" — ", 1)[0]
            return first.strip() or None
    return None


_DATE_RE = re.compile(
    r"\b(?P<year>20\d{2})-(?P<month>\d{2})-(?P<day>\d{2})\b"
)


def _extract_iso_date(text: str) -> str | None:
    m = _DATE_RE.search(text)
    if m:
        return f"{m.group('year')}-{m.group('month')}-{m.group('day')}"
    return None


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _file_abstract(path: Path) -> str:
    if not path.name.endswith(".md"):
        return path.name
    try:
        text = path.read_text(encoding="utf-8")
        mf = frontmatter.parse(text)
    except Exception:
        return path.name
    body = mf.body.strip()
    if not body:
        return "(empty)"
    return body.splitlines()[0][:160]


def _directory_summary(fs: FilesystemStore, dir_uri: str) -> str:
    overview = fs.read_overview(dir_uri)
    if overview:
        return overview.splitlines()[0][:160]
    return "(directory)"


def _collect_relations(neo4j: Neo4jStore, uris: list[str]) -> list[dict]:
    if not uris:
        return []
    relations: list[dict] = []
    for uri in uris:
        try:
            rows = neo4j.run_template(
                "MATCH (n:Node {source_uri: $uri})-[r:RELATES_TO]->(m:Node) "
                "WHERE r.status = 'ACTIVE' AND m.status = 'ACTIVE' "
                "RETURN n.source_uri AS subject_uri, r.relation_label AS relation, "
                "m.source_uri AS object_uri LIMIT 25",
                {"uri": uri},
                timeout_s=5,
            )
        except Exception:
            rows = []
        relations.extend(rows)
    return relations
