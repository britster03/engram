"""Ingest worker — write-path pipeline (§5.3, §5.4).

Implements the full seven-step pipeline:
  1. Event recording (API thread, sync SQLite insert).
  2. Write-path gate (Core Model).
  3. S-R-O extraction + L0 abstract (Core Model).
  4. Entity linking with disambiguation (Core Model + embeddings).
  5. Filesystem write (authoritative; atomic via temp+rename+fsync).
  6. Dedup / conflict resolution + KG index update.
  7. Consolidation enqueue (CONSOLIDATE_OVERVIEW, REGENERATE_MANIFEST,
     PROPAGATE_OVERVIEW).

Every step is idempotent so the Reconciliation Worker can replay from any
crashed state without data corruption.
"""

from __future__ import annotations

import logging
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from slugify import slugify

from engram import frontmatter, prompts, tracing
from engram import metrics as metrics_mod
from engram import uri as uri_mod
from engram.config import EngramConfig
from engram.ingest.conflict import apply_decision, classify
from engram.ingest.entity_linker import resolve as entity_resolve
from engram.models.core import CoreModelError, CoreModelProvider
from engram.models.embeddings import EmbeddingService
from engram.storage.filesystem import FilesystemStore
from engram.storage.sqlite import SqliteStore
from engram.tenancy import (
    DEFAULT_TENANT_ID,
    Tenant,
    TenantQuotas,
    current_tenant_id,
    set_current_tenant,
)

log = logging.getLogger(__name__)


@dataclass
class IngestContext:
    cfg: EngramConfig
    sqlite: SqliteStore
    fs: FilesystemStore
    neo4j: Any
    core: CoreModelProvider
    embed: EmbeddingService


def process_event(ctx: IngestContext, event_id: str) -> str:
    """Drive one event through the pipeline. Returns the final status string."""
    event = ctx.sqlite.get_event(event_id)
    if event is None:
        raise RuntimeError(f"unknown event_id: {event_id}")
    if event["status"] in {"INDEXED", "COMPLETE", "GATED_SKIP"}:
        return event["status"]

    # Pin the ambient tenant context for every downstream call.
    event_tenant = event.get("tenant_id") or DEFAULT_TENANT_ID
    set_current_tenant(
        Tenant(tenant_id=event_tenant, display_name=event_tenant,
               api_key_hashes=[], quotas=TenantQuotas(), status="ACTIVE")
    )

    payload = event["payload"]
    turn_pair = _turn_pair(payload)
    event_time = _turn_time(payload)

    # --- Step 2: Write-path gate -----------------------------------------
    with _Timed("gate"), tracing.span("ingest.gate", event_id=event_id):
        try:
            gate = _call_gate(ctx, turn_pair, session_summary=payload.get("session_summary"))
        except CoreModelError as err:
            ctx.sqlite.set_event_status(event_id, "FAILED", error_message=str(err))
            raise
        metrics_mod.core_model_calls.labels(task="gate_write",
                                            provider=ctx.cfg.core_model.provider).inc()
    if not gate.get("store", False):
        ctx.sqlite.set_event_status(event_id, "GATED_SKIP", error_message=gate.get("reason"))
        return "GATED_SKIP"
    ctx.sqlite.set_event_status(event_id, "GATED_STORE")

    # --- Step 3: S-R-O extraction + L0 abstract --------------------------
    with _Timed("extract"), tracing.span("ingest.extract", event_id=event_id):
        existing = ctx.sqlite.get_extraction(event_id)
        if existing is None:
            extraction = _call_extract(
                ctx, turn_pair,
                session_context=payload.get("session_context"),
                turn_timestamp=event_time,
            )
            ctx.sqlite.save_extraction(
                event_id=event_id,
                resolved_text=extraction["resolved_text"],
                triplets=extraction["triplets"],
                l0_abstract=extraction["l0_abstract"],
                tenant_id=event_tenant,
            )
            metrics_mod.core_model_calls.labels(task="extract",
                                                provider=ctx.cfg.core_model.provider).inc()
        else:
            extraction = existing

    # --- Step 4: Entity linking with disambiguation ----------------------
    with _Timed("entity_link"), tracing.span("ingest.entity_link", event_id=event_id):
        entities = _resolve_entities(ctx, extraction)

    # --- Step 5: Filesystem write (authoritative) ------------------------
    with _Timed("fs_write"), tracing.span("ingest.fs_write", event_id=event_id):
        event_date = extraction.get("event_date")
        episode_uri, _ = _write_episode(
            ctx, event_id, event["session_id"], extraction,
            event_time=event_time, event_date=event_date,
        )
        entity_records: list[tuple[str, str, str]] = []
        for slug, display_name, matched_uri in entities:
            if matched_uri:
                entity_records.append((slug, display_name, matched_uri))
                continue
            ent_uri = _write_entity(
                ctx, event_id, event["session_id"], slug, display_name,
                extraction["l0_abstract"],
                event_time=event_time, event_date=event_date,
            )
            entity_records.append((slug, display_name, ent_uri))
        ctx.sqlite.fs_outbox_write(event_id, episode_uri, tenant_id=event_tenant)
        _record_linked_entities(
            ctx, event_id, extraction["triplets"], entity_records, tenant_id=event_tenant,
        )

    # --- Step 6: Validate metadata, then dedup/conflict + KG index ------
    with _Timed("kg_index"), tracing.span("ingest.kg_index", event_id=event_id):
        try:
            _validate_written_frontmatter(ctx, episode_uri, entity_records)
            _index_neo4j(ctx, event_id, episode_uri, extraction, entity_records)
        except Exception as err:
            log.exception("KG index failed for event %s", event_id)
            ctx.sqlite.fs_outbox_mark(event_id, "INDEX_FAILED", error=str(err))
            raise
    ctx.sqlite.fs_outbox_mark(event_id, "INDEXED")
    ctx.sqlite.set_event_status(event_id, "INDEXED")

    # --- Step 7: Consolidation enqueue -----------------------------------
    with _Timed("consolidation_enqueue"), tracing.span(
        "ingest.consolidation_enqueue", event_id=event_id,
    ):
        _enqueue_consolidation(ctx, episode_uri, [uri for _, _, uri in entity_records])
    ctx.sqlite.set_event_status(event_id, "COMPLETE")
    return "COMPLETE"


# ----------------------------------------------------------------------
# Step helpers
# ----------------------------------------------------------------------

class _Timed:
    """Small context manager that records stage latency to Prometheus."""

    def __init__(self, stage: str) -> None:
        self.stage = stage
        self._t0 = 0.0

    def __enter__(self) -> _Timed:
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *_exc) -> None:
        dt = time.perf_counter() - self._t0
        metrics_mod.ingest_stage.labels(stage=self.stage).observe(dt)


def _turn_pair(payload: dict[str, Any]) -> dict[str, str]:
    """Extract (user, assistant) content from the payload.

    Accepts either `turn_pair` (simple pair) or `turn_group` (tool-using,
    §5.2). For a turn group we pair the initial user turn with the final
    assistant turn; intermediate tool_call / tool_result turns are preserved
    in `payload` for provenance but are not fed into gate/extract.
    """
    tp = payload.get("turn_pair") or payload.get("turn_group") or payload
    u = tp.get("user")
    a = tp.get("assistant")
    return {
        "user": u.get("content", "") if isinstance(u, dict) else str(u or ""),
        "assistant": a.get("content", "") if isinstance(a, dict) else str(a or ""),
    }


def _turn_time(payload: dict[str, Any]) -> str | None:
    """Event time for this turn pair, from the per-turn `timestamp`.

    The API accepts a `timestamp` on each turn, but the worker previously
    ignored it and stamped memories with wall-clock ingest time — so facts from
    a 2023 conversation ingested in 2026 looked like 2026 events. Prefer the
    user turn's timestamp, then the assistant's; None when neither is set.
    """
    tp = payload.get("turn_pair") or payload.get("turn_group") or payload
    for role in ("user", "assistant"):
        turn = tp.get(role) if isinstance(tp, dict) else None
        if isinstance(turn, dict):
            ts = turn.get("timestamp")
            if isinstance(ts, str) and ts.strip():
                return ts.strip()
    return None


def _event_date_str(event_time: str | None) -> str:
    """YYYY-MM-DD for the episode filename, from the event time when available.

    Falls back to wall-clock date when no usable timestamp is provided.
    """
    if event_time:
        try:
            return datetime.fromisoformat(event_time).strftime("%Y-%m-%d")
        except ValueError:
            if len(event_time) >= 10 and event_time[4] == "-" and event_time[7] == "-":
                return event_time[:10]
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _call_gate(
    ctx: IngestContext, turn_pair: dict[str, str], *, session_summary: str | None
) -> dict:
    prompt = prompts.render("gate_write", turn_pair=turn_pair, session_summary=session_summary)
    result = ctx.core.complete(
        system_prompt=prompt,
        user_prompt="Respond with a JSON object matching the schema.",
    )
    if not isinstance(result.output, dict) or "store" not in result.output:
        raise CoreModelError(f"gate returned invalid output: {result.raw_text[:200]}")
    return result.output


def _call_extract(
    ctx: IngestContext,
    turn_pair: dict[str, str],
    *,
    session_context: str | None,
    turn_timestamp: str | None = None,
) -> dict:
    """Extract S-R-O triplets, resolved text, and the event date.

    `turn_timestamp` is what lets the model turn "last Saturday" into a real
    date. Without it the extractor cannot resolve relative time at all, and
    memories end up carrying the speaking date as if it were the event date.
    """
    prompt = prompts.render(
        "extract",
        turn_pair=turn_pair,
        session_context=session_context,
        turn_timestamp=turn_timestamp,
    )
    result = ctx.core.complete(
        system_prompt=prompt,
        user_prompt="Respond with a JSON object matching the schema.",
    )
    out = result.output
    if not isinstance(out, dict):
        raise CoreModelError(f"extract returned non-object: {result.raw_text[:200]}")
    out.setdefault("resolved_text", turn_pair["assistant"])
    out.setdefault("triplets", [])
    out.setdefault("l0_abstract", turn_pair["assistant"][:200])
    out["event_date"] = _clean_event_date(out.get("event_date"))
    return out


_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _clean_event_date(value: Any) -> str | None:
    """Accept only a well-formed YYYY-MM-DD string; reject anything else.

    The model is asked for null when it cannot determine a date, but it may
    still return "unknown", an empty string, or a full timestamp.
    """
    if not isinstance(value, str):
        return None
    text = value.strip()[:10]
    if not _ISO_DATE.match(text):
        return None
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return None
    return text


def _resolve_entities(
    ctx: IngestContext, extraction: dict[str, Any]
) -> list[tuple[str, str, str | None]]:
    """Return [(slug, display_name, matched_uri_or_None)] for each distinct entity.

    `matched_uri` is set when the linker decided the mention matches an existing
    ENTITY node. Otherwise a new node is created downstream.
    """
    triplets = extraction.get("triplets", [])
    l0 = extraction.get("l0_abstract") or ""

    seen: set[str] = set()
    items: list[tuple[str, str]] = []
    for trip in triplets:
        for key in ("subject", "object"):
            raw = trip.get(key)
            if not raw or not isinstance(raw, str):
                continue
            slug = slugify(raw, separator="-", lowercase=True)
            if not slug or slug in seen:
                continue
            seen.add(slug)
            items.append((slug, raw))

    records: list[tuple[str, str, str | None]] = []
    for slug, display_name in items:
        try:
            link = entity_resolve(
                neo4j=ctx.neo4j,
                embed=ctx.embed,
                core=ctx.core,
                entity_name=display_name,
                surrounding_sentence=extraction.get("resolved_text", "")[:500],
                incoming_abstract=l0,
            )
            metrics_mod.core_model_calls.labels(
                task="entity_link", provider=ctx.cfg.core_model.provider
            ).inc()
            matched_uri = link.matched_uri if link else None
        except Exception:
            log.warning(
                "entity linker failed for %r; defaulting to new entity",
                display_name, exc_info=True,
            )
            matched_uri = None
        records.append((slug, display_name, matched_uri))
    return records


def _write_episode(
    ctx: IngestContext,
    event_id: str,
    session_id: str | None,
    extraction: dict[str, Any],
    event_time: str | None = None,
    event_date: str | None = None,
) ->tuple[str, str]:
    date_str = _event_date_str(event_time)
    summary_slug = slugify(extraction["l0_abstract"], separator="-", lowercase=True)[:60]
    filename = f"{date_str}_{summary_slug or 'episode'}.md"
    uri = f"mem://user/episodes/{filename}"
    fm = {
        "id": str(uuid.uuid4()),
        "node_type": "DOCUMENT",
        "status": "ACTIVE",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_session_id": session_id,
        "schema_version": 1,
        "provenance": {
            "extractor": "core_model_v1",
            "confidence": 0.9,
            "ingest_event_id": event_id,
        },
    }
    # Separate WHEN IT HAPPENED from WHEN IT WAS SAID. `valid_from` is the
    # extracted event date when the model could determine one, because a memory
    # asked "when did X happen?" must not answer with the date of the
    # conversation. `asserted_at` keeps the speaking time. `created_at` remains
    # the ingest time.
    if event_time or event_date:
        fm["temporal"] = {
            "valid_from": event_date or event_time,
            "valid_until": None,
        }
        if event_time:
            fm["temporal"]["asserted_at"] = event_time
    body = f"{extraction['l0_abstract']}\n\n{extraction['resolved_text']}\n"
    mf = frontmatter.MemoryFile(frontmatter=fm, body=body)
    path = ctx.fs.write_atomic(uri, mf.serialize())
    return uri, str(path)


def _write_entity(
    ctx: IngestContext,
    event_id: str,
    session_id: str | None,
    slug: str,
    display_name: str,
    l0_abstract: str,
    event_time: str | None = None,
    event_date: str | None = None,
) ->str:
    filename = f"{slug}.md"
    uri = f"mem://user/entities/{slug}/{filename}"
    if ctx.fs.exists(uri):
        return uri  # idempotent — existing entity node is authoritative
    fm = {
        "id": str(uuid.uuid4()),
        "node_type": "ENTITY",
        "status": "ACTIVE",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_session_id": session_id,
        "schema_version": 1,
        "normalize": {"canonical_name": display_name, "aliases": [display_name]},
        "provenance": {
            "extractor": "core_model_v1",
            "confidence": 0.9,
            "ingest_event_id": event_id,
        },
    }
    # First-mention event time from the turn timestamp (see _write_episode).
    if event_time:
        fm["temporal"] = {"valid_from": event_time, "valid_until": None}
    body = f"{display_name} (entity).\n\n{l0_abstract}\n"
    mf = frontmatter.MemoryFile(frontmatter=fm, body=body)
    ctx.fs.write_atomic(uri, mf.serialize())
    return uri


def _record_linked_entities(
    ctx: IngestContext,
    event_id: str,
    triplets: list[dict[str, Any]],
    entity_records: list[tuple[str, str, str]],
    *,
    tenant_id: str = DEFAULT_TENANT_ID,
) -> None:
    """Persist linked_entities rows (§16.1.4) so replay can find resolved IDs."""
    slug_to_uri = {slug: uri for slug, _, uri in entity_records}
    rows: list[tuple[str, str, int, str | None, str | None]] = []
    for idx, trip in enumerate(triplets):
        s_slug = slugify(trip.get("subject", ""), separator="-", lowercase=True)
        o_slug = slugify(trip.get("object", ""), separator="-", lowercase=True)
        rows.append(
            (event_id, tenant_id, idx, slug_to_uri.get(s_slug), slug_to_uri.get(o_slug))
        )
    if not rows:
        return
    with ctx.sqlite.transaction() as conn:
        conn.executemany(
            "INSERT OR REPLACE INTO linked_entities "
            "(event_id, tenant_id, triplet_idx, subject_node_id, object_node_id) "
            "VALUES (?, ?, ?, ?, ?)",
            rows,
        )


def _index_neo4j(
    ctx: IngestContext,
    event_id: str,
    episode_uri: str,
    extraction: dict[str, Any],
    entity_records: list[tuple[str, str, str]],
) -> None:
    """Merge episode + entity nodes and apply conflict-resolved RELATES_TO edges."""
    now = datetime.now(timezone.utc).isoformat()
    # Episode node
    episode_emb = ctx.embed.embed(extraction["l0_abstract"])
    ctx.neo4j.merge_node(
        source_uri=episode_uri,
        parent_uri="mem://user/episodes",
        properties={
            "id": str(uuid.uuid4()),
            "node_type": "DOCUMENT",
            "status": "ACTIVE",
            "l0_abstract": extraction["l0_abstract"],
            "l0_embedding": episode_emb,
            "retrieval_weight": 1.0,
            "created_at": now,
            "last_accessed_at": now,
            "access_count": 0,
            "schema_version": 1,
            "provenance_ingest_event_id": event_id,
        },
    )
    # Entity nodes
    slug_to_uri: dict[str, str] = {}
    for slug, display_name, ent_uri in entity_records:
        if slug not in slug_to_uri:
            emb = ctx.embed.embed(display_name)
            ctx.neo4j.merge_node(
                source_uri=ent_uri,
                parent_uri=f"mem://user/entities/{slug}",
                properties={
                    "id": str(uuid.uuid4()),
                    "node_type": "ENTITY",
                    "status": "ACTIVE",
                    "l0_abstract": f"{display_name} — mentioned in episode.",
                    "l0_embedding": emb,
                    "retrieval_weight": 1.0,
                    "created_at": now,
                    "last_accessed_at": now,
                    "access_count": 0,
                    "schema_version": 1,
                },
            )
            slug_to_uri[slug] = ent_uri
    # Semantic edges — run the conflict classifier per triplet
    for idx, trip in enumerate(extraction.get("triplets", [])):
        s_raw = trip.get("subject")
        o_raw = trip.get("object")
        rel = trip.get("relation")
        conf = float(trip.get("confidence", 0.0))
        if not (s_raw and o_raw and rel) or conf < 0.3:
            continue
        s_slug = slugify(s_raw, separator="-", lowercase=True)
        o_slug = slugify(o_raw, separator="-", lowercase=True)
        s_uri = slug_to_uri.get(s_slug)
        o_uri = slug_to_uri.get(o_slug)
        if not (s_uri and o_uri):
            continue
        if conf < 0.6:
            _write_low_confidence_fact(
                ctx=ctx,
                event_id=event_id,
                triplet_idx=idx,
                triplet=trip,
                subject_uri=s_uri,
                object_uri=o_uri,
                confidence=conf,
                now=now,
            )
            continue
        decision = classify(
            neo4j=ctx.neo4j,
            embed=ctx.embed,
            subject_uri=s_uri,
            relation_label=str(rel),
            object_uri=o_uri,
            object_abstract=str(o_raw),
            core=ctx.core,
            incoming_confidence=conf,
        )
        apply_decision(
            neo4j=ctx.neo4j,
            decision=decision,
            subject_uri=s_uri,
            object_uri=o_uri,
            relation_label=str(rel),
            properties={
                "confidence": conf,
                "created_at": now,
                "ingest_event_id": event_id,
            },
        )
        # Cross-reference from episode to subject entity (REFERENCES edge)
        ctx.neo4j.merge_edge(
            subject_uri=episode_uri,
            object_uri=s_uri,
            relation_label="mentions",
            edge_type="REFERENCES",
            properties={"created_at": now, "ingest_event_id": event_id},
        )


def _write_low_confidence_fact(
    *,
    ctx: IngestContext,
    event_id: str,
    triplet_idx: int,
    triplet: dict[str, Any],
    subject_uri: str,
    object_uri: str,
    confidence: float,
    now: str,
) -> str:
    """Persist uncertain triplets as LOW_CONFIDENCE FACT nodes."""
    event = ctx.sqlite.get_event(event_id) or {}
    session_id = event.get("session_id")
    rel = str(triplet.get("relation") or "related_to")
    subject = str(triplet.get("subject") or "")
    obj = str(triplet.get("object") or "")
    rel_slug = slugify(rel, separator="-", lowercase=True)[:40] or "fact"
    obj_slug = slugify(obj, separator="-", lowercase=True)[:40] or "object"
    uri = f"mem://user/facts/{event_id}/{triplet_idx}_{rel_slug}_{obj_slug}.md"
    sentence = f"{subject} {rel} {obj}".strip()
    if not ctx.fs.exists(uri):
        fm = {
            "id": str(uuid.uuid4()),
            "node_type": "FACT",
            "status": "LOW_CONFIDENCE",
            "created_at": now,
            "source_session_id": session_id,
            "schema_version": 1,
            "provenance": {
                "extractor": "core_model_v1",
                "confidence": confidence,
                "ingest_event_id": event_id,
            },
        }
        body = f"{sentence}\n\nConfidence: {confidence:.2f}\n"
        ctx.fs.write_atomic(uri, frontmatter.MemoryFile(frontmatter=fm, body=body).serialize())

    abstract = f"Low-confidence fact: {sentence}"
    ctx.neo4j.merge_node(
        source_uri=uri,
        parent_uri=uri_mod.parent_uri(uri),
        properties={
            "id": str(uuid.uuid4()),
            "node_type": "FACT",
            "status": "LOW_CONFIDENCE",
            "l0_abstract": abstract,
            "l0_embedding": ctx.embed.embed(abstract),
            "retrieval_weight": 1.0,
            "created_at": now,
            "last_accessed_at": now,
            "access_count": 0,
            "schema_version": 1,
            "confidence": confidence,
            "source_session_id": session_id,
            "provenance_ingest_event_id": event_id,
        },
    )
    ctx.neo4j.merge_edge(
        subject_uri=uri,
        object_uri=subject_uri,
        relation_label="subject",
        edge_type="REFERENCES",
        properties={"created_at": now, "ingest_event_id": event_id, "confidence": confidence},
    )
    ctx.neo4j.merge_edge(
        subject_uri=uri,
        object_uri=object_uri,
        relation_label="object",
        edge_type="REFERENCES",
        properties={"created_at": now, "ingest_event_id": event_id, "confidence": confidence},
    )
    return uri


def _validate_written_frontmatter(
    ctx: IngestContext,
    episode_uri: str,
    entity_records: list[tuple[str, str, str]],
) -> None:
    """Run §6.3 reserved-key validation on every file just written."""
    uris = [episode_uri, *{uri for _, _, uri in entity_records}]
    for uri in uris:
        if not ctx.fs.exists(uri):
            continue
        raw = ctx.fs.read(uri)
        mf = frontmatter.parse(raw)
        frontmatter.validate_required_keys(mf.frontmatter)
        frontmatter.validate_metadata(mf.frontmatter)


def _enqueue_consolidation(
    ctx: IngestContext,
    episode_uri: str,
    entity_uris: list[str],
) -> None:
    """Enqueue overview + manifest + propagate for each touched parent directory.

    The unique index on (node_id, task_type) WHERE status IN (PENDING, PROCESSING)
    deduplicates repeated writes — combined with the worker draining PENDING
    tasks, this implements the §7.5 debounce naturally.
    """
    touched: set[str] = set()
    for uri in [episode_uri, *entity_uris]:
        parent = uri_mod.parent_uri(uri)
        if parent is not None:
            touched.add(parent)
    tid = current_tenant_id()
    # Overview/manifest work is debounced (§7.5): each write pushes the rebuild
    # deadline back, so a bulk ingest produces one rebuild per directory once
    # the writes stop instead of one per turn pair.
    overview_delay = ctx.cfg.consolidation.overview_debounce_seconds
    manifest_delay = ctx.cfg.consolidation.manifest_update_delay_seconds
    for dir_uri in touched:
        ctx.sqlite.enqueue_task(node_id=dir_uri, task_type="CONSOLIDATE_OVERVIEW",
                                priority=5, tenant_id=tid,
                                delay_seconds=overview_delay)
        ctx.sqlite.enqueue_task(node_id=dir_uri, task_type="REGENERATE_MANIFEST",
                                priority=5, tenant_id=tid,
                                delay_seconds=manifest_delay)
        ctx.sqlite.enqueue_task(node_id=dir_uri, task_type="PROPAGATE_OVERVIEW",
                                priority=7, tenant_id=tid,
                                delay_seconds=overview_delay)
