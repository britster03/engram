"""Ingest worker — write-path pipeline (§5.3, §5.4).

Implements the full seven-step pipeline:
  1. Event recording (API thread, sync SQLite insert).
  2. Write-path gate (Core Model).
  3. S-R-O extraction + L0 abstract (Core Model).
  4. Entity linking with disambiguation (Core Model + embeddings).
  5. Filesystem write (authoritative; atomic via temp+rename+fsync).
  6. Dedup / conflict resolution + KG index update.
  7. Generation-aware REFRESH_DIRECTORY consolidation intent.

Each committed stage and nondeterministic output is persisted independently
of worker-facing event status, so recovery resumes after the last durable
boundary instead of rerunning gate, extraction, or entity linking.
"""

from __future__ import annotations

import hashlib
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from slugify import slugify

from engram import frontmatter, prompts, tracing
from engram import metrics as metrics_mod
from engram import uri as uri_mod
from engram.config import EngramConfig
from engram.ingest.atomize import atomize_triplets
from engram.ingest.conflict import apply_decision, classify
from engram.ingest.entity_linker import resolve as entity_resolve
from engram.models.core import CoreModelError, CoreModelProvider
from engram.models.embeddings import EmbeddingService
from engram.models.semantic import ExtractOutput, GateWriteOutput, complete_validated
from engram.storage.filesystem import FilesystemStore
from engram.storage.graph_projection import project_memory_node
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
    stage_hook: Callable[[str], None] | None = None


_STAGE_ORDER = {
    "RECEIVED": 0,
    "GATED": 1,
    "EXTRACTED": 2,
    "LINKED": 3,
    "FILESYSTEM_COMMITTED": 4,
    "KG_COMMITTED": 5,
    "CONSOLIDATION_COMMITTED": 6,
    "COMPLETE": 7,
    "GATED_SKIP": 7,
}


def process_event(ctx: IngestContext, event_id: str) -> str:
    """Drive one event through the pipeline. Returns the final status string."""
    event = ctx.sqlite.get_event(event_id)
    if event is None:
        raise RuntimeError(f"unknown event_id: {event_id}")

    # Pin the ambient tenant context for every downstream call.
    event_tenant = event.get("tenant_id") or DEFAULT_TENANT_ID
    set_current_tenant(
        Tenant(tenant_id=event_tenant, display_name=event_tenant,
               api_key_hashes=[], quotas=TenantQuotas(), status="ACTIVE")
    )
    stage_state = ctx.sqlite.get_event_stage(event_id, tenant_id=event_tenant)
    stage = str(stage_state["completed_stage"])
    if stage in {"COMPLETE", "GATED_SKIP"}:
        terminal = "GATED_SKIP" if stage == "GATED_SKIP" else "COMPLETE"
        if event["status"] != terminal:
            ctx.sqlite.set_event_status(event_id, terminal, tenant_id=event_tenant)
        return terminal

    payload = event["payload"]
    turn_pair = _turn_pair(payload)

    # --- Step 2: Write-path gate -----------------------------------------
    if not _stage_at_least(stage, "GATED"):
        with _Timed("gate"), tracing.span("ingest.gate", event_id=event_id):
            if payload.get("force_store") is True:
                gate = {"store": True, "reason": "authenticated force_store override"}
            else:
                try:
                    gate = _call_gate(
                        ctx, turn_pair, session_summary=payload.get("session_summary")
                    )
                except CoreModelError as err:
                    ctx.sqlite.set_event_status(event_id, "FAILED", error_message=str(err))
                    raise
                metrics_mod.core_model_calls.labels(
                    task="gate_write", provider=ctx.cfg.core_model.provider
                ).inc()
        ctx.sqlite.advance_event_stage(
            event_id, "GATED", tenant_id=event_tenant, gate_output=gate
        )
        stage = "GATED"
        _after_stage(ctx, stage)
    else:
        gate = stage_state.get("gate_output") or {"store": True, "reason": "legacy replay"}
    if not gate.get("store", False):
        ctx.sqlite.advance_event_stage(event_id, "GATED_SKIP", tenant_id=event_tenant)
        ctx.sqlite.set_event_status(
            event_id,
            "GATED_SKIP",
            error_message=str(gate.get("reason") or ""),
            tenant_id=event_tenant,
        )
        _after_stage(ctx, "GATED_SKIP")
        return "GATED_SKIP"
    ctx.sqlite.set_event_status(event_id, "GATED_STORE", tenant_id=event_tenant)

    # --- Step 3: S-R-O extraction + L0 abstract --------------------------
    existing = ctx.sqlite.get_extraction(event_id)
    extraction: dict[str, Any]
    if not _stage_at_least(stage, "EXTRACTED"):
        with _Timed("extract"), tracing.span("ingest.extract", event_id=event_id):
            if existing is None:
                extraction = _call_extract(
                    ctx, turn_pair, session_context=payload.get("session_context")
                )
                metrics_mod.core_model_calls.labels(
                    task="extract", provider=ctx.cfg.core_model.provider
                ).inc()
            else:
                extraction = existing
            extraction = _prepare_extraction(ctx, extraction, payload)
            ctx.sqlite.save_extraction(
                event_id=event_id,
                resolved_text=extraction["resolved_text"],
                triplets=extraction["triplets"],
                l0_abstract=extraction["l0_abstract"],
                tenant_id=event_tenant,
            )
        ctx.sqlite.advance_event_stage(event_id, "EXTRACTED", tenant_id=event_tenant)
        stage = "EXTRACTED"
        _after_stage(ctx, stage)
    else:
        if existing is None:
            raise RuntimeError(f"{event_id} is EXTRACTED but has no extraction record")
        extraction = existing

    # --- Step 4: Entity linking with disambiguation ----------------------
    if not _stage_at_least(stage, "LINKED"):
        with _Timed("entity_link"), tracing.span("ingest.entity_link", event_id=event_id):
            entities = _resolve_entities(ctx, extraction)
        link_output = [
            {"slug": slug, "display_name": display_name, "matched_uri": matched_uri}
            for slug, display_name, matched_uri in entities
        ]
        ctx.sqlite.advance_event_stage(
            event_id, "LINKED", tenant_id=event_tenant, link_output=link_output
        )
        stage = "LINKED"
        _after_stage(ctx, stage)
    else:
        stored_link_output = stage_state.get("link_output")
        entities = _restore_entity_links(
            ctx,
            event_id,
            extraction,
            tenant_id=event_tenant,
            stored=stored_link_output if isinstance(stored_link_output, list) else None,
        )

    entity_records = _materialized_entity_records(entities)
    episode_uri = f"mem://user/episodes/{event_id}.md"
    fact_uris: dict[int, str] = {}

    # --- Step 5: Filesystem write (authoritative) ------------------------
    if not _stage_at_least(stage, "FILESYSTEM_COMMITTED"):
        with _Timed("fs_write"), tracing.span("ingest.fs_write", event_id=event_id):
            episode_uri, _ = _write_episode(ctx, event, extraction)
            entity_records = []
            for slug, display_name, matched_uri in entities:
                ent_uri = matched_uri or _write_entity(
                    ctx, event_id, event["session_id"], slug, display_name, payload
                )
                entity_records.append((slug, display_name, ent_uri))
            fact_uris = _write_fact_files(
                ctx, event_id, extraction, entity_records, created_at=str(event["created_at"])
            )
            _record_artifacts(
                ctx,
                event_id,
                [episode_uri, *[uri for _, _, uri in entity_records], *fact_uris.values()],
                tenant_id=event_tenant,
            )
        ctx.sqlite.fs_outbox_write(event_id, episode_uri, tenant_id=event_tenant)
        _record_linked_entities(
            ctx, event_id, extraction["triplets"], entity_records, tenant_id=event_tenant,
        )
        ctx.sqlite.advance_event_stage(
            event_id, "FILESYSTEM_COMMITTED", tenant_id=event_tenant
        )
        stage = "FILESYSTEM_COMMITTED"
        _after_stage(ctx, stage)
    else:
        fact_uris = _fact_uris_for(extraction, event_id)

    # --- Step 6: Validate metadata, then dedup/conflict + KG index ------
    if not _stage_at_least(stage, "KG_COMMITTED"):
        with _Timed("kg_index"), tracing.span("ingest.kg_index", event_id=event_id):
            try:
                _validate_written_frontmatter(
                    ctx, episode_uri, entity_records, list(fact_uris.values())
                )
                _index_neo4j(
                    ctx,
                    event_id,
                    episode_uri,
                    extraction,
                    entity_records,
                    payload,
                    fact_uris=fact_uris,
                )
            except Exception as err:
                log.exception("KG index failed for event %s", event_id)
                ctx.sqlite.fs_outbox_mark(event_id, "INDEX_FAILED", error=str(err))
                ctx.sqlite.mark_event_artifacts_kg(
                    event_id, "FAILED", error=str(err)[:500]
                )
                raise
        ctx.sqlite.mark_event_artifacts_kg(event_id, "COMMITTED")
        ctx.sqlite.fs_outbox_mark(event_id, "INDEXED")
        ctx.sqlite.advance_event_stage(event_id, "KG_COMMITTED", tenant_id=event_tenant)
        ctx.sqlite.set_event_status(event_id, "INDEXED", tenant_id=event_tenant)
        stage = "KG_COMMITTED"
        _after_stage(ctx, stage)

    # --- Step 7: Consolidation enqueue -----------------------------------
    if not _stage_at_least(stage, "CONSOLIDATION_COMMITTED"):
        with _Timed("consolidation_enqueue"), tracing.span(
            "ingest.consolidation_enqueue", event_id=event_id,
        ):
            _enqueue_consolidation(
                ctx,
                episode_uri,
                [*[uri for _, _, uri in entity_records], *fact_uris.values()],
            )
        ctx.sqlite.advance_event_stage(
            event_id, "CONSOLIDATION_COMMITTED", tenant_id=event_tenant
        )
        stage = "CONSOLIDATION_COMMITTED"
        _after_stage(ctx, stage)
    ctx.sqlite.advance_event_stage(event_id, "COMPLETE", tenant_id=event_tenant)
    ctx.sqlite.set_event_status(event_id, "COMPLETE", tenant_id=event_tenant)
    _after_stage(ctx, "COMPLETE")
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


def _stage_at_least(current: str, expected: str) -> bool:
    try:
        return _STAGE_ORDER[current] >= _STAGE_ORDER[expected]
    except KeyError as err:
        raise RuntimeError(f"unknown ingest stage: {err.args[0]}") from err


def _after_stage(ctx: IngestContext, stage: str) -> None:
    """Run the optional crash-injection hook after a durable stage commit."""
    if ctx.stage_hook is not None:
        ctx.stage_hook(stage)


def _prepare_extraction(
    ctx: IngestContext,
    extraction: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Validate and canonicalise deterministic extraction fields before indexing."""
    from engram.relations import vocabulary

    prepared = dict(extraction)
    prepared["resolved_text"] = str(prepared.get("resolved_text") or "")
    prepared["l0_abstract"] = str(prepared.get("l0_abstract") or "")[:500]
    raw_triplets = prepared.get("triplets")
    if not isinstance(raw_triplets, list):
        raise CoreModelError("extract triplets must be a list")
    asserted_at = _source_asserted_at(payload)
    normalized: list[dict[str, Any]] = []
    vocab = vocabulary()
    candidate_triplets = atomize_triplets(
        [dict(raw) for raw in raw_triplets if isinstance(raw, dict)]
    )
    for raw in candidate_triplets:
        subject = str(raw.get("subject") or "").strip()
        relation = str(raw.get("relation") or "").strip()
        obj = str(raw.get("object") or "").strip()
        try:
            confidence = max(0.0, min(1.0, float(raw.get("confidence", 0.0))))
        except (TypeError, ValueError):
            confidence = 0.0
        if not (subject and relation and obj) or confidence < 0.3:
            continue
        trip = dict(raw)
        object_kind = str(raw.get("object_kind") or "").upper()
        object_norm = obj.casefold().strip()
        relation_norm = relation.casefold().strip()
        relation_tail = relation_norm.removeprefix("feels_")
        if (
            object_norm in {"true", "false", "yes", "no", "none", "null", "unknown"}
            or (relation_norm.startswith("feels_") and object_norm == relation_tail)
        ):
            object_kind = "LITERAL"
        elif object_kind not in {"ENTITY", "LITERAL"}:
            object_kind = "ENTITY"
        trip.update(
            {
                "subject": subject,
                "relation": relation,
                "object": obj,
                "object_kind": object_kind,
                "confidence": confidence,
            }
        )
        canonical = vocab.canonicalise(relation, ctx.embed)
        if canonical:
            if canonical != relation:
                trip["relation_original"] = relation
            trip["relation"] = canonical
            trip["relation_normalized"] = True
            trip.pop("relation_review_required", None)
        else:
            trip["relation_normalized"] = False
            trip["relation_review_required"] = True
        if asserted_at:
            temporal_value = trip.get("temporal")
            temporal: dict[str, Any] = (
                dict(temporal_value) if isinstance(temporal_value, dict) else {}
            )
            trip["temporal"] = {**temporal, "asserted_at": asserted_at}
        normalized.append(trip)
    prepared["triplets"] = normalized
    return prepared


def _source_asserted_at(payload: dict[str, Any]) -> str | None:
    pair = payload.get("turn_pair") or payload.get("turn_group") or payload
    if not isinstance(pair, dict):
        return None
    for role in ("assistant", "user"):
        turn = pair.get(role)
        if isinstance(turn, dict) and turn.get("timestamp"):
            return str(turn["timestamp"])
    return None


def _restore_entity_links(
    ctx: IngestContext,
    event_id: str,
    extraction: dict[str, Any],
    *,
    tenant_id: str,
    stored: list[dict[str, Any]] | None,
) -> list[tuple[str, str, str | None]]:
    """Restore committed linker decisions without invoking the model again."""
    if stored is not None:
        return [
            (
                str(row.get("slug") or ""),
                str(row.get("display_name") or ""),
                str(row["matched_uri"]) if row.get("matched_uri") else None,
            )
            for row in stored
            if row.get("slug") and row.get("display_name")
        ]

    rows = ctx.sqlite.get_conn().execute(
        "SELECT triplet_idx, subject_node_id, object_node_id FROM linked_entities "
        "WHERE event_id = ? AND tenant_id = ? ORDER BY triplet_idx",
        (event_id, tenant_id),
    ).fetchall()
    by_idx = {int(row["triplet_idx"]): row for row in rows}
    restored: list[tuple[str, str, str | None]] = []
    seen: set[str] = set()
    for idx, trip in enumerate(extraction.get("triplets", [])):
        linked = by_idx.get(idx)
        for key, uri_key in (
            ("subject", "subject_node_id"),
            ("object", "object_node_id"),
        ):
            display_name = str(trip.get(key) or "").strip()
            slug = slugify(display_name, separator="-", lowercase=True)
            if not slug or slug in seen:
                continue
            seen.add(slug)
            matched_uri = str(linked[uri_key]) if linked is not None and linked[uri_key] else None
            restored.append((slug, display_name, matched_uri))
    if not restored and extraction.get("triplets"):
        raise RuntimeError(f"{event_id} is LINKED but has no persisted linker output")
    return restored


def _entity_uri(slug: str) -> str:
    return f"mem://user/entities/{slug}/{slug}.md"


def _materialized_entity_records(
    entities: list[tuple[str, str, str | None]],
) -> list[tuple[str, str, str]]:
    return [
        (slug, display_name, matched_uri or _entity_uri(slug))
        for slug, display_name, matched_uri in entities
    ]


def _record_artifacts(
    ctx: IngestContext,
    event_id: str,
    uris: list[str],
    *,
    tenant_id: str,
) -> None:
    """Register every required memory file using its authoritative identity."""
    event = ctx.sqlite.get_event(event_id, tenant_id=tenant_id) or {}
    event_provenance = _source_provenance(event.get("payload") or {})
    for uri in dict.fromkeys(uris):
        memory = frontmatter.parse(ctx.fs.read(uri))
        fm = memory.frontmatter
        provenance_value = fm.get("provenance")
        provenance: dict[str, Any] = (
            dict(provenance_value) if isinstance(provenance_value, dict) else {}
        )
        ctx.sqlite.upsert_ingest_artifact(
            event_id=event_id,
            tenant_id=tenant_id,
            artifact_type=str(fm["node_type"]),
            source_uri=uri,
            artifact_id=str(fm["id"]),
            content_hash=str(fm.get("content_hash") or _content_hash(memory.body)),
            source_session_id=str(event["session_id"]) if event.get("session_id") else None,
            source_turn_ids=event_provenance["source_turn_ids"],
            confidence=(
                float(provenance["confidence"])
                if provenance.get("confidence") is not None
                else None
            ),
            extractor_version=(
                str(provenance["extractor"])
                if provenance.get("extractor") is not None
                else None
            ),
        )


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


def _call_gate(
    ctx: IngestContext, turn_pair: dict[str, str], *, session_summary: str | None
) -> dict:
    prompt = prompts.render("gate_write", turn_pair=turn_pair, session_summary=session_summary)
    validated, _result = complete_validated(
        ctx.core,
        task="gate_write",
        schema=GateWriteOutput,
        system_prompt=prompt,
        user_prompt="Respond with a JSON object matching the schema.",
    )
    return validated.model_dump(mode="json")


def _call_extract(
    ctx: IngestContext, turn_pair: dict[str, str], *, session_context: str | None
) -> dict:
    prompt = prompts.render("extract", turn_pair=turn_pair, session_context=session_context)
    validated, _result = complete_validated(
        ctx.core,
        task="extract",
        schema=ExtractOutput,
        system_prompt=prompt,
        user_prompt="Respond with a JSON object matching the schema.",
    )
    return validated.model_dump(mode="json")


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
        keys = ["subject"]
        if trip.get("object_kind") != "LITERAL":
            keys.append("object")
        for key in keys:
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
    event: dict[str, Any],
    extraction: dict[str, Any],
) -> tuple[str, str]:
    event_id = str(event["event_id"])
    uri = f"mem://user/episodes/{event_id}.md"
    if ctx.fs.exists(uri):
        return uri, str(ctx.fs.path_for(uri))
    payload = event.get("payload") or {}
    provenance = _source_provenance(payload)
    created_at = str(event.get("created_at") or datetime.now(timezone.utc).isoformat())
    body = f"{extraction['l0_abstract']}\n\n{extraction['resolved_text']}\n"
    fm = {
        "id": _stable_id(uri),
        "tenant_id": current_tenant_id(),
        "node_type": "DOCUMENT",
        "status": "ACTIVE",
        "created_at": created_at,
        "updated_at": created_at,
        "content_hash": _content_hash(body),
        "source_session_id": event.get("session_id"),
        "source_turn_ids": provenance["source_turn_ids"],
        "source_conversation_id": provenance["source_conversation_id"],
        "source_session_ids": provenance["source_session_ids"],
        "source_speakers": provenance["source_speakers"],
        "source_timestamps": provenance["source_timestamps"],
        "temporal": {
            "asserted_at": _source_asserted_at(payload) or created_at,
            "valid_from": None,
            "valid_until": None,
            "phrase": "source turn timestamp",
        },
        "schema_version": 1,
        "provenance": {
            "extractor": "core_model_v1",
            "confidence": 0.9,
            "ingest_event_id": event_id,
            "source_turn_ids": provenance["source_turn_ids"],
            "source_task": provenance["source_task"],
            "multimodal_turn_ids": provenance["multimodal_turn_ids"],
        },
    }
    mf = frontmatter.MemoryFile(frontmatter=fm, body=body)
    path = ctx.fs.write_atomic(uri, mf.serialize())
    return uri, str(path)


def _write_entity(
    ctx: IngestContext,
    event_id: str,
    session_id: str | None,
    slug: str,
    display_name: str,
    payload: dict[str, Any],
) -> str:
    uri = _entity_uri(slug)
    if ctx.fs.exists(uri):
        return uri  # idempotent — existing entity node is authoritative
    created_at = datetime.now(timezone.utc).isoformat()
    provenance = _source_provenance(payload)
    body = f"{display_name} (entity).\n"
    fm = {
        "id": _stable_id(uri),
        "tenant_id": current_tenant_id(),
        "node_type": "ENTITY",
        "status": "ACTIVE",
        "created_at": created_at,
        "updated_at": created_at,
        "content_hash": _content_hash(body),
        "source_session_id": session_id,
        "source_turn_ids": provenance["source_turn_ids"],
        "schema_version": 1,
        "normalize": {"canonical_name": display_name, "aliases": [display_name]},
        "provenance": {
            "extractor": "core_model_v1",
            "confidence": 0.9,
            "ingest_event_id": event_id,
            "source_turn_ids": provenance["source_turn_ids"],
        },
    }
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


def _fact_uris_for(extraction: dict[str, Any], event_id: str) -> dict[int, str]:
    result: dict[int, str] = {}
    for idx, trip in enumerate(extraction.get("triplets", [])):
        try:
            confidence = float(trip.get("confidence", 0.0))
        except (TypeError, ValueError):
            continue
        if 0.3 <= confidence < 0.6:
            result[idx] = _low_confidence_fact_uri(event_id, idx, trip)
    return result


def _write_fact_files(
    ctx: IngestContext,
    event_id: str,
    extraction: dict[str, Any],
    entity_records: list[tuple[str, str, str]],
    *,
    created_at: str,
) -> dict[int, str]:
    """Commit low-confidence FACT files before any KG mutation occurs."""
    slug_to_uri = {slug: uri for slug, _, uri in entity_records}
    result: dict[int, str] = {}
    for idx, trip in enumerate(extraction.get("triplets", [])):
        try:
            confidence = float(trip.get("confidence", 0.0))
        except (TypeError, ValueError):
            continue
        if not 0.3 <= confidence < 0.6:
            continue
        subject_slug = slugify(str(trip.get("subject") or ""), separator="-", lowercase=True)
        object_slug = slugify(str(trip.get("object") or ""), separator="-", lowercase=True)
        subject_uri = slug_to_uri.get(subject_slug)
        object_uri = slug_to_uri.get(object_slug)
        if not subject_uri or not object_uri:
            continue
        result[idx] = _write_low_confidence_fact_file(
            ctx=ctx,
            event_id=event_id,
            triplet_idx=idx,
            triplet=trip,
            subject_uri=subject_uri,
            object_uri=object_uri,
            confidence=confidence,
            created_at=created_at,
        )
    return result


def _index_neo4j(
    ctx: IngestContext,
    event_id: str,
    episode_uri: str,
    extraction: dict[str, Any],
    entity_records: list[tuple[str, str, str]],
    payload: dict[str, Any],
    *,
    fact_uris: dict[int, str],
) -> None:
    """Merge episode + entity nodes and apply conflict-resolved RELATES_TO edges."""
    # Episode node
    source = _source_provenance(payload)
    episode_memory = frontmatter.parse(ctx.fs.read(episode_uri))
    episode_fm = episode_memory.frontmatter
    now = str(episode_fm["created_at"])
    episode_emb = ctx.embed.embed(extraction["l0_abstract"])
    ctx.neo4j.merge_node(
        source_uri=episode_uri,
        parent_uri="mem://user/episodes",
        properties=project_memory_node(
            episode_memory,
            l0_abstract=extraction["l0_abstract"],
            l0_embedding=episode_emb,
        ),
    )
    # Entity nodes
    slug_to_uri: dict[str, str] = {}
    for slug, display_name, ent_uri in entity_records:
        if slug not in slug_to_uri:
            emb = ctx.embed.embed(display_name)
            entity_memory = frontmatter.parse(ctx.fs.read(ent_uri))
            ctx.neo4j.merge_node(
                source_uri=ent_uri,
                parent_uri=f"mem://user/entities/{slug}",
                properties=project_memory_node(
                    entity_memory,
                    l0_abstract=display_name,
                    l0_embedding=emb,
                ),
            )
            slug_to_uri[slug] = ent_uri
    # Semantic edges — run the conflict classifier per triplet
    for idx, trip in enumerate(extraction.get("triplets", [])):
        s_raw = trip.get("subject")
        o_raw = trip.get("object")
        rel = trip.get("relation_canonical") or trip.get("relation")
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
            fact_uri = fact_uris.get(idx)
            if fact_uri is None:
                raise RuntimeError(f"missing committed FACT artifact for triplet {idx}")
            _write_low_confidence_fact(
                ctx=ctx,
                event_id=event_id,
                triplet_idx=idx,
                triplet=trip,
                subject_uri=s_uri,
                object_uri=o_uri,
                confidence=conf,
                now=now,
                expected_uri=fact_uri,
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
                "source_turn_ids": source["source_turn_ids"],
            },
        )
        # Cross-reference from episode to subject entity (REFERENCES edge)
        ctx.neo4j.merge_edge(
            subject_uri=episode_uri,
            object_uri=s_uri,
            relation_label="mentions",
            edge_type="REFERENCES",
            properties={
                "created_at": now,
                "ingest_event_id": event_id,
                "source_turn_ids": source["source_turn_ids"],
            },
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
    expected_uri: str,
) -> str:
    """Index a previously committed LOW_CONFIDENCE FACT artifact."""
    event = ctx.sqlite.get_event(event_id) or {}
    source = _source_provenance(event.get("payload") or {})
    rel = str(triplet.get("relation") or "related_to")
    subject = str(triplet.get("subject") or "")
    obj = str(triplet.get("object") or "")
    uri = _low_confidence_fact_uri(event_id, triplet_idx, triplet)
    if uri != expected_uri or not ctx.fs.exists(uri):
        raise RuntimeError(f"FACT artifact missing or changed for triplet {triplet_idx}")
    sentence = f"{subject} {rel} {obj}".strip()

    fact_memory = frontmatter.parse(ctx.fs.read(uri))
    abstract = f"Low-confidence fact: {sentence}"
    ctx.neo4j.merge_node(
        source_uri=uri,
        parent_uri=uri_mod.parent_uri(uri),
        properties=project_memory_node(
            fact_memory,
            l0_abstract=abstract,
            l0_embedding=ctx.embed.embed(abstract),
        ),
    )
    ctx.neo4j.merge_edge(
        subject_uri=uri,
        object_uri=subject_uri,
        relation_label="subject",
        edge_type="REFERENCES",
        properties={
            "created_at": now,
            "ingest_event_id": event_id,
            "confidence": confidence,
            "source_turn_ids": source["source_turn_ids"],
        },
    )
    ctx.neo4j.merge_edge(
        subject_uri=uri,
        object_uri=object_uri,
        relation_label="object",
        edge_type="REFERENCES",
        properties={
            "created_at": now,
            "ingest_event_id": event_id,
            "confidence": confidence,
            "source_turn_ids": source["source_turn_ids"],
        },
    )
    return uri


def _low_confidence_fact_uri(
    event_id: str,
    triplet_idx: int,
    triplet: dict[str, Any],
) -> str:
    rel = str(triplet.get("relation") or "related_to")
    obj = str(triplet.get("object") or "")
    rel_slug = slugify(rel, separator="-", lowercase=True)[:40] or "fact"
    obj_slug = slugify(obj, separator="-", lowercase=True)[:40] or "object"
    return f"mem://user/facts/{event_id}/{triplet_idx}_{rel_slug}_{obj_slug}.md"


def _write_low_confidence_fact_file(
    *,
    ctx: IngestContext,
    event_id: str,
    triplet_idx: int,
    triplet: dict[str, Any],
    subject_uri: str,
    object_uri: str,
    confidence: float,
    created_at: str,
) -> str:
    uri = _low_confidence_fact_uri(event_id, triplet_idx, triplet)
    if ctx.fs.exists(uri):
        return uri
    event = ctx.sqlite.get_event(event_id) or {}
    session_id = event.get("session_id")
    source = _source_provenance(event.get("payload") or {})
    rel = str(triplet.get("relation") or "related_to")
    subject = str(triplet.get("subject") or "")
    obj = str(triplet.get("object") or "")
    sentence = f"{subject} {rel} {obj}".strip()
    body = f"{sentence}\n\nConfidence: {confidence:.2f}\n"
    fm = {
        "id": _stable_id(uri),
        "tenant_id": current_tenant_id(),
        "node_type": "FACT",
        "status": "LOW_CONFIDENCE",
        "created_at": created_at,
        "updated_at": created_at,
        "content_hash": _content_hash(body),
        "source_session_id": session_id,
        "source_turn_ids": source["source_turn_ids"],
        "schema_version": 1,
        "fact": {
            "subject": subject,
            "relation": rel,
            "object": obj,
            "subject_uri": subject_uri,
            "object_uri": object_uri,
        },
        "temporal": triplet.get("temporal") or {
            "asserted_at": created_at,
            "valid_from": None,
            "valid_until": None,
            "phrase": sentence,
        },
        "provenance": {
            "extractor": "core_model_v1",
            "confidence": confidence,
            "ingest_event_id": event_id,
            "source_turn_ids": source["source_turn_ids"],
        },
    }
    ctx.fs.write_atomic(uri, frontmatter.MemoryFile(frontmatter=fm, body=body).serialize())
    return uri


def _stable_id(uri: str) -> str:
    """Return a replay-stable UUID for a tenant-scoped memory URI."""
    value = f"{current_tenant_id()}:{uri}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, value))


def _content_hash(body: str) -> str:
    return frontmatter.content_hash(body)


def _source_provenance(payload: dict[str, Any]) -> dict[str, Any]:
    pair = payload.get("turn_pair") or payload.get("turn_group") or payload
    turns = [pair.get("user"), pair.get("assistant")]
    records = [turn for turn in turns if isinstance(turn, dict)]

    def unique(field: str) -> list[str]:
        return list(dict.fromkeys(str(row[field]) for row in records if row.get(field)))

    source_turn_ids = unique("external_id")
    conversation_ids = unique("source_conversation_id")
    return {
        "source_turn_ids": source_turn_ids,
        "source_conversation_id": conversation_ids[0] if conversation_ids else None,
        "source_session_ids": unique("source_session_id"),
        "source_speakers": unique("speaker"),
        "source_timestamps": unique("timestamp"),
        "source_task": next(
            (str(row["source_task"]) for row in records if row.get("source_task")),
            None,
        ),
        "multimodal_turn_ids": [
            str(row["external_id"])
            for row in records
            if row.get("external_id") and (row.get("image_caption") or row.get("image_urls"))
        ],
    }


def _validate_written_frontmatter(
    ctx: IngestContext,
    episode_uri: str,
    entity_records: list[tuple[str, str, str]],
    fact_uris: list[str],
) -> None:
    """Run §6.3 reserved-key validation on every file just written."""
    uris = [episode_uri, *{uri for _, _, uri in entity_records}, *fact_uris]
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
    """Coalesce every touched directory into one generation-aware refresh."""
    touched: set[str] = set()
    for uri in [episode_uri, *entity_uris]:
        parent = uri_mod.parent_uri(uri)
        if parent is not None:
            touched.add(parent)
    tid = current_tenant_id()
    for dir_uri in touched:
        ctx.sqlite.enqueue_directory_refresh(
            node_id=dir_uri,
            priority=5,
            tenant_id=tid,
            debounce_seconds=ctx.cfg.consolidation.overview_debounce_seconds,
            child_signature=_directory_child_signature(ctx.fs, dir_uri),
        )


def _directory_child_signature(fs: FilesystemStore, dir_uri: str) -> str:
    """Hash non-generated direct children for stable refresh generations."""
    digest = hashlib.sha256()
    for child_uri in sorted(fs.list_children(dir_uri)):
        digest.update(child_uri.encode("utf-8"))
        path = fs.path_for(child_uri)
        if path.is_file():
            digest.update(hashlib.sha256(path.read_bytes()).digest())
        else:
            digest.update(b"directory")
    return digest.hexdigest()
