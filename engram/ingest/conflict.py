"""Conflict resolution (§6.5).

Classifies a newly-extracted triplet against existing RELATES_TO edges from
the same subject and applies the SUPERSEDES / no-op / add-new decisions.

The default classifier is rule-based on cosine similarity (fast, zero LLM
cost). When the rule-based tier is uncertain (0.5 < object cosine ≤ 0.95
against an existing edge with the same relation), the Core Model is asked
to disambiguate via `prompts/dedup.j2`. If the Core Model is unavailable or
fails, we default to CO_EXISTENCE (safer: co-existence can be cleaned up by
NORMALIZE/ATOMIZE later; an incorrect DUPLICATE or CONTRADICTION is harder
to reverse).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from engram import prompts
from engram.models.core import CoreModelProvider
from engram.models.embeddings import EmbeddingService
from engram.storage.neo4j_store import Neo4jStore

log = logging.getLogger(__name__)


DUPLICATE_OBJECT_COS = 0.95
CONTRADICTION_OBJECT_COS = 0.95
DUPLICATE_RELATION_COS = 0.90


@dataclass
class ConflictDecision:
    case: str                       # "DUPLICATE" | "CONTRADICTION" | "CO_EXISTENCE"
    existing_edge_id: str | None
    reason: str


def classify(
    *,
    neo4j: Neo4jStore,
    embed: EmbeddingService,
    subject_uri: str,
    relation_label: str,
    object_uri: str,
    object_abstract: str,
    core: CoreModelProvider | None = None,
    incoming_confidence: float = 0.9,
) -> ConflictDecision:
    """Return the appropriate conflict case for a new triplet.

    Two-tier decision:
      1. Rule-based classifier on cosine similarity (fast, zero LLM cost).
      2. If the rule-based tier lands in the ambiguous band (0.5 < obj_cos
         ≤ 0.95 under the same relation), call `core` to disambiguate using
         prompts/dedup.j2. If `core` is None or the call fails, we default
         to CO_EXISTENCE (safer than a wrong DUPLICATE / CONTRADICTION).
    """
    existing = _fetch_active_edges(neo4j, subject_uri)
    if not existing:
        return ConflictDecision("CO_EXISTENCE", None, "no-existing-edges")

    new_rel_vec = embed.embed(relation_label)
    new_obj_vec = embed.embed(object_abstract)

    ambiguous_candidates: list[tuple[dict, float]] = []
    for edge in existing:
        rel_cos = _cos(new_rel_vec, embed.embed(edge.get("relation_label") or ""))
        same_relation = (
            edge.get("relation_label") == relation_label or rel_cos >= DUPLICATE_RELATION_COS
        )
        if not same_relation:
            continue
        object_abs = edge.get("object_abstract") or edge.get("object_uri") or ""
        obj_cos = _cos(new_obj_vec, embed.embed(object_abs))
        if obj_cos > DUPLICATE_OBJECT_COS:
            return ConflictDecision(
                "DUPLICATE", edge.get("edge_id"),
                f"object cosine {obj_cos:.2f} > {DUPLICATE_OBJECT_COS}",
            )
        if obj_cos < 0.5:
            return ConflictDecision(
                "CONTRADICTION", edge.get("edge_id"),
                f"object cosine {obj_cos:.2f} — different object",
            )
        # 0.5 ≤ obj_cos ≤ 0.95 — ambiguous
        ambiguous_candidates.append((edge, obj_cos))

    if not ambiguous_candidates:
        return ConflictDecision("CO_EXISTENCE", None, "no-matching-relation")

    # Ambiguous — ask the Core Model when available. Otherwise co-existence.
    if core is None:
        _best_edge, best_score = max(ambiguous_candidates, key=lambda p: p[1])
        return ConflictDecision(
            "CO_EXISTENCE", None,
            f"ambiguous cosine {best_score:.2f}; no core model available",
        )
    try:
        prompt = prompts.render(
            "dedup",
            incoming={
                "subject": subject_uri,
                "relation": relation_label,
                "object": object_uri,
                "l0_abstract": object_abstract,
                "confidence": incoming_confidence,
            },
            existing_edges=[
                {
                    "edge_id": e.get("edge_id"),
                    "relation_label": e.get("relation_label"),
                    "object_uri": e.get("object_uri"),
                    "object_abstract": e.get("object_abstract"),
                }
                for e, _ in ambiguous_candidates
            ],
        )
        result = core.complete(
            system_prompt=prompt,
            user_prompt="Return the dedup JSON.",
        )
        out = result.output if isinstance(result.output, dict) else {}
        case = str(out.get("case", "CO_EXISTENCE"))
        if case not in {"DUPLICATE", "CONTRADICTION", "CO_EXISTENCE"}:
            case = "CO_EXISTENCE"
        return ConflictDecision(
            case,
            out.get("existing_edge_id"),
            str(out.get("reason", "core-model-dedup")),
        )
    except Exception:
        log.warning("dedup core call failed; defaulting to CO_EXISTENCE", exc_info=True)
        return ConflictDecision("CO_EXISTENCE", None, "core-model-error")


def apply_decision(
    *,
    neo4j: Neo4jStore,
    decision: ConflictDecision,
    subject_uri: str,
    object_uri: str,
    relation_label: str,
    properties: dict[str, Any] | None = None,
) -> None:
    """Mutate Neo4j according to the decision (§6.5)."""
    props = dict(properties or {})
    now = str(props.get("created_at") or datetime.now(timezone.utc).isoformat())
    if decision.case == "DUPLICATE":
        _touch_edge(neo4j, decision.existing_edge_id, now=now)
        return
    if decision.case == "CONTRADICTION" and decision.existing_edge_id:
        old_object = _supersede_edge(neo4j, decision.existing_edge_id, now=now)
        # §6.5.1: mark the old object HISTORICAL if no ACTIVE edges
        # still reference it as subject or object.
        if old_object:
            _mark_orphan_historical(neo4j, old_object, now=now)
    props.setdefault("status", "ACTIVE")
    props.setdefault("created_at", now)
    neo4j.merge_edge(
        subject_uri=subject_uri,
        object_uri=object_uri,
        relation_label=relation_label,
        edge_type="RELATES_TO",
        properties=props,
    )
    # Write a SUPERSEDES edge from new → old for history navigation (§6.4)
    if decision.case == "CONTRADICTION" and decision.existing_edge_id:
        try:
            neo4j.run_template(
                "MATCH (s:Node {tenant_id: $tenant_id, source_uri: $s_uri}), "
                "(o:Node {tenant_id: $tenant_id, source_uri: $o_uri}) "
                "MERGE (s)-[e:SUPERSEDES {edge_id: $eid}]->(o) "
                "SET e.created_at = $now, e.tenant_id = $tenant_id",
                {"s_uri": subject_uri, "o_uri": object_uri,
                 "eid": decision.existing_edge_id, "now": now},
            )
        except Exception:
            log.debug("SUPERSEDES edge write failed", exc_info=True)


def _fetch_active_edges(neo4j: Neo4jStore, subject_uri: str) -> list[dict]:
    try:
        return neo4j.run_template(
            "MATCH (s:Node {tenant_id: $tenant_id, source_uri: $uri})"
            "-[r:RELATES_TO]->(o:Node {tenant_id: $tenant_id}) "
            "WHERE r.tenant_id = $tenant_id "
            "AND r.status = 'ACTIVE' AND o.status = 'ACTIVE' "
            "RETURN elementId(r) AS edge_id, r.relation_label AS relation_label, "
            "o.source_uri AS object_uri, o.l0_abstract AS object_abstract",
            {"uri": subject_uri},
            timeout_s=5,
        )
    except Exception:
        return []


def _touch_edge(neo4j: Neo4jStore, edge_id: Any, *, now: str) -> None:
    try:
        neo4j.run_template(
            "MATCH ()-[r:RELATES_TO]->() WHERE elementId(r) = $eid "
            "AND r.tenant_id = $tenant_id "
            "SET r.last_accessed_at = $now, r.access_count = coalesce(r.access_count, 0) + 1",
            {"eid": edge_id, "now": now},
        )
    except Exception:
        log.debug("touch_edge failed for %s", edge_id, exc_info=True)


def _supersede_edge(neo4j: Neo4jStore, edge_id: Any, *, now: str) -> str | None:
    """Mark the edge HISTORICAL and return its object source_uri (for orphan check)."""
    try:
        rows = neo4j.run_template(
            "MATCH (s:Node {tenant_id: $tenant_id})-[r:RELATES_TO]->"
            "(o:Node {tenant_id: $tenant_id}) WHERE elementId(r) = $eid "
            "AND r.tenant_id = $tenant_id "
            "SET r.status = 'HISTORICAL', r.superseded_at = $now "
            "RETURN o.source_uri AS object_uri",
            {"eid": edge_id, "now": now},
        )
        if rows:
            return str(rows[0].get("object_uri") or "")
        return None
    except Exception:
        log.debug("supersede_edge failed for %s", edge_id, exc_info=True)
        return None


def _mark_orphan_historical(neo4j: Neo4jStore, object_uri: str, *, now: str) -> None:
    """§6.5.1: a node with no ACTIVE relationship edges becomes HISTORICAL."""
    try:
        neo4j.run_template(
            "MATCH (n:Node {tenant_id: $tenant_id, source_uri: $uri}) "
            "OPTIONAL MATCH (n)-[r:RELATES_TO]-(:Node {tenant_id: $tenant_id}) "
            "WHERE r.tenant_id = $tenant_id AND coalesce(r.status, 'ACTIVE') = 'ACTIVE' "
            "WITH n, count(r) AS active_edges "
            "WHERE active_edges = 0 AND n.status = 'ACTIVE' "
            "SET n.status = 'HISTORICAL', n.superseded_at = $now",
            {"uri": object_uri, "now": now},
        )
    except Exception:
        log.debug("orphan check failed for %s", object_uri, exc_info=True)


def _cos(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    num = sum(x * y for x, y in zip(a, b, strict=True))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(x * x for x in b) ** 0.5
    return 0.0 if na == 0 or nb == 0 else num / (na * nb)
