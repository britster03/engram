"""Conflict resolution (§6.5).

Classifies a newly-extracted triplet against existing RELATES_TO edges from
the same subject and applies the SUPERSEDES / no-op / add-new decisions.

The default classifier is rule-based on cosine similarity (fast, zero LLM
cost). When the rule-based tier is uncertain (0.5 ≤ object cosine ≤ 0.95
against an existing edge with the same relation), the Core Model is asked
to disambiguate via `prompts/dedup.j2`. A different object is not evidence
of contradiction: relations such as likes, visited, works_at, and parent_of
are multi-valued or historical. CONTRADICTION is therefore accepted only
when the caller supplies independent explicit-correction evidence. If the
Core Model is unavailable or fails, we default to CO_EXISTENCE (safer:
co-existence can be cleaned up later; an incorrect DUPLICATE or
CONTRADICTION suppresses valid retrieval evidence).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from engram import prompts
from engram.models.core import CoreModelProvider
from engram.models.embeddings import EmbeddingService
from engram.models.semantic import ConflictOutput, complete_validated
from engram.storage.neo4j_store import Neo4jStore

log = logging.getLogger(__name__)


DUPLICATE_OBJECT_COS = 0.95
DUPLICATE_RELATION_COS = 0.90
MAX_CORE_CANDIDATES = 20


@dataclass
class ConflictDecision:
    case: str                       # "DUPLICATE" | "CONTRADICTION" | "CO_EXISTENCE"
    existing_edge_id: str | None
    reason: str
    existing_assertion_uri: str | None = None


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
    allow_contradiction: bool = False,
) -> ConflictDecision:
    """Return the appropriate conflict case for a new triplet.

    Two-tier decision:
      1. Rule-based classifier on cosine similarity (fast, zero LLM cost).
      2. If the rule-based tier lands in the ambiguous band (0.5 ≤ obj_cos
         ≤ 0.95 under the same relation), call `core` to disambiguate using
         prompts/dedup.j2. A Core Model CONTRADICTION is honored only when
         `allow_contradiction` confirms the caller observed an explicit
         correction/retraction independently of object dissimilarity.

    All matching edges are scored before deciding. Returning on the first
    dissimilar edge can hide a later exact duplicate when a subject already
    has several values for the same relation.
    """
    existing = _fetch_active_edges(neo4j, subject_uri)
    if not existing:
        return ConflictDecision("CO_EXISTENCE", None, "no-existing-edges")

    new_rel_vec = embed.embed(relation_label)
    new_obj_vec = embed.embed(object_abstract)

    matching_candidates: list[tuple[dict, float]] = []
    for edge in existing:
        rel_cos = _cos(new_rel_vec, embed.embed(edge.get("relation_label") or ""))
        same_relation = (
            edge.get("relation_label") == relation_label or rel_cos >= DUPLICATE_RELATION_COS
        )
        if not same_relation:
            continue
        object_abs = edge.get("object_abstract") or edge.get("object_uri") or ""
        obj_cos = _cos(new_obj_vec, embed.embed(object_abs))
        if edge.get("object_uri") == object_uri:
            obj_cos = 1.0
        matching_candidates.append((edge, obj_cos))

    if not matching_candidates:
        return ConflictDecision("CO_EXISTENCE", None, "no-matching-relation")

    matching_candidates.sort(key=lambda item: item[1], reverse=True)
    best_edge, best_score = matching_candidates[0]
    if best_score > DUPLICATE_OBJECT_COS:
        return ConflictDecision(
            "DUPLICATE",
            best_edge.get("edge_id"),
            f"best object cosine {best_score:.2f} > {DUPLICATE_OBJECT_COS}",
            existing_assertion_uri=best_edge.get("assertion_uri"),
        )

    # Dissimilarity means a different value, not a contradiction. Only ask
    # the model about low-similarity candidates when the caller separately
    # observed explicit correction/retraction evidence.
    if best_score < 0.5:
        if allow_contradiction:
            return ConflictDecision(
                "CONTRADICTION",
                best_edge.get("edge_id"),
                f"explicit correction with different object (cosine {best_score:.2f})",
                existing_assertion_uri=best_edge.get("assertion_uri"),
            )
        return ConflictDecision(
            "CO_EXISTENCE",
            None,
            f"different object (best cosine {best_score:.2f}); no explicit correction",
        )

    core_candidates = (
        matching_candidates
        if allow_contradiction
        else [item for item in matching_candidates if item[1] >= 0.5]
    )[:MAX_CORE_CANDIDATES]

    # Ambiguous — ask the Core Model when available. Otherwise co-existence.
    if core is None:
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
            allow_contradiction=allow_contradiction,
            existing_edges=[
                {
                    "edge_id": e.get("edge_id"),
                    "relation_label": e.get("relation_label"),
                    "object_uri": e.get("object_uri"),
                    "object_abstract": e.get("object_abstract"),
                    "object_cosine": score,
                }
                for e, score in core_candidates
            ],
        )
        validated, _result = complete_validated(
            core,
            task="dedup",
            schema=ConflictOutput,
            system_prompt=prompt,
            user_prompt="Return the dedup JSON.",
        )
        if validated.case == "CO_EXISTENCE":
            return ConflictDecision(
                "CO_EXISTENCE", None, validated.reason or "core-model-dedup"
            )
        selected = next(
            (
                edge
                for edge, _score in core_candidates
                if str(edge.get("edge_id")) == str(validated.existing_edge_id)
            ),
            None,
        )
        if selected is None:
            return ConflictDecision(
                "CO_EXISTENCE", None, "core model selected an unknown existing edge"
            )
        if validated.case == "CONTRADICTION" and not allow_contradiction:
            return ConflictDecision(
                "CO_EXISTENCE",
                None,
                "core model proposed contradiction without explicit correction evidence",
            )
        return ConflictDecision(
            validated.case,
            selected.get("edge_id"),
            validated.reason or "core-model-dedup",
            existing_assertion_uri=(
                str(selected["assertion_uri"])
                if selected.get("assertion_uri")
                else None
            ),
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
    incoming_assertion_uri: str | None = None,
) -> None:
    """Mutate Neo4j according to the decision (§6.5)."""
    props = dict(properties or {})
    now = str(props.get("created_at") or datetime.now(timezone.utc).isoformat())
    if decision.case == "DUPLICATE":
        _touch_edge(neo4j, decision.existing_edge_id, now=now)
        if incoming_assertion_uri and decision.existing_assertion_uri:
            neo4j.merge_edge(
                subject_uri=incoming_assertion_uri,
                object_uri=decision.existing_assertion_uri,
                relation_label="duplicate_of",
                edge_type="DUPLICATE_OF",
                properties={"created_at": now},
            )
        return
    if decision.case == "CONTRADICTION" and decision.existing_edge_id:
        _supersede_edge(neo4j, decision.existing_edge_id, now=now)
    props.setdefault("status", "ACTIVE")
    props.setdefault("created_at", now)
    neo4j.merge_edge(
        subject_uri=subject_uri,
        object_uri=object_uri,
        relation_label=relation_label,
        edge_type="RELATES_TO",
        properties=props,
    )
    # History belongs to immutable assertions, never to entity identities.
    if (
        decision.case == "CONTRADICTION"
        and incoming_assertion_uri
        and decision.existing_assertion_uri
    ):
        neo4j.merge_edge(
            subject_uri=incoming_assertion_uri,
            object_uri=decision.existing_assertion_uri,
            relation_label="supersedes",
            edge_type="SUPERSEDES",
            properties={"created_at": now},
        )


def _fetch_active_edges(neo4j: Neo4jStore, subject_uri: str) -> list[dict]:
    try:
        return neo4j.run_template(
            "MATCH (s:Node {tenant_id: $tenant_id, source_uri: $uri})"
            "-[r:RELATES_TO]->(o:Node {tenant_id: $tenant_id}) "
            "WHERE r.tenant_id = $tenant_id "
            "AND r.status = 'ACTIVE' AND o.status = 'ACTIVE' "
            "RETURN elementId(r) AS edge_id, r.relation_label AS relation_label, "
            "o.source_uri AS object_uri, o.l0_abstract AS object_abstract, "
            "r.assertion_uri AS assertion_uri",
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
    """Mark the derived edge HISTORICAL and return its former object URI."""
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


def _cos(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    num = sum(x * y for x, y in zip(a, b, strict=True))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(x * x for x in b) ** 0.5
    return 0.0 if na == 0 or nb == 0 else num / (na * nb)
