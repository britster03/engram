"""Entity linking (§5.4.4, §5.7).

Given an extracted entity name, decide whether it matches an existing ENTITY
node or is new. Uses the safer defaults from §5.7:
  - cosine ≥ 0.92 AND name-token-overlap ≥ 0.5 → proceed to disambiguation
  - below threshold → treat as new entity
  - Core Model disambiguation always runs even for a single candidate
  - Uncertainty defaults to new-entity creation; wrong merges are recoverable
    via `/memories/{id}/unmerge`
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from engram import prompts
from engram.models.core import CoreModelProvider
from engram.models.embeddings import EmbeddingService
from engram.storage.neo4j_store import Neo4jStore

log = logging.getLogger(__name__)


CANDIDATE_COS = 0.92
NAME_TOKEN_OVERLAP = 0.5
MAX_CANDIDATES = 5


@dataclass
class LinkResult:
    matched_uri: str | None
    confidence: float
    reason: str


def resolve(
    *,
    neo4j: Neo4jStore,
    embed: EmbeddingService,
    core: CoreModelProvider,
    entity_name: str,
    surrounding_sentence: str,
    incoming_abstract: str,
) -> LinkResult:
    emb = embed.embed(entity_name)
    try:
        candidates = neo4j.vector_search(
            emb,
            k=MAX_CANDIDATES,
            uri_prefix="mem://user/entities/",
            dormant_floor=0.0,
        )
    except Exception:  # noqa: BLE001
        candidates = []
    # Filter by token-overlap
    filtered: list[dict[str, Any]] = []
    for c in candidates:
        if float(c.get("score", 0.0)) < CANDIDATE_COS:
            continue
        other_abstract = str(c.get("l0_abstract") or "")
        overlap = _token_overlap(entity_name, other_abstract)
        if overlap >= NAME_TOKEN_OVERLAP:
            c["token_overlap"] = overlap
            filtered.append(c)
    if not filtered:
        return LinkResult(None, 0.0, "no-candidate-above-threshold")
    # Core Model disambiguates even with a single candidate
    try:
        prompt = prompts.render(
            "entity_link",
            entity_name=entity_name,
            surrounding_sentence=surrounding_sentence,
            incoming_abstract=incoming_abstract,
            candidates=[
                {
                    "source_uri": c["source_uri"],
                    "l0_abstract": c.get("l0_abstract"),
                    "cosine": c.get("score"),
                    "token_overlap": c.get("token_overlap"),
                }
                for c in filtered
            ],
        )
        result = core.complete(
            system_prompt=prompt,
            user_prompt="Return the disambiguation JSON.",
        )
        out = result.output if isinstance(result.output, dict) else {}
        matched = out.get("matched_id")
        conf = float(out.get("confidence", 0.0))
        reason = str(out.get("reason", ""))
        # Defensive validation: the model must return a string that matches
        # one of the candidate source_uris. Small models sometimes copy the
        # prompt's example shape ("mem://.../alice/") instead of a real
        # source_uri; reject anything that isn't in the candidate list so we
        # never try to read a directory as a file downstream.
        if matched and isinstance(matched, str):
            candidate_uris = {c["source_uri"] for c in filtered}
            if matched in candidate_uris:
                return LinkResult(matched, conf, reason)
            log.info(
                "entity_link rejected: matched_id %r is not in candidate list %s",
                matched, list(candidate_uris),
            )
        return LinkResult(None, conf, reason or "core-model-said-no")
    except Exception:  # noqa: BLE001
        log.warning("entity_link core call failed; defaulting to new entity", exc_info=True)
        return LinkResult(None, 0.0, "core-model-error")


def _token_overlap(a: str, b: str) -> float:
    at = {w.lower() for w in a.split() if w}
    bt = {w.lower() for w in b.split() if w}
    if not at:
        return 0.0
    return len(at & bt) / len(at)
