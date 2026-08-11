"""Canonical filesystem-memory to graph-node projection.

Runtime ingest and disaster rebuild must derive the same stable node fields.
Operational fields are intentionally seeded from the immutable creation time;
normal retrieval can update them later without changing semantic identity.
"""

from __future__ import annotations

from typing import Any

from engram.frontmatter import MemoryFile


def memory_l0_abstract(memory: MemoryFile, *, fallback: str = "") -> str:
    fm = memory.frontmatter
    body_first = next((line.strip() for line in memory.body.splitlines() if line.strip()), "")
    node_type = str(fm.get("node_type") or "DOCUMENT")
    if node_type == "ENTITY":
        normalize = fm.get("normalize")
        if isinstance(normalize, dict) and normalize.get("canonical_name"):
            return str(normalize["canonical_name"])
    if node_type == "FACT" and fm.get("status") == "LOW_CONFIDENCE":
        return f"Low-confidence fact: {body_first or fallback}"
    return body_first or fallback


def project_memory_node(
    memory: MemoryFile,
    *,
    l0_abstract: str,
    l0_embedding: list[float],
) -> dict[str, Any]:
    """Return Neo4j-safe stable properties from a validated memory envelope."""
    fm = memory.frontmatter
    created_at = str(fm.get("created_at") or "")
    props: dict[str, Any] = {
        "id": str(fm.get("id") or ""),
        "node_type": str(fm.get("node_type") or "DOCUMENT"),
        "status": str(fm.get("status") or "ACTIVE"),
        "l0_abstract": l0_abstract,
        "l0_embedding": l0_embedding,
        "retrieval_weight": float(fm.get("retrieval_weight", 1.0)),
        "created_at": created_at,
        "updated_at": str(fm.get("updated_at") or created_at),
        "last_accessed_at": created_at,
        "access_count": 0,
        "schema_version": int(fm.get("schema_version", 1)),
        "content_hash": str(fm.get("content_hash") or ""),
    }
    for key in (
        "source_event_id",
        "source_episode_uri",
        "source_session_id",
        "source_turn_ids",
        "source_conversation_id",
        "source_session_ids",
        "source_speakers",
        "source_timestamps",
    ):
        if fm.get(key) is not None:
            props[key] = fm[key]

    provenance = fm.get("provenance")
    if isinstance(provenance, dict):
        if provenance.get("ingest_event_id") is not None:
            props["provenance_ingest_event_id"] = str(provenance["ingest_event_id"])
        if provenance.get("extractor") is not None:
            props["extractor_version"] = str(provenance["extractor"])
        if provenance.get("confidence") is not None:
            props["confidence"] = float(provenance["confidence"])

    normalize = fm.get("normalize")
    if isinstance(normalize, dict):
        if normalize.get("canonical_name") is not None:
            props["canonical_name"] = str(normalize["canonical_name"])
        if isinstance(normalize.get("aliases"), list):
            props["aliases"] = [str(value) for value in normalize["aliases"]]

    temporal = fm.get("temporal")
    if isinstance(temporal, dict):
        for key in ("asserted_at", "valid_from", "valid_until", "phrase"):
            if temporal.get(key) is not None:
                props[f"temporal_{key}"] = str(temporal[key])

    fact = fm.get("fact")
    if isinstance(fact, dict):
        for key in (
            "subject",
            "relation",
            "object",
            "object_kind",
            "subject_uri",
            "object_uri",
        ):
            if fact.get(key) is not None:
                props[f"fact_{key}"] = str(fact[key])
    return props
