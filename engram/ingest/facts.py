"""Stable identity helpers for immutable extracted assertions."""

from __future__ import annotations

from typing import Any

from slugify import slugify


def fact_uri(event_id: str, triplet_idx: int, triplet: dict[str, Any]) -> str:
    """Return the replay-stable URI for one atomized extraction claim."""
    relation = str(triplet.get("relation_canonical") or triplet.get("relation") or "related_to")
    obj = str(triplet.get("object") or "")
    relation_slug = slugify(relation, separator="-", lowercase=True)[:40] or "fact"
    object_slug = slugify(obj, separator="-", lowercase=True)[:40] or "object"
    return f"mem://user/facts/{event_id}/{triplet_idx}_{relation_slug}_{object_slug}.md"


def fact_sentence(triplet: dict[str, Any]) -> str:
    """Render the concise assertion text embedded and presented to retrieval."""
    subject = str(triplet.get("subject") or "")
    relation = str(triplet.get("relation_canonical") or triplet.get("relation") or "related_to")
    obj = str(triplet.get("object") or "")
    return f"{subject} {relation} {obj}".strip()
