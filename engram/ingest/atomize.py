"""Deterministic extraction atomization shared by ingest and maintenance."""

from __future__ import annotations

import re
from typing import Any

_CONJUNCTION_TOKEN = re.compile(r"\s+(?:and|&)\s+", re.IGNORECASE)

# Only relations whose objects are naturally multi-valued are safe to split.
# Structural/ownership relations are deliberately absent: phrases such as
# "necklace with a cross and a heart" and "black and white flower design"
# describe one object and were observed being corrupted by the old global
# `and` splitter during the LoCoMo audit.
_MULTI_VALUE_RELATIONS = frozenset(
    {
        "avoids",
        "contains",
        "dislikes",
        "interested_in",
        "likes",
        "mentions",
        "prefers",
        "reads",
        "uses",
        "watches",
    }
)

_COMPOUND_ACTIVITY_PREFIXES = (
    "building ",
    "creating ",
    "helping ",
    "learning ",
    "making ",
    "promoting ",
    "supporting ",
    "working ",
)


def atomize_triplets(triplets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Split compound objects into independently indexable claims.

    Each output retains the original metadata and receives an ``atomized_from``
    marker when a split occurs. Subjects are deliberately not split because a
    coordinated subject is often a named group; extraction should emit separate
    subject claims when that distinction matters.
    """
    atomic: list[dict[str, Any]] = []
    for triplet in triplets:
        # A committed/replayed extraction has already been atomized. Re-splitting
        # it would change triplet indexes and therefore immutable FACT identities.
        if triplet.get("atomized_from"):
            atomic.append(dict(triplet))
            continue
        obj = str(triplet.get("object") or "")
        relation = str(
            triplet.get("relation_canonical") or triplet.get("relation") or ""
        ).casefold()
        lowered = obj.casefold().strip()
        if (
            relation not in _MULTI_VALUE_RELATIONS
            or not _CONJUNCTION_TOKEN.search(obj)
            or " with " in lowered
            or lowered.startswith(_COMPOUND_ACTIVITY_PREFIXES)
        ):
            atomic.append(dict(triplet))
            continue

        # A comma is treated as a list separator only when the same object also
        # contains an explicit conjunction. This atomizes Oxford-style lists
        # while preserving ordinary values such as "Portland, Oregon".
        pieces: list[str] = []
        for conjunct in _CONJUNCTION_TOKEN.split(obj):
            pieces.extend(conjunct.split(",") if "," in conjunct else [conjunct])
        pieces = [piece.strip(" ,.;:") for piece in pieces]
        pieces = [piece for piece in pieces if piece]
        if len(pieces) <= 1:
            atomic.append(dict(triplet))
            continue
        for piece in pieces:
            atomic.append(
                {
                    **triplet,
                    "object": piece,
                    "atomized_from": obj,
                }
            )
    return atomic
