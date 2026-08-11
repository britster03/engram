"""Deterministic extraction atomization shared by ingest and maintenance."""

from __future__ import annotations

import re
from typing import Any

_COMPOUND_TOKEN = re.compile(r"\s+and\s+|\s+&\s+", re.IGNORECASE)


def atomize_triplets(triplets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Split compound objects into independently indexable claims.

    Each output retains the original metadata and receives an ``atomized_from``
    marker when a split occurs. Subjects are deliberately not split because a
    coordinated subject is often a named group; extraction should emit separate
    subject claims when that distinction matters.
    """
    atomic: list[dict[str, Any]] = []
    for triplet in triplets:
        obj = str(triplet.get("object") or "")
        pieces = [piece.strip(" ,.;:") for piece in _COMPOUND_TOKEN.split(obj)]
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
