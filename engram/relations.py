"""Controlled relation-label vocabulary (§6.4.2).

Loads `prompts/relations.yaml` at import time and exposes two functions:

  - `canonicalise(label, embed)` → canonical form if one is known, else None.
  - `aliases()` and `canonical_labels()` for introspection.

Normalisation proceeds in this order:
  1. Lowercase + whitespace-normalise the incoming label.
  2. Look up in the `aliases` map (deterministic).
  3. Compute cosine similarity against pre-computed canonical-label
     embeddings; return the nearest if ≥ 0.85.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Protocol

import yaml

_YAML_PATH = Path(__file__).parent / "prompts" / "relations.yaml"
_CANONICAL_MIN_COS = 0.85


class _EmbedderLike(Protocol):
    def embed(self, text: str) -> list[float]: ...


class RelationVocabulary:
    def __init__(self) -> None:
        data = yaml.safe_load(_YAML_PATH.read_text(encoding="utf-8"))
        self.canonical: list[str] = list(data.get("relations") or [])
        self.aliases: dict[str, str] = {
            k.lower(): v for k, v in (data.get("aliases") or {}).items()
        }
        self._embeds: dict[str, list[float]] | None = None
        self._lock = threading.Lock()

    def canonicalise(self, label: str, embed: _EmbedderLike) -> str | None:
        if not label:
            return None
        norm = " ".join(label.strip().split()).lower()
        if norm in self.aliases:
            return self.aliases[norm]
        if norm in {c.lower() for c in self.canonical}:
            return next(c for c in self.canonical if c.lower() == norm)
        return self._nearest_canonical(label, embed)

    def _nearest_canonical(self, label: str, embed: _EmbedderLike) -> str | None:
        with self._lock:
            if self._embeds is None:
                self._embeds = {c: embed.embed(c) for c in self.canonical}
        query_vec = embed.embed(label)
        best: tuple[str, float] | None = None
        for canon, vec in self._embeds.items():
            score = _cosine(query_vec, vec)
            if best is None or score > best[1]:
                best = (canon, score)
        if best and best[1] >= _CANONICAL_MIN_COS:
            return best[0]
        return None


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    num = sum(x * y for x, y in zip(a, b, strict=True))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(x * x for x in b) ** 0.5
    if na == 0 or nb == 0:
        return 0.0
    return num / (na * nb)


_vocab: RelationVocabulary | None = None


def vocabulary() -> RelationVocabulary:
    global _vocab
    if _vocab is None:
        _vocab = RelationVocabulary()
    return _vocab
