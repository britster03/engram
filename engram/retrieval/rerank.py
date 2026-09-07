"""Cross-encoder reranking of retrieved memories (§4.4 precision pass).

Vector search scores a query and a memory *independently* — each is embedded
once, and similarity is cosine distance between those fixed vectors. That is
cheap enough to scan the whole store, but it cannot weigh the two texts against
each other, so a memory that merely shares vocabulary with the question ranks
alongside one that actually answers it.

A cross-encoder reads the query and the memory *together* and scores the pair
directly. It is far too slow to run over every memory, which is why it runs as
a second pass over the candidates vector search already shortlisted.

Two failure modes in the LoCoMo baseline motivate this:
  - 603 answers cited a retrieved-but-wrong memory (the right one was present
    but not ranked first), and
  - wrong answers carried MORE context than correct ones (20.0 vs 18.2 nodes),
    so passing everything found actively dilutes the signal.

Reranking addresses both: score precisely, then pass fewer, better memories.

Degradation is deliberate and silent: if the model cannot be loaded (offline,
missing dependency) the original ordering is returned untouched, so retrieval
keeps working exactly as it did before.
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


class Reranker:
    """Re-scores (query, memory) pairs with a cross-encoder.

    Loading is lazy so importing this module never pulls a model into memory,
    and a load failure disables reranking rather than breaking retrieval.
    """

    def __init__(
        self,
        model_name: str = "cross-encoder/ms-marco-MiniLM-L-6-v2",
        device: str = "cpu",
        *,
        max_length: int = 512,
    ) -> None:
        self.model_name = model_name
        self.device = device
        self.max_length = max_length
        self._model: Any | None = None
        self._unavailable = False

    def _load(self) -> Any | None:
        if self._model is not None or self._unavailable:
            return self._model
        try:
            from sentence_transformers import CrossEncoder

            self._model = CrossEncoder(
                self.model_name, device=self.device, max_length=self.max_length
            )
            log.info("reranker loaded: %s on %s", self.model_name, self.device)
        except Exception as err:
            # Offline, missing package, or bad model name — retrieval must still
            # work, just without the precision pass.
            log.warning("reranker unavailable (%s); passing hits through", err)
            self._unavailable = True
        return self._model

    @staticmethod
    def _text_for(hit: dict[str, Any]) -> str:
        """Best available text to judge a hit by.

        Prefers the richest field present: a loaded overview, then the L0
        abstract, falling back to the URI so a hit is never scored on an empty
        string (which the model would rank arbitrarily).
        """
        for key in ("overview", "l0_abstract", "body"):
            value = hit.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()[:2000]
        return str(hit.get("source_uri", ""))

    def rerank(
        self,
        query: str,
        hits: list[dict[str, Any]],
        *,
        top_k: int,
    ) -> list[dict[str, Any]]:
        """Return the `top_k` hits most relevant to `query`, best first.

        Hits keep their original fields; `rerank_score` is added so traces can
        show why an item survived. Returns the input untouched (trimmed to
        `top_k`) when the model is unavailable or there is nothing to gain.
        """
        if not hits or len(hits) <= 1:
            return hits
        model = self._load()
        if model is None:
            return hits[:top_k]

        pairs = [(query, self._text_for(h)) for h in hits]
        try:
            scores = model.predict(pairs, show_progress_bar=False)
        except Exception as err:
            log.warning("reranking failed (%s); keeping original order", err)
            return hits[:top_k]

        for hit, score in zip(hits, scores):
            hit["rerank_score"] = float(score)
        ordered = sorted(hits, key=lambda h: h.get("rerank_score", 0.0), reverse=True)
        return ordered[:top_k]


class NullReranker:
    """No-op used when reranking is disabled in config."""

    def rerank(
        self, query: str, hits: list[dict[str, Any]], *, top_k: int
    ) -> list[dict[str, Any]]:
        return hits[:top_k]
