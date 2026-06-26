"""BGE-Small-EN-v1.5 embedding service (§1.4).

Used for the KG vector index (384-dim) and L0 memory-hit fallback. Loaded
once per process, reused across ingest and retrieval.

Wraps an optional distributed `EmbeddingCache` so repeated embed calls on
the same text hit Redis instead of re-encoding. The cache key includes
the model tag so upgrading BGE doesn't serve stale vectors.
"""

from __future__ import annotations

import threading
from typing import Any

from engram.config import GatingConfig


class EmbeddingService:
    _instance: EmbeddingService | None = None
    _lock = threading.Lock()

    def __init__(self, cfg: GatingConfig, cache: Any | None = None) -> None:
        from sentence_transformers import SentenceTransformer

        self.cfg = cfg
        self.model = SentenceTransformer(cfg.embedding_model_path, device=cfg.device)
        get_dim = getattr(
            self.model,
            "get_embedding_dimension",
            self.model.get_sentence_embedding_dimension,
        )
        self.dim = int(get_dim())
        if self.dim != 384:
            raise RuntimeError(
                f"expected 384-dim embeddings, got {self.dim}; update Neo4j vector index"
            )
        self._cache = cache
        self._model_tag = cfg.embedding_model_path.replace("/", "_")

    @classmethod
    def get(cls, cfg: GatingConfig, *, cache: Any | None = None) -> EmbeddingService:
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls(cfg, cache=cache)
            elif cache is not None and cls._instance._cache is None:
                # Late-bound cache attach (e.g. Redis came up after embed service).
                cls._instance._cache = cache
            return cls._instance

    def embed(self, text: str) -> list[float]:
        if self._cache is not None:
            cached = self._cache.get(text, model_tag=self._model_tag)
            if cached is not None:
                return cached
        vec = self.model.encode(text, normalize_embeddings=True).tolist()
        if self._cache is not None:
            self._cache.set(text, vec, model_tag=self._model_tag)
        return vec

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if self._cache is None:
            arr = self.model.encode(
                texts,
                batch_size=self.cfg.max_batch_size,
                normalize_embeddings=True,
                show_progress_bar=False,
            )
            return arr.tolist()

        # Cache-aware batch: split into hit / miss.
        results: list[list[float] | None] = [None] * len(texts)
        miss_idx: list[int] = []
        miss_text: list[str] = []
        for i, t in enumerate(texts):
            cached = self._cache.get(t, model_tag=self._model_tag)
            if cached is not None:
                results[i] = cached
            else:
                miss_idx.append(i)
                miss_text.append(t)
        if miss_text:
            arr = self.model.encode(
                miss_text,
                batch_size=self.cfg.max_batch_size,
                normalize_embeddings=True,
                show_progress_bar=False,
            ).tolist()
            for idx, t, vec in zip(miss_idx, miss_text, arr, strict=True):
                results[idx] = vec
                self._cache.set(t, vec, model_tag=self._model_tag)
        return [r for r in results if r is not None]
