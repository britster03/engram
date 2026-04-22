"""Embedding + overview cache unit tests (MemoryCache backend)."""

from __future__ import annotations

from engram.cache import EmbeddingCache, MemoryCache, OverviewCache


def test_memory_cache_roundtrip():
    c = MemoryCache()
    c.set("k", b"v", ttl_seconds=60)
    assert c.get("k") == b"v"
    c.delete("k")
    assert c.get("k") is None


def test_embedding_cache_encodes_and_decodes():
    ec = EmbeddingCache(MemoryCache())
    vec = [0.1, -0.25, 1.5, 0.0] * 96   # 384 dims
    ec.set("hello world", vec, model_tag="bge-small-v1.5")
    out = ec.get("hello world", model_tag="bge-small-v1.5")
    assert out is not None
    assert len(out) == len(vec)
    for a, b in zip(vec, out, strict=False):
        assert abs(a - b) < 1e-5


def test_embedding_cache_miss_on_different_text():
    ec = EmbeddingCache(MemoryCache())
    ec.set("alice", [1.0] * 384, model_tag="bge")
    assert ec.get("bob", model_tag="bge") is None
    assert ec.get("alice", model_tag="different-tag") is None


def test_overview_cache_tenant_isolation():
    oc = OverviewCache(MemoryCache())
    oc.set("tenant-a", "mem://user/entities/alice/", "tenant A view")
    oc.set("tenant-b", "mem://user/entities/alice/", "tenant B view")
    assert oc.get("tenant-a", "mem://user/entities/alice/") == "tenant A view"
    assert oc.get("tenant-b", "mem://user/entities/alice/") == "tenant B view"
    oc.invalidate("tenant-a", "mem://user/entities/alice/")
    assert oc.get("tenant-a", "mem://user/entities/alice/") is None
    assert oc.get("tenant-b", "mem://user/entities/alice/") == "tenant B view"
