"""Tenant-scoped runtime-versus-rebuild structural parity."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from engram.config import EngramConfig
from engram.frontmatter import parse
from engram.ingest.worker import IngestContext, process_event
from engram.rebuild_kg import rebuild
from engram.storage.filesystem import FilesystemStore
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from engram.storage.sqlite import SqliteStore
from engram.tenancy import Tenant, TenantQuotas, set_current_tenant
from engram.uri import pair_id as pair_id_fn

from .providers import DeterministicCoreProvider, DeterministicEmbeddingService


class LowConfidenceProvider(DeterministicCoreProvider):
    def complete(self, **kwargs):  # type: ignore[no-untyped-def,override]
        from engram.models.core import CompletionResult

        prompt = str(kwargs.get("system_prompt") or "")
        if prompt.startswith("[GATE]"):
            return CompletionResult(output={"store": True, "reason": "fact"}, raw_text="{}")
        if prompt.startswith("[EXTRACT]"):
            return CompletionResult(
                output={
                    "resolved_text": "User may move to Lisbon.",
                    "l0_abstract": "User may move to Lisbon.",
                    "triplets": [
                        {
                            "subject": "user",
                            "relation": "moved_to",
                            "object": "Lisbon",
                            "confidence": 0.45,
                        }
                    ],
                },
                raw_text="{}",
            )
        return super().complete(**kwargs)


class AssertionProvider(DeterministicCoreProvider):
    def __init__(self, obj: str, *, explicit_correction: bool = False) -> None:
        self.obj = obj
        self.explicit_correction = explicit_correction

    def complete(self, **kwargs):  # type: ignore[no-untyped-def,override]
        from engram.models.core import CompletionResult

        prompt = str(kwargs.get("system_prompt") or "")
        if prompt.startswith("[GATE]"):
            return CompletionResult(output={"store": True, "reason": "fact"}, raw_text="{}")
        if prompt.startswith("[EXTRACT]"):
            return CompletionResult(
                output={
                    "resolved_text": f"User works at {self.obj}.",
                    "l0_abstract": f"User works at {self.obj}.",
                    "triplets": [
                        {
                            "subject": "user",
                            "relation": "works_at",
                            "object": self.obj,
                            "confidence": 0.9,
                            "explicit_correction": self.explicit_correction,
                        }
                    ],
                },
                raw_text="{}",
            )
        return super().complete(**kwargs)


class AmbiguousDuplicateProvider(AssertionProvider):
    def complete(self, **kwargs):  # type: ignore[no-untyped-def,override]
        from engram.models.core import CompletionResult

        prompt = str(kwargs.get("system_prompt") or "")
        if prompt.startswith("[DEDUP]"):
            match = re.search(r"^- edge_id:\s*(\S+)", prompt, flags=re.MULTILINE)
            assert match is not None
            return CompletionResult(
                output={
                    "case": "DUPLICATE",
                    "existing_edge_id": match.group(1),
                    "reason": "semantic aliases describe the same role",
                },
                raw_text="{}",
            )
        return super().complete(**kwargs)


class AmbiguousEmbeddingService:
    """Place two object aliases in the Core-model disambiguation band."""

    dim = 2

    def embed(self, text: str) -> list[float]:
        if text == "Meta career":
            return [1.0, 0.0]
        if text == "Meta work":
            return [0.8, 0.6]
        if text == "works_at":
            return [1.0, 0.0]
        return [0.0, 1.0]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [self.embed(text) for text in texts]


@pytest.fixture
def cfg(tmp_path: Path) -> EngramConfig:
    return EngramConfig.model_validate(
        {
            "api": {"api_key": "test-key"},
            "core_model": {"provider": "ollama_cloud", "api_key": "x"},
            "frontier_llm": {"provider": "ollama_cloud", "api_key": "x"},
            "filesystem": {"data_dir": str(tmp_path / "mem")},
            "event_ledger": {"path": str(tmp_path / "events.db")},
            "session_cache": {"backend": "memory"},
            "knowledge_graph": {"backend": "memory"},
        }
    )


def _ingest(
    *,
    cfg: EngramConfig,
    sqlite: SqliteStore,
    fs: FilesystemStore,
    graph: InMemoryKnowledgeGraph,
    tenant_id: str,
    index: int,
    core: DeterministicCoreProvider,
    embed: Any | None = None,
) -> str:
    set_current_tenant(Tenant(tenant_id=tenant_id, display_name=tenant_id, quotas=TenantQuotas()))
    event_id, _ = sqlite.record_event(
        pair_id=pair_id_fn(f"session-{tenant_id}", index * 2, index * 2 + 1),
        session_id=f"session-{tenant_id}",
        source="test",
        event_type="INGEST",
        tenant_id=tenant_id,
        payload={
            "turn_pair": {
                "user": {
                    "content": "I moved to Berlin.",
                    "external_id": f"{tenant_id}:u:{index}",
                    "timestamp": "2024-01-01T00:00:00Z",
                },
                "assistant": {
                    "content": "That is a big move.",
                    "external_id": f"{tenant_id}:a:{index}",
                    "timestamp": "2024-01-01T00:00:01Z",
                },
            }
        },
    )
    ctx = IngestContext(
        cfg=cfg,
        sqlite=sqlite,
        fs=fs,
        neo4j=graph,
        core=core,
        embed=embed or DeterministicEmbeddingService(),  # type: ignore[arg-type]
    )
    assert process_event(ctx, event_id) == "COMPLETE"
    return event_id


def _snapshot(graph: InMemoryKnowledgeGraph, tenant_id: str) -> str:
    nodes = dict(graph.iter_nodes(tenant_id=tenant_id))
    edges = sorted(
        (edge for edge in graph.edges if edge.get("tenant_id") == tenant_id),
        key=lambda edge: (
            str(edge.get("type")),
            str(edge.get("source")),
            str(edge.get("target")),
            str(edge.get("relation_label")),
        ),
    )
    return json.dumps({"nodes": nodes, "edges": edges}, sort_keys=True, separators=(",", ":"))


def test_rebuild_is_scoped_and_matches_runtime_for_active_and_low_confidence(
    cfg: EngramConfig,
):
    sqlite = SqliteStore(cfg.event_ledger.path)
    fs = FilesystemStore(cfg.filesystem.data_dir)
    graph = InMemoryKnowledgeGraph()
    _ingest(
        cfg=cfg,
        sqlite=sqlite,
        fs=fs,
        graph=graph,
        tenant_id="tenant-a",
        index=0,
        core=DeterministicCoreProvider(),
    )
    _ingest(
        cfg=cfg,
        sqlite=sqlite,
        fs=fs,
        graph=graph,
        tenant_id="tenant-a",
        index=1,
        core=LowConfidenceProvider(),
    )
    _ingest(
        cfg=cfg,
        sqlite=sqlite,
        fs=fs,
        graph=graph,
        tenant_id="tenant-b",
        index=0,
        core=DeterministicCoreProvider(),
    )
    before_a = _snapshot(graph, "tenant-a")
    before_b = _snapshot(graph, "tenant-b")

    dry = rebuild(
        cfg,
        tenant_ids=["tenant-a"],
        dry_run=True,
        graph=graph,
        embed=DeterministicEmbeddingService(),
    )
    assert dry["tenants"] == ["tenant-a"]
    assert dry["nodes_planned"] > 0
    assert _snapshot(graph, "tenant-a") == before_a

    result = rebuild(
        cfg,
        tenant_ids=["tenant-a"],
        dry_run=False,
        graph=graph,
        embed=DeterministicEmbeddingService(),
    )
    assert result["failed"] == 0
    assert _snapshot(graph, "tenant-a") == before_a
    assert _snapshot(graph, "tenant-b") == before_b


def test_rebuild_refuses_implicit_or_unknown_scope(cfg: EngramConfig):
    with pytest.raises(ValueError, match="explicit tenant_ids"):
        rebuild(cfg)
    with pytest.raises(ValueError, match="unknown or empty"):
        rebuild(cfg, tenant_ids=["does-not-exist"], dry_run=True)


def test_rebuild_matches_duplicate_explicit_correction_and_history(
    cfg: EngramConfig,
):
    sqlite = SqliteStore(cfg.event_ledger.path)
    fs = FilesystemStore(cfg.filesystem.data_dir)
    graph = InMemoryKnowledgeGraph()
    for index, (employer, correction) in enumerate(
        (("Meta", False), ("Meta", False), ("Google", True))
    ):
        _ingest(
            cfg=cfg,
            sqlite=sqlite,
            fs=fs,
            graph=graph,
            tenant_id="history-tenant",
            index=index,
            core=AssertionProvider(employer, explicit_correction=correction),
        )
    before = _snapshot(graph, "history-tenant")
    assert any(
        edge.get("type") == "RELATES_TO" and edge.get("status") == "HISTORICAL"
        for edge in graph.edges
        if edge.get("tenant_id") == "history-tenant"
    )
    assert any(
        edge.get("type") == "DUPLICATE_OF"
        for edge in graph.edges
        if edge.get("tenant_id") == "history-tenant"
    )
    supersedes = [
        edge
        for edge in graph.edges
        if edge.get("tenant_id") == "history-tenant" and edge.get("type") == "SUPERSEDES"
    ]
    assert len(supersedes) == 1
    assert all("/facts/" in edge["source"] and "/facts/" in edge["target"] for edge in supersedes)
    fact_nodes = {
        uri: node
        for uri, node in graph.iter_nodes(tenant_id="history-tenant")
        if node.get("node_type") == "FACT"
    }
    assert len(fact_nodes) == 3
    assert all(node.get("status") == "ACTIVE" for node in fact_nodes.values())
    superseded_fact_uris = {edge["target"] for edge in supersedes}
    query_vector = DeterministicEmbeddingService().embed("User works at Meta.")
    active_hits = graph.vector_search(query_vector, k=100, tenant_id="history-tenant")
    assert superseded_fact_uris.isdisjoint({str(hit["source_uri"]) for hit in active_hits})
    entity_nodes = [
        node
        for _uri, node in graph.iter_nodes(tenant_id="history-tenant")
        if node.get("node_type") == "ENTITY"
    ]
    assert all(node.get("status") == "ACTIVE" for node in entity_nodes)
    rebuild(
        cfg,
        tenant_ids=["history-tenant"],
        dry_run=False,
        graph=graph,
        embed=DeterministicEmbeddingService(),
    )
    assert _snapshot(graph, "history-tenant") == before


def test_rebuild_replays_persisted_core_duplicate_decision(cfg: EngramConfig) -> None:
    sqlite = SqliteStore(cfg.event_ledger.path)
    fs = FilesystemStore(cfg.filesystem.data_dir)
    graph = InMemoryKnowledgeGraph()
    embed = AmbiguousEmbeddingService()
    for index, employer in enumerate(("Meta career", "Meta work")):
        _ingest(
            cfg=cfg,
            sqlite=sqlite,
            fs=fs,
            graph=graph,
            tenant_id="ambiguous-tenant",
            index=index,
            core=AmbiguousDuplicateProvider(employer),
            embed=embed,
        )

    before = _snapshot(graph, "ambiguous-tenant")
    duplicate_edges = [
        edge
        for edge in graph.edges
        if edge.get("tenant_id") == "ambiguous-tenant" and edge.get("type") == "DUPLICATE_OF"
    ]
    assert len(duplicate_edges) == 1
    conflict_cases = []
    for path in (Path(cfg.filesystem.data_dir) / "ambiguous-tenant").rglob("*.md"):
        memory = parse(path.read_text(encoding="utf-8"))
        if memory.frontmatter.get("node_type") == "FACT":
            conflict_cases.append(memory.frontmatter["conflict"]["case"])
    assert sorted(conflict_cases) == ["CO_EXISTENCE", "DUPLICATE"]

    rebuild(
        cfg,
        tenant_ids=["ambiguous-tenant"],
        dry_run=False,
        graph=graph,
        embed=embed,
    )
    assert _snapshot(graph, "ambiguous-tenant") == before
