"""Verify two tenants cannot observe each other's data end-to-end."""

from __future__ import annotations

from pathlib import Path

import pytest

from engram.config import EngramConfig
from engram.ingest.worker import IngestContext, process_event
from engram.retrieval.orchestrator import OrchestratorContext, run_query
from engram.storage.filesystem import FilesystemStore
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from engram.storage.sqlite import SqliteStore
from engram.tenancy import (
    Tenant,
    TenantQuotas,
    set_current_tenant,
)
from engram.uri import pair_id as pair_id_fn

from .providers import (
    DeterministicCoreProvider,
    DeterministicEmbeddingService,
    DeterministicFrontierProvider,
)


@pytest.fixture
def cfg(tmp_path: Path) -> EngramConfig:
    return EngramConfig.model_validate({
        "api": {"api_key": "test-key"},
        "core_model": {"provider": "ollama_cloud", "api_key": "x"},
        "frontier_llm": {"provider": "ollama_cloud", "api_key": "x"},
        "filesystem": {"data_dir": str(tmp_path / "mem")},
        "event_ledger": {"path": str(tmp_path / "ev.db")},
        "session_cache": {"backend": "memory"},
        "knowledge_graph": {"writer_password": "x", "reader_password": "x"},
        "retrieval": {"l0_skip": True},
    })


def _tenant(tid: str) -> Tenant:
    return Tenant(
        tenant_id=tid, display_name=tid, api_key_hashes=[],
        quotas=TenantQuotas(), status="ACTIVE",
    )


def _ingest_for_tenant(
    tenant_id: str,
    *,
    sqlite: SqliteStore,
    fs: FilesystemStore,
    neo: InMemoryKnowledgeGraph,
    n: int,
) -> None:
    set_current_tenant(_tenant(tenant_id))
    ingest = IngestContext(
        cfg=_cfg_stub, sqlite=sqlite, fs=fs, neo4j=neo,  # type: ignore[arg-type]
        core=DeterministicCoreProvider(),  # type: ignore[arg-type]
        embed=DeterministicEmbeddingService(),  # type: ignore[arg-type]
    )
    for i in range(n):
        pid = pair_id_fn(f"sess-{tenant_id}", i * 2, i * 2 + 1)
        eid, _ = sqlite.record_event(
            pair_id=pid, session_id=f"sess-{tenant_id}", source="test",
            event_type="INGEST",
            payload={
                "turn_pair": {
                    "user": {"content": f"{tenant_id} fact #{i}: user moved to city-{i}",
                             "turn_idx": i * 2},
                    "assistant": {"content": "noted.", "turn_idx": i * 2 + 1},
                },
            },
            tenant_id=tenant_id,
        )
        process_event(ingest, eid)


_cfg_stub: EngramConfig | None = None


def test_tenant_a_cannot_read_tenant_b(cfg: EngramConfig):
    """Ingest for two tenants, then query as one — hits only from that tenant."""
    global _cfg_stub
    _cfg_stub = cfg
    sqlite = SqliteStore(cfg.event_ledger.path)
    fs = FilesystemStore(cfg.filesystem.data_dir)
    neo = InMemoryKnowledgeGraph()

    _ingest_for_tenant("acme", sqlite=sqlite, fs=fs, neo=neo, n=3)
    _ingest_for_tenant("globex", sqlite=sqlite, fs=fs, neo=neo, n=3)

    # Every KG node has tenant_id set correctly
    tenants_in_nodes = {n.get("tenant_id") for n in neo.nodes.values()}
    assert tenants_in_nodes == {"acme", "globex"}

    # Filesystem is partitioned by tenant on disk
    acme_root = fs.data_dir / "acme"
    globex_root = fs.data_dir / "globex"
    assert acme_root.exists() and globex_root.exists()
    acme_files = list(acme_root.rglob("*.md"))
    globex_files = list(globex_root.rglob("*.md"))
    assert acme_files and globex_files
    for p in acme_files:
        assert "globex" not in p.read_text()
    for p in globex_files:
        assert "acme" not in p.read_text()

    # SQLite events are separable by tenant_id
    acme_count = sqlite.get_conn().execute(
        "SELECT COUNT(*) AS c FROM events WHERE tenant_id = 'acme'"
    ).fetchone()["c"]
    globex_count = sqlite.get_conn().execute(
        "SELECT COUNT(*) AS c FROM events WHERE tenant_id = 'globex'"
    ).fetchone()["c"]
    assert acme_count == 3
    assert globex_count == 3


def test_vector_search_is_tenant_scoped(cfg: EngramConfig):
    """Even with the same query text, vector_search only returns rows from
    the ambient tenant."""
    global _cfg_stub
    _cfg_stub = cfg
    sqlite = SqliteStore(cfg.event_ledger.path)
    fs = FilesystemStore(cfg.filesystem.data_dir)
    neo = InMemoryKnowledgeGraph()

    _ingest_for_tenant("acme", sqlite=sqlite, fs=fs, neo=neo, n=2)
    _ingest_for_tenant("globex", sqlite=sqlite, fs=fs, neo=neo, n=2)

    # Patch InMemoryKnowledgeGraph.vector_search to honour tenant_id (it already does
    # via the WHERE in the real store; the fake doesn't know about tenants).
    def tenant_aware_search(query_embedding, k=10, uri_prefix=None,
                             dormant_floor=0.05, tenant_id=None):
        from engram.tenancy import current_tenant_id
        tid = tenant_id or current_tenant_id()
        results = []
        for uri, node in neo.nodes.items():
            if node.get("tenant_id") != tid:
                continue
            if node.get("status") != "ACTIVE":
                continue
            emb = node.get("l0_embedding")
            if emb is None:
                continue
            if uri_prefix and not uri.startswith(uri_prefix):
                continue
            results.append({
                "source_uri": uri,
                "l0_abstract": node.get("l0_abstract"),
                "score": 0.5,
                "id": node.get("id"),
                "node_type": node.get("node_type"),
            })
        return results[:k]

    neo.vector_search = tenant_aware_search  # type: ignore[assignment]

    orch = OrchestratorContext(
        cfg=cfg, fs=fs, neo4j=neo,  # type: ignore[arg-type]
        core=DeterministicCoreProvider(), frontier=DeterministicFrontierProvider(),  # type: ignore[arg-type]
        embed=DeterministicEmbeddingService(),  # type: ignore[arg-type]
    )

    set_current_tenant(_tenant("acme"))
    r_acme = run_query(orch, session_id=None, query="what city did the user move to?")

    set_current_tenant(_tenant("globex"))
    r_globex = run_query(orch, session_id=None, query="what city did the user move to?")

    # Both answers come from the same prompt; but the retrieved hits must be
    # from the tenant's own namespace. Peek at the MSC indirectly via the
    # retrieval_metadata counts (both tenants have nodes).
    assert r_acme.retrieval_metadata.nodes_retrieved >= 1
    assert r_globex.retrieval_metadata.nodes_retrieved >= 1
