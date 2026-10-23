from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest
from temporalio.exceptions import ApplicationError

from engram.storage.memory_kg import InMemoryKnowledgeGraph
from engram.storage.postgres import PostgresStore
from engram.temporal import activities
from engram.temporal.worker import parse_queue
from engram.temporal.workflows import ProjectionWorkflow


class _Cursor:
    rowcount = 1

    def fetchone(self):
        return {"dispatch_id": "dsp-1"}


class _Connection:
    def __init__(self):
        self.sql = ""
        self.params = None

    def execute(self, sql, params):
        self.sql = sql
        self.params = params
        return _Cursor()


class _StoreForActivity(PostgresStore):
    def __init__(self, dispatch):
        self.dispatch = dispatch
        self.completed: list[str] = []
        self.dead: list[tuple[str, str]] = []

    def get_dispatch(self, dispatch_id):
        return self.dispatch if dispatch_id == self.dispatch["dispatch_id"] else None

    def complete_dispatch(self, dispatch_id, **_kwargs):
        self.completed.append(dispatch_id)
        return True

    def mark_dispatch_dead(self, dispatch_id, error):
        self.dead.append((dispatch_id, error))
        return True


def test_projection_dispatch_uses_mutation_id_payload_and_revision():
    store = object.__new__(PostgresStore)
    store.projection_task_queue = "custom-projection"
    connection = _Connection()

    dispatch_id = store._enqueue_dispatch_in_tx(
        connection,
        "PROJECTION",
        "mutation-1",
        "acme",
        payload={"memory_id": "memory-1", "operation": "UPSERT"},
        aggregate_revision=7,
    )

    assert dispatch_id == "dsp-1"
    assert connection.params[2] == "mutation-1"
    assert connection.params[5] == "custom-projection"
    assert '"memory_id": "memory-1"' in connection.params[7]
    assert connection.params[8] == 7
    assert "payload" in connection.sql
    assert "aggregate_revision" in connection.sql
    assert connection.params[6] == "projection:mutation-1"


def test_projection_workflow_and_worker_queue_are_opaque_and_selectable():
    assert list(inspect.signature(ProjectionWorkflow.run).parameters) == ["self", "dispatch_id"]
    assert parse_queue([]) == "all"
    assert parse_queue(["--queue", "projection"]) == "projection"
    assert parse_queue(["--queue", "code-ingest"]) == "code-ingest"
    with pytest.raises(SystemExit):
        parse_queue(["--queue", "not-a-queue"])


def test_projection_activity_loads_canonical_mutation_and_completes_dispatch(monkeypatch):
    dispatch = {
        "dispatch_id": "dsp-1",
        "aggregate_id": "mutation-1",
        "tenant_id": "acme",
        "aggregate_revision": 3,
        "payload": {
            "aggregate_type": "MEMORY",
            "memory_id": "memory-1",
            "operation": "UPSERT",
        },
    }
    store = _StoreForActivity(dispatch)
    graph = InMemoryKnowledgeGraph()

    class Repository:
        def load_mutation_projection(self, mutation_id):
            assert mutation_id == "mutation-1"
            return {
                "tenant_id": "acme",
                "revision": 3,
                "aggregate_type": "MEMORY",
                "memory_id": "memory-1",
                "properties": {"canonical_name": "Alice"},
            }

    state = SimpleNamespace(control_plane=store, canonical_repository=Repository(), neo4j=graph)
    monkeypatch.setattr(activities, "get_state", lambda: state)

    assert activities.project_neo4j_activity("dsp-1") == "APPLIED"
    assert store.completed == ["dsp-1"]
    nodes = list(graph.iter_nodes(tenant_id="acme"))
    assert nodes[0][1]["memory_id"] == "memory-1"
    assert nodes[0][1]["projected_revision"] == 3


def test_projection_activity_reports_missing_canonical_repository(monkeypatch):
    store = object.__new__(PostgresStore)
    store.get_dispatch = lambda _dispatch_id: None
    store.get_dispatch_for_aggregate = lambda *_args, **_kwargs: None
    state = SimpleNamespace(control_plane=store, neo4j=InMemoryKnowledgeGraph())
    monkeypatch.setattr(activities, "get_state", lambda: state)

    with pytest.raises(ApplicationError, match="load_mutation_projection"):
        activities.project_neo4j_activity("mutation-1")


def test_memory_projection_rejects_older_revision_and_is_tenant_scoped():
    graph = InMemoryKnowledgeGraph()
    first = graph.upsert_memory_projection(
        memory_id="memory-1",
        revision=4,
        properties={"canonical_name": "new"},
        tenant_id="acme",
    )
    stale = graph.upsert_memory_projection(
        memory_id="memory-1",
        revision=3,
        properties={"canonical_name": "old"},
        tenant_id="acme",
    )
    other_tenant = graph.upsert_memory_projection(
        memory_id="memory-1",
        revision=1,
        properties={"canonical_name": "other"},
        tenant_id="other",
    )

    assert first["applied"] is True
    assert stale["applied"] is False
    assert stale["projected_revision"] == 4
    assert other_tenant["applied"] is True
    assert graph.node_count(tenant_id="acme") == 1
    assert graph.node_count(tenant_id="other") == 1
    assert next(iter(graph.iter_nodes(tenant_id="acme")))[1]["canonical_name"] == "new"


def test_canonical_vector_discovery_returns_stable_identity_and_revision():
    graph = InMemoryKnowledgeGraph()
    graph.upsert_memory_projection(
        memory_id="memory-1",
        revision=4,
        properties={"l0_embedding": [1.0, 0.0]},
        tenant_id="acme",
    )

    result = graph.vector_search([1.0, 0.0], tenant_id="acme")

    assert result[0]["memory_id"] == "memory-1"
    assert result[0]["projected_revision"] == 4


def test_claim_projection_is_deduplicated_by_claim_id_and_revision():
    graph = InMemoryKnowledgeGraph()
    graph.upsert_memory_projection(memory_id="subject", revision=1, tenant_id="acme")
    graph.upsert_memory_projection(memory_id="object", revision=1, tenant_id="acme")
    applied = graph.upsert_claim_projection(
        claim_id="claim-1",
        subject_memory_id="subject",
        object_memory_id="object",
        predicate="works_at",
        revision=2,
        tenant_id="acme",
    )
    stale = graph.upsert_claim_projection(
        claim_id="claim-1",
        subject_memory_id="subject",
        object_memory_id="object",
        predicate="old_predicate",
        revision=1,
        tenant_id="acme",
    )

    assert applied["applied"] is True
    assert stale["applied"] is False
    assert stale["projected_revision"] == 2
    assert graph.edge_count(tenant_id="acme") == 1


def test_neo4j_claim_projection_uses_valid_subquery_import_syntax():
    source = inspect.getsource(
        __import__("engram.storage.neo4j_store", fromlist=["Neo4jStore"]).Neo4jStore
    )

    assert "WITH s, o, current_revision WHERE" not in source
    assert source.count('"WITH s, o, current_revision "') >= 2
