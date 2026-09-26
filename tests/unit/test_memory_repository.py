"""Focused checks for the canonical PostgreSQL/domain boundary."""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone

import pytest

from engram.storage.canonical_schema import CANONICAL_SCHEMA_SQL, schema_statements
from engram.storage.memory_repository import (
    InvalidClaimError,
    MemoryRepository,
    TypedClaim,
    infer_object_type,
    normalize_alias,
    normalize_predicate,
)


def test_canonical_schema_uses_shared_temporal_dispatch_and_required_types():
    required_tables = {
        "canonical_mutations",
        "memory_nodes",
        "memory_versions",
        "memory_claims",
        "memory_evidence",
        "entity_aliases",
        "memory_hierarchy",
        "memory_overviews",
        "memory_uri_aliases",
        "ingest_artifacts",
    }
    assert all(
        f"CREATE TABLE IF NOT EXISTS {table}" in CANONICAL_SCHEMA_SQL for table in required_tables
    )
    assert "CREATE TABLE IF NOT EXISTS projection_outbox" not in CANONICAL_SCHEMA_SQL
    assert "UNIQUE (tenant_id, source_event_id, mutation_type)" in CANONICAL_SCHEMA_SQL
    for memory_type in (
        "ENTITY",
        "EPISODE",
        "SESSION_SUMMARY",
        "COLLECTION",
        "PROJECT",
        "FILE",
        "CLASS",
        "FUNCTION",
        "METHOD",
        "EXTERNAL_MODULE",
    ):
        assert f"'{memory_type}'" in CANONICAL_SCHEMA_SQL
    assert "content BYTEA NOT NULL" in CANONICAL_SCHEMA_SQL
    assert "media_type TEXT NOT NULL" in CANONICAL_SCHEMA_SQL
    assert "expires_at TIMESTAMPTZ" in CANONICAL_SCHEMA_SQL
    assert len(schema_statements()) > 20


def test_typed_normalization_and_exact_object_contract():
    assert normalize_alias("  Angie\u00a0Jones ") == "angie jones"
    assert normalize_alias("atul_singh") == "atul singh"
    assert normalize_alias("Orion-DEV") == "orion-dev"
    assert normalize_predicate("holds_role") == "HAS_ROLE"
    assert infer_object_type(True) == "BOOLEAN"
    assert infer_object_type(2) == "INTEGER"
    assert infer_object_type(date(2026, 9, 1)) == "DATE"
    with pytest.raises(InvalidClaimError):
        TypedClaim(
            subject_id=uuid.uuid4(),
            predicate="HAS_ROLE",
            object_type="ROLE",
        )


def _postgres_repository() -> tuple[MemoryRepository, object]:
    if not os.environ.get("ENGRAM_TEST_DATABASE_URL"):
        pytest.skip("set ENGRAM_TEST_DATABASE_URL to run PostgreSQL canonical tests")
    from tests.postgres_support import PostgresTestStore

    store = PostgresTestStore()
    repository = MemoryRepository(store)
    repository.ensure_schema()
    return repository, store


def test_postgres_canonical_lifecycle_and_replay():
    repository, store = _postgres_repository()
    event_id, _ = store.record_event(
        pair_id=f"canonical-{uuid.uuid4()}",
        session_id="session-1",
        source="test",
        event_type="INGEST",
        payload={"text": "Angie became a director."},
        tenant_id="tenant-a",
    )
    node = repository.create_memory(
        memory_type="ENTITY",
        canonical_name="Angie Jones",
        origin_event_id=event_id,
        tenant_id="tenant-a",
        mutation_key="create-angie",
    )
    replay = repository.create_memory(
        memory_type="ENTITY",
        canonical_name="ignored on replay",
        origin_event_id=event_id,
        tenant_id="tenant-a",
        mutation_key="create-angie",
    )
    assert replay.id == node.id
    version = repository.append_version(
        node.id,
        body="Angie is a director.",
        source_event_id=event_id,
        tenant_id="tenant-a",
        mutation_key="version-angie",
    )
    assert (
        repository.append_version(
            node.id,
            body="different replay body",
            source_event_id=event_id,
            tenant_id="tenant-a",
            mutation_key="version-angie",
        ).id
        == version.id
    )
    state = repository.get_current_state(node.id, tenant_id="tenant-a")
    assert state is not None
    assert state.current_version is not None
    assert state.current_version.id == version.id
    assert repository.get_history(node.id, tenant_id="tenant-a")["versions"]
    assert repository.get_mutation("create-angie", tenant_id="tenant-a") is not None
    retired = repository.retire_memory(node.id, tenant_id="tenant-a", mutation_key="retire-angie")
    assert retired.status == "HISTORICAL"


def test_postgres_claim_typing_conflict_history_and_shared_dispatch():
    repository, store = _postgres_repository()
    event_id, _ = store.record_event(
        pair_id=f"claims-{uuid.uuid4()}",
        session_id=None,
        source="test",
        event_type="INGEST",
        payload={},
        tenant_id="tenant-a",
    )
    subject = repository.create_memory(
        memory_type="ENTITY",
        canonical_name="Angie",
        origin_event_id=event_id,
        tenant_id="tenant-a",
        emit_projection=False,
    )
    claims = repository.apply_claims(
        subject.id,
        [
            {"predicate": "holds_role", "object_value": "Director", "object_type": "ROLE"},
            {
                "predicate": "birthday",
                "object_value": "2026-09-01",
                "object_type": "DATE",
            },
        ],
        source_event_id=event_id,
        tenant_id="tenant-a",
    )
    assert {claim.predicate for claim in claims} == {"HAS_ROLE", "HAS_BIRTH_DATE"}
    assert {claim.object_type for claim in claims} == {"ROLE", "DATE"}
    assert repository.get_claims_as_of(subject.id, datetime.now(timezone.utc), tenant_id="tenant-a")
    later_event, _ = store.record_event(
        pair_id=f"claims-later-{uuid.uuid4()}",
        session_id=None,
        source="test",
        event_type="INGEST",
        payload={},
        tenant_id="tenant-a",
    )
    replacement = repository.add_claim(
        subject_id=subject.id,
        predicate="role",
        object_value="VP Engineering",
        object_type="ROLE",
        valid_from=datetime(2026, 9, 2, tzinfo=timezone.utc),
        source_event_id=later_event,
        source_triplet_index=0,
        tenant_id="tenant-a",
    )
    repository.supersede_claim(claims[0].id, replacement.id, tenant_id="tenant-a")
    old = next(
        item
        for item in repository.get_claim_history(subject.id, tenant_id="tenant-a")
        if item.id == claims[0].id
    )
    assert old.valid_until == datetime(2026, 9, 2, tzinfo=timezone.utc)
    assert old.id in {
        item.id
        for item in repository.get_claims_as_of(
            subject.id, datetime(2026, 9, 1, tzinfo=timezone.utc), tenant_id="tenant-a"
        )
    }
    assert old.id not in {
        item.id
        for item in repository.get_claims_as_of(
            subject.id, datetime(2026, 9, 3, tzinfo=timezone.utc), tenant_id="tenant-a"
        )
    }
    scalar_dispatch = next(
        item
        for item in repository.claim_projection_dispatches(tenant_id="tenant-a", limit=20)
        if item.payload.get("claim_id") == str(claims[1].id)
    )
    scalar_snapshot = repository.load_projection_snapshot(
        scalar_dispatch.aggregate_id, tenant_id="tenant-a"
    )
    assert scalar_snapshot is not None
    assert scalar_snapshot["scalar_claim_id"] == str(claims[1].id)
    assert scalar_snapshot["claim_id"] is None
    assert scalar_snapshot["properties"]["object_value"] == "2026-09-01"
    assert scalar_snapshot["properties"]["is_scalar_claim"] is True

    object_node = repository.create_memory(
        memory_type="ENTITY",
        canonical_name="Acme",
        origin_event_id=later_event,
        tenant_id="tenant-a",
        emit_projection=False,
    )
    relationship = repository.add_claim(
        subject_id=subject.id,
        predicate="works_at",
        object_entity_id=object_node.id,
        source_event_id=later_event,
        source_triplet_index=1,
        tenant_id="tenant-a",
    )
    relationship_dispatch = next(
        item
        for item in repository.claim_projection_dispatches(tenant_id="tenant-a", limit=50)
        if item.payload.get("claim_id") == str(relationship.id)
    )
    relationship_snapshot = repository.load_projection_snapshot(
        relationship_dispatch.aggregate_id, tenant_id="tenant-a"
    )
    assert relationship_snapshot is not None
    assert relationship_snapshot["claim_id"] == str(relationship.id)
    assert relationship_snapshot["subject_memory_id"] == str(subject.id)
    assert relationship_snapshot["object_memory_id"] == str(object_node.id)
    assert relationship_snapshot["predicate"] == "WORKS_AT"
    assert relationship_snapshot["properties"]["status"] == "ACTIVE"
    assert relationship_snapshot["node_properties"]["canonical_uri"] == subject.canonical_uri
    assert scalar_dispatch.aggregate_type == "PROJECTION"
    assert relationship_dispatch.aggregate_type == "PROJECTION"


def test_postgres_tenant_alias_hierarchy_overview_and_zip_artifact():
    repository, store = _postgres_repository()
    event_id, _ = store.record_event(
        pair_id=f"zip-{uuid.uuid4()}",
        session_id="session-zip",
        source="test",
        event_type="UPLOAD",
        payload={},
        tenant_id="tenant-a",
    )
    commit = repository.commit_code_project(
        event_id=event_id,
        project_name="User service",
        project_uri="mem://projects/user-service",
        archive_bytes=b"PK\x03\x04canonical-test",
        files={"src/main.py": "print('ok')"},
        tenant_id="tenant-a",
    )
    assert commit.project.canonical_uri.startswith("mem://memory/")
    assert (
        repository.resolve_uri("mem://projects/user-service", tenant_id="tenant-a").id
        == commit.project.id
    )
    assert commit.artifact.content == b"PK\x03\x04canonical-test"
    assert commit.artifact.content_hash
    children = repository.get_children(commit.project.id, tenant_id="tenant-a")
    assert children
    with pytest.raises(ValueError, match="cycle"):
        repository.add_hierarchy(
            parent_id=children[0].id,
            child_id=commit.project.id,
            tenant_id="tenant-a",
        )
    listed = repository.list_memories("tenant-a", prefix="mem://projects/user-service", limit=10)
    assert commit.project.id in {item.id for item in listed.nodes}
    overview = repository.save_overview(
        scope_id=commit.project.id,
        content="project overview",
        input_revision=commit.project.revision,
        tenant_id="tenant-a",
    )
    assert (
        repository.get_overview(commit.project.id, tenant_id="tenant-a", require_current=False).id
        == overview.id
    )
    overview_task = repository.enqueue_overview_regeneration(
        commit.project.id, tenant_id="tenant-a"
    )
    assert overview_task is not None
    assert repository.get_memory(commit.project.id, tenant_id="tenant-b") is None
