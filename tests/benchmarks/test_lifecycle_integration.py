"""Real isolated PG/Neo4j checks, with no model calls."""
import json
import gzip
import os
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def state():
    if not os.environ.get("LOCOMO_PROJECT","").startswith("engram-lifecycle-"):
        pytest.skip("Requires the private lifecycle Compose stack")
    from benchmarks.lifecycle.infrastructure import assert_isolated
    from engram.storage.postgres import PostgresStore
    from engram.storage.neo4j_store import Neo4jStore
    from engram.storage.memory_repository import MemoryRepository
    from engram.storage.redis_cache import SessionCache
    cfg=assert_isolated()
    pg=PostgresStore(cfg.event_ledger.dsn,initialize_schema=False)
    neo=Neo4jStore(cfg.knowledge_graph)
    result=SimpleNamespace(cfg=cfg,control_plane=pg,neo4j=neo,memory_repository=MemoryRepository(pg),session_cache=SessionCache(cfg.session_cache,default_ttl_seconds=1800),embed=None,core=None,frontier=None,fs=None)
    yield result
    pg.close()
    neo.close()


def test_checkpoint_restore_absent_future_and_tenant_isolation(state,tmp_path):
    from benchmarks.lifecycle.snapshot import save,restore,fingerprint
    tid="locomo-test-"+uuid.uuid4().hex[:12]
    pg=state.control_plane
    event_id,_ = pg.record_event(pair_id="first",session_id="s1",source="test",event_type="QUERY",payload={"turn_pair":{"user":{"content":"Alice first"},"assistant":{"content":""}}},tenant_id=tid,initial_status="COMPLETE")
    state.memory_repository.record_ingest_artifact(event_id=event_id,artifact_type="SOURCE_TEXT",artifact_key="binary",content=b"original\x00bytes\xff",media_type="application/octet-stream",tenant_id=tid)
    state.memory_repository.record_ingest_artifact(event_id=event_id,artifact_type="SOURCE_TEXT",artifact_key="json-null",content=b"json",media_type="application/octet-stream",tenant_id=tid)
    with pg.transaction() as conn:
        conn.execute("UPDATE ingest_artifacts SET source_span='null'::jsonb WHERE tenant_id=? AND artifact_key='json-null'",(tid,))
    alice=state.memory_repository.create_memory(memory_type="ENTITY",canonical_name="Alice",tenant_id=tid,emit_projection=False)
    bob=state.memory_repository.create_memory(memory_type="ENTITY",canonical_name="Bob",tenant_id=tid,emit_projection=False)
    state.memory_repository.add_claim(subject_id=alice.id,predicate="knows",object_entity_id=bob.id,object_type="ENTITY",confidence=1,source_event_id=event_id,tenant_id=tid,emit_projection=False)
    pg.save_session_state("s1",{"session_id":"s1","turns":[]},tenant_id=tid)
    state.neo4j.upsert_memory_projection(memory_id="alice",revision=1,properties={"canonical_name":"Alice"},tenant_id=tid)
    state.neo4j.upsert_memory_projection(memory_id="other",revision=1,properties={"canonical_name":"Other"},tenant_id=tid+"-other")
    expected=fingerprint(state,tid)
    path=tmp_path/"checkpoint.json.gz"
    save(state,tid,path,cutoff=1)
    pg.record_event(pair_id="future",session_id="s2",source="test",event_type="QUERY",payload={},tenant_id=tid,initial_status="COMPLETE")
    state.neo4j.upsert_memory_projection(memory_id="future",revision=1,properties={},tenant_id=tid)
    restored=restore(state,path)
    assert fingerprint(state,tid)==expected
    assert {e["session_id"] for e in restored["postgres"]["events"]}=={"s1"}
    assert pg.list_session_events("s1",tenant_id=tid+"-other")==[]
    assert pg.get_conn().execute("SELECT source_span IS NULL AS sql_null FROM ingest_artifacts WHERE tenant_id=? AND artifact_key='binary'",(tid,)).fetchone()["sql_null"]
    assert not pg.get_conn().execute("SELECT source_span IS NULL AS sql_null FROM ingest_artifacts WHERE tenant_id=? AND artifact_key='json-null'",(tid,)).fetchone()["sql_null"]
    assert pg.get_conn().execute("SELECT object_value IS NULL AS sql_null FROM memory_claims WHERE tenant_id=?",(tid,)).fetchone()["sql_null"]
    # Older saved checkpoints lack SQL-null flags. Their row order must be
    # reproduced after stripping the new flags, including entity claims.
    from benchmarks.lifecycle.snapshot import canonical,checksum,collect
    from benchmarks.lifecycle.ledger import atomic
    with pg.transaction() as conn:
        conn.execute("DELETE FROM ingest_artifacts WHERE tenant_id=? AND artifact_key='json-null'",(tid,))
    restored=collect(state,tid)
    expected=fingerprint(state,tid)
    for rows in restored["postgres"].values():
        for row in rows:
            row.pop("__sql_nulls__",None)
        rows.sort(key=canonical)
    legacy=tmp_path/"legacy.json.gz"
    with gzip.open(legacy,"wt") as stream:
        stream.write(canonical(restored))
    import hashlib
    atomic(legacy.with_suffix(".manifest.json"),{"file_sha256":hashlib.sha256(legacy.read_bytes()).hexdigest(),"fingerprints":{k:checksum(restored[k]) for k in ["postgres","neo4j","schema"]}})
    restore(state,legacy)
    assert fingerprint(state,tid)==expected
    with state.neo4j.reader().session() as session:
        assert session.run("MATCH (n {tenant_id:$tid}) RETURN count(n) AS n",tid=tid+"-other").single()["n"]==1
    with state.neo4j.writer().session() as session:
        session.run("MATCH (n) WHERE n.tenant_id IN $ids DETACH DELETE n",ids=[tid,tid+"-other"]).consume()
    # Test artifacts use their own tenant, and are absent from the dataset.


def test_native_queries_cannot_write(state):
    from benchmarks.lifecycle.adapter import readonly_context
    with readonly_context(state,"locomo-test-readonly") as ctx:
        with pytest.raises(Exception,match="read-only"):
            ctx.control_plane.save_session_state("forbidden",{},tenant_id="locomo-test-readonly")
