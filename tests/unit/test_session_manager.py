"""Tests for the session manager and compaction."""

from pathlib import Path

from engram.config import SessionCacheConfig
from engram.models.core import CompletionResult, CoreModelProvider
from engram.session.manager import SessionManager, commit_session, compact_session
from engram.storage.filesystem import FilesystemStore
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from engram.storage.redis_cache import SessionCache
from engram.storage.sqlite import SqliteStore


class _InMemoryCache(SessionCache):
    """Override SessionCache to force memory backend."""

    def __init__(self, ttl: int) -> None:
        super().__init__(SessionCacheConfig(backend="memory"), default_ttl_seconds=ttl)


class _StubCore(CoreModelProvider):
    def complete(self, **_kwargs):  # type: ignore[override]
        return CompletionResult(
            output={
                "compacted": "(summary here)",
                "key_facts": ["user moved to NYC"],
                "key_entities": ["NYC"],
            },
            raw_text="(stub)",
        )


class _StubEmbed:
    def embed(self, _text: str) -> list[float]:
        return [0.1] * 384


def test_create_and_get_roundtrip():
    cache = _InMemoryCache(ttl=60)
    mgr = SessionManager(cache, window_threshold_ratio=0.4,
                         max_turns_before_window=50, session_ttl_minutes=30)
    s = mgr.create()
    assert s.status == "ACTIVE"
    assert len(s.turns) == 0
    same = mgr.get(s.session_id)
    assert same is not None
    assert same.session_id == s.session_id


def test_append_turn_pair_tracks_indices():
    cache = _InMemoryCache(ttl=60)
    mgr = SessionManager(cache, window_threshold_ratio=0.4,
                         max_turns_before_window=50, session_ttl_minutes=30)
    s = mgr.create()
    s, needs = mgr.append_turn_pair(s.session_id, "hello", "hi")
    assert len(s.turns) == 2
    assert s.turns[0].turn_idx == 0
    assert s.turns[1].turn_idx == 1
    assert needs is False
    s, needs = mgr.append_turn_pair(s.session_id, "again", "again")
    assert s.turns[2].turn_idx == 2
    assert s.turns[3].turn_idx == 3


def test_compaction_updates_bound():
    cache = _InMemoryCache(ttl=60)
    mgr = SessionManager(cache, window_threshold_ratio=0.4,
                         max_turns_before_window=4, session_ttl_minutes=30)
    s = mgr.create()
    for i in range(6):
        mgr.append_turn_pair(s.session_id, f"user msg {i}", f"asst msg {i}")
    s = mgr.get(s.session_id)
    assert s is not None
    before_bound = s.compacted_turns_idx_upper_bound
    s = compact_session(mgr, s, _StubCore())
    assert s.compacted is not None and "(summary here)" in s.compacted
    assert s.compacted_turns_idx_upper_bound > before_bound
    assert "user moved to NYC" in s.key_facts


def test_compaction_reenqueue_preserves_tenant(tmp_path: Path):
    cache = _InMemoryCache(ttl=60)
    mgr = SessionManager(cache, window_threshold_ratio=0.4,
                         max_turns_before_window=4, session_ttl_minutes=30)
    sqlite = SqliteStore(tmp_path / "events.db")
    s = mgr.create()
    for i in range(6):
        mgr.append_turn_pair(s.session_id, f"user msg {i}", f"asst msg {i}")
    s = mgr.get(s.session_id)
    assert s is not None

    compact_session(mgr, s, _StubCore(), sqlite=sqlite, tenant_id="tenant-a")

    rows = sqlite.get_conn().execute(
        "SELECT DISTINCT tenant_id FROM events WHERE source = 'session_compact'"
    ).fetchall()
    assert [row["tenant_id"] for row in rows] == ["tenant-a"]


def test_commit_session_writes_summary_and_reenqueues_turns(tmp_path: Path):
    cache = _InMemoryCache(ttl=60)
    mgr = SessionManager(cache, window_threshold_ratio=0.4,
                         max_turns_before_window=50, session_ttl_minutes=30)
    sqlite = SqliteStore(tmp_path / "events.db")
    fs = FilesystemStore(tmp_path / "mem")
    kg = InMemoryKnowledgeGraph()
    s = mgr.create()
    mgr.append_turn_pair(s.session_id, "I moved to Berlin.", "Stored.")
    s = mgr.get(s.session_id)
    assert s is not None

    committed = commit_session(
        mgr,
        s,
        _StubCore(),
        sqlite=sqlite,
        fs=fs,
        neo4j=kg,
        embed=_StubEmbed(),  # type: ignore[arg-type]
        tenant_id="tenant-a",
    )

    assert committed.status == "COMMITTED"
    rows = sqlite.get_conn().execute(
        "SELECT tenant_id, status FROM events WHERE session_id = ?",
        (s.session_id,),
    ).fetchall()
    assert [(row["tenant_id"], row["status"]) for row in rows] == [("tenant-a", "RECEIVED")]
    summary_uri = f"mem://user/session_summaries/{s.session_id}.md"
    assert fs.exists(summary_uri)
    node = next(
        n for n in kg.graph(root_uri=summary_uri, tenant_id="tenant-a")["nodes"]
        if n["source_uri"] == summary_uri
    )
    assert node["node_type"] == "SESSION_SUMMARY"
