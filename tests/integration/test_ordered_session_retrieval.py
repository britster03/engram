"""Real PostgreSQL tests; all writes are confined to isolated test schemas."""

import uuid

import pytest

from engram.config import SessionCacheConfig
from engram.retrieval.tools import MemoryTools, lexical_query
from engram.session.manager import SessionManager, SessionState, _commit_canonical_session_summary
from engram.storage.memory_repository import MemoryRepository
from engram.storage.redis_cache import SessionCache
from tests.postgres_support import PostgresTestStore
from tests.unit.test_adaptive_retrieval import RecordingFrontier, _context


@pytest.fixture
def store():
    value = PostgresTestStore()
    MemoryRepository(value).ensure_schema()
    yield value
    value.close()


def record(store, session_id, content, *, tenant="_default", index=0, event_type="QUERY"):
    return store.record_event(
        pair_id=uuid.uuid4().hex,
        session_id=session_id,
        tenant_id=tenant,
        source="test",
        event_type=event_type,
        payload={
            "derived_response": event_type == "QUERY",
            "turn_pair": {
                "user": {"content": content, "turn_idx": index},
                "assistant": {"content": "Noted", "turn_idx": index + 1},
            },
        },
    )[0]


def test_past_search_excludes_active_session_and_other_tenants(store):
    wanted = record(store, "past", "I prefer coffee")
    record(store, "active", "I prefer coffee")
    record(store, "past", "I prefer coffee", tenant="other")
    rows = store.search_session_events(
        lexical_query("What did I say about coffee?"),
        tenant_id="_default",
        exclude_session_id="active",
    )
    assert [row["event_id"] for row in rows] == [wanted]
    assert rows[0]["payload"]["turn_pair"]["user"]["content"] == "I prefer coffee"


def test_search_is_bounded_and_parameterized(store):
    for index in range(5):
        record(store, "past", "coffee", index=index * 2)
    rows = store.search_session_events("coffee", tenant_id="_default", limit=2)
    assert len(rows) == 2
    assert store.search_session_events("coffee", tenant_id="' OR TRUE --") == []


def test_cache_miss_reconstructs_turn_indices_and_derived_flags(store):
    record(store, "active", "My name is Ravi", index=4, event_type="INGEST")
    record(store, "active", "What is my name?", index=6)
    cache = SessionCache(SessionCacheConfig(backend="memory"), default_ttl_seconds=60)
    manager = SessionManager(
        cache,
        window_threshold_ratio=0.4,
        max_turns_before_window=50,
        session_ttl_minutes=1,
        control_plane=store,
    )
    state = manager.get("active")
    assert [turn.turn_idx for turn in state.turns] == [4, 5, 6, 7]
    assert [turn.ingestible for turn in state.turns] == [True, True, False, False]
    # A durable write invalidates the cache; the next read warms it from the
    # committed snapshot rather than publishing a potentially stale writer copy.
    assert manager.get("active").turns == state.turns
    assert cache.get("active") is not None
    state, _ = manager.append_turn_pair("active", "Next", "Reply", ingestible=False)
    assert [turn.turn_idx for turn in state.turns[-2:]] == [8, 9]


def test_summary_match_resolves_to_original_records(store):
    repository = MemoryRepository(store)
    wanted = record(store, "past", "We chose option B")
    _commit_canonical_session_summary(
        repository=repository,
        state=SessionState(session_id="past"),
        summary="Atlas database choice",
        tenant_id="_default",
    )
    context = _context(repository, RecordingFrontier())
    context.control_plane = store
    from engram.retrieval.planner import AdaptivePlanner

    result = MemoryTools(context, tenant_id="_default", session_id="active").past_session_search(
        AdaptivePlanner().plan("What did we decide about Atlas?")
    )
    assert result.sources[0]["event_id"] == wanted
    assert "We chose option B" in result.text
    assert "Atlas database choice" not in result.text


def test_current_session_history_is_tenant_scoped(store):
    record(store, "same-id", "secret", tenant="other")
    assert store.list_session_events("same-id", tenant_id="_default", limit=100) == []


def test_committed_and_empty_sessions_survive_cache_loss(store):
    cache = SessionCache(SessionCacheConfig(backend="memory"), default_ttl_seconds=60)
    manager = SessionManager(
        cache,
        window_threshold_ratio=0.4,
        max_turns_before_window=50,
        session_ttl_minutes=1,
        control_plane=store,
    )
    state = manager.create()
    cache.delete(state.session_id)
    assert manager.get(state.session_id).status == "ACTIVE"
    state.status = "COMMITTED"
    manager._persist(state)
    cache.delete(state.session_id)
    assert manager.get(state.session_id).status == "COMMITTED"
    assert store.get_session_state(state.session_id, tenant_id="other") is None


def test_session_append_commits_snapshot_and_event_atomically(store, monkeypatch):
    cache = SessionCache(SessionCacheConfig(backend="memory"), default_ttl_seconds=60)
    manager = SessionManager(
        cache,
        window_threshold_ratio=0.4,
        max_turns_before_window=50,
        session_ttl_minutes=1,
        control_plane=store,
    )
    state = manager.create()

    def fail(**kwargs):
        raise ConnectionError("simulated event write failure")

    monkeypatch.setattr(store, "record_event", fail)
    with pytest.raises(ConnectionError):
        manager.append_turn_pair(state.session_id, "must roll back", "answer", ingestible=False)
    assert store.get_session_state(state.session_id, tenant_id="_default")["turns"] == []
    assert manager.get(state.session_id).turns == []
    assert cache.get(state.session_id)["turns"] == []


def test_current_session_search_can_find_an_old_turn_beyond_cached_tail(store):
    wanted = record(store, "active", "My favorite drink is coffee", index=0)
    record(store, "other-session", "My favorite drink is coffee", index=0)
    rows = store.search_session_events("coffee", tenant_id="_default", only_session_id="active")
    assert [row["event_id"] for row in rows] == [wanted]


def test_reference_neighbors_are_turn_ordered_and_tenant_session_scoped(store):
    before = record(store, "active", "Caroline applied to City University", index=2)
    anchor = record(store, "active", "She will study there", index=4)
    after = record(store, "active", "The course begins in June", index=6)
    record(store, "foreign-session", "private foreign session", index=3)
    record(store, "active", "private tenant", tenant="other", index=3)
    rows = store.adjacent_session_events(anchor, tenant_id="_default")
    assert {row["event_id"] for row in rows} == {before, after}
    assert store.adjacent_session_events(anchor, tenant_id="other") == []


def test_enforced_search_expands_known_predicate_and_keeps_reference_bundle(store):
    from engram.retrieval.domain import RetrievalPlan
    before = record(store, "past", "Alice accepted a new position", index=0)
    anchor = record(store, "past", "She is the engineering director", index=2)
    context = _context(MemoryRepository(store), RecordingFrontier())
    context.control_plane = store
    context.cfg.retrieval.quality_mode = "enforce"
    plan = RetrievalPlan(query="What is Alice's role?", entity_hints=["Alice"], predicate_hint="HAS_ROLE")
    tools = MemoryTools(context, tenant_id="_default", session_id="active")
    assert "position" in tools._search_query(plan)
    assert before in {s["event_id"] for s in tools.past_session_search(plan).sources}
    # Directly exercise the bounded co-reference expansion when the selected
    # hit contains the pronoun; no inference of the person's identity occurs.
    row = store.get_event(anchor, tenant_id="_default")
    expanded = tools._reference_neighbors([row])
    assert {r["event_id"] for r in expanded} == {before, anchor}
    assert all("reference:" + anchor in r["reference_bundles"] for r in expanded)
