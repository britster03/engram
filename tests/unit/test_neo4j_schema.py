from __future__ import annotations

from typing import Any

import pytest

from engram.config import KnowledgeGraphConfig
from engram.storage.neo4j_store import INDEX_NAMES, Neo4jStore


class _Result:
    def __init__(self, rows: list[dict[str, str]] | None = None) -> None:
        self.rows = rows or []

    def consume(self) -> None:
        return None

    def __iter__(self):
        return iter(self.rows)


class _Session:
    def __init__(self, states: dict[str, str]) -> None:
        self.states = states
        self.queries: list[tuple[str, dict[str, Any]]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def run(self, query: str, **params: Any) -> _Result:
        self.queries.append((query, params))
        if query.startswith("SHOW INDEXES"):
            return _Result([
                {"name": name, "state": state} for name, state in self.states.items()
            ])
        return _Result()


class _Driver:
    def __init__(self, session: _Session) -> None:
        self._session = session

    def session(self) -> _Session:
        return self._session


def _store(states: dict[str, str]) -> tuple[Neo4jStore, _Session]:
    session = _Session(states)
    store = Neo4jStore(KnowledgeGraphConfig())
    store._writer = _Driver(session)  # type: ignore[assignment]
    store._reader = _Driver(session)  # type: ignore[assignment]
    return store, session


def test_ensure_indexes_waits_and_verifies_every_required_index() -> None:
    store, session = _store({name: "ONLINE" for name in INDEX_NAMES})
    store.ensure_indexes(timeout_seconds=17)
    assert any(
        query.startswith("CALL db.awaitIndexes") and params["timeout_seconds"] == 17
        for query, params in session.queries
    )


def test_ensure_indexes_fails_closed_when_vector_index_is_missing() -> None:
    states = {name: "ONLINE" for name in INDEX_NAMES if name != "l0_idx"}
    store, _session = _store(states)
    assert store.indexes_ready() is False
    with pytest.raises(RuntimeError, match="l0_idx=MISSING"):
        store.ensure_indexes()
