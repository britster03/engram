"""Enumeration packing must use original speaker records and bounded reads."""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from engram.retrieval.domain import RetrievalPlan
from engram.retrieval.orchestrator import RetrievalMetadata
from engram.retrieval.sufficiency import EvidenceItem, SufficiencyController
from tests.unit.test_adaptive_retrieval import RecordingFrontier, _context


def controller(store):
    ctx = _context(None, RecordingFrontier())
    ctx.control_plane = store
    return SufficiencyController(ctx, RetrievalMetadata(),
                                 RetrievalPlan(query="List Riley's activities."), "_default")


def claim(event="original", **updates):
    return EvidenceItem(id="claim:" + event, text="An incorrectly merged actor enjoys drumming.",
        source_kind="canonical", tenant_id="_default", source={"event_ids": [event]}, **updates)


def record(event="original", kind="INGEST", text="Riley: I enjoy drumming."):
    return {"event_id": event, "session_id": "old", "event_type": kind,
            "payload": {"turn_pair": {"user": {"content": text, "turn_idx": 8}}}}


def test_origin_replaces_derived_actor_text_and_is_cached():
    getter = Mock(return_value=record())
    control = controller(SimpleNamespace(get_event=getter))
    first = control._original_items([claim()])
    assert first[0].text == "Riley: I enjoy drumming."
    assert first[0].id == "event:original:user:8"
    assert first[0].source_kind == "conversation_record"
    assert control._original_items([claim()]) == first
    getter.assert_called_once_with("original", tenant_id="_default")


@pytest.mark.parametrize("origin", [None, record(kind="SESSION_SUMMARY"), record(text="")])
def test_missing_summary_or_empty_origins_are_discovery_only(origin):
    control = controller(SimpleNamespace(get_event=lambda *a, **k: origin))
    assert not control.eligible(control._original_items([claim()]))


def test_origin_failure_does_not_promote_an_unresolved_claim():
    control = controller(SimpleNamespace(get_event=Mock(side_effect=ConnectionError())))
    assert not control.eligible(control._original_items([claim()]))
    assert "enumeration_original_records" in control.md.degraded_sources


def test_graph_edges_and_original_records_keep_the_same_atomic_bundle():
    control = controller(SimpleNamespace(get_event=lambda *a, **k: record()))
    edge = claim(bundles=["path:x"])
    originals = control._original_items([edge])
    assert edge in originals
    assert all(item.bundles == ["path:x"] for item in originals)


def test_origin_batches_are_tenant_scoped_and_bounded():
    calls = []
    def execute(statement, parameters):
        assert "tenant_id = ?" in statement
        assert parameters[0] == "_default"
        calls.append(parameters[1])
        return SimpleNamespace(fetchall=lambda: [record(event) for event in parameters[1]])
    control = controller(SimpleNamespace(get_conn=lambda: SimpleNamespace(execute=execute)))
    items = [claim(str(index)) for index in range(201)]
    assert len(control._original_items(items)) == 201
    assert [len(batch) for batch in calls] == [200, 1]
    control._original_items(items)
    assert len(calls) == 2
