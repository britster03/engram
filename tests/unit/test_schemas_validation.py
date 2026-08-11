"""Pydantic schema validation tests — caps + field constraints."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from engram.api import schemas


def test_query_rejects_oversized_query():
    with pytest.raises(ValidationError):
        schemas.QueryRequest(query="x" * 10_000)


def test_query_rejects_empty():
    with pytest.raises(ValidationError):
        schemas.QueryRequest(query="")


def test_query_depth_pattern():
    with pytest.raises(ValidationError):
        schemas.QueryRequest(query="hi", max_depth="L5")
    assert schemas.QueryRequest(query="hi", max_depth="L4").max_depth == "L4"


def test_query_max_reentries_bounds():
    with pytest.raises(ValidationError):
        schemas.QueryRequest(query="hi", max_reentries=6)
    with pytest.raises(ValidationError):
        schemas.QueryRequest(query="hi", max_reentries=-1)


def test_ingest_rejects_session_id_with_whitespace():
    with pytest.raises(ValidationError):
        schemas.IngestRequest(
            session_id="bad id",
            turn_pair=schemas.TurnPair(
                user=schemas.TurnContent(content="hi"),
                assistant=schemas.TurnContent(content="ok"),
            ),
        )


def test_ingest_rejects_oversized_turn_content():
    with pytest.raises(ValidationError):
        schemas.IngestRequest(
            turn_pair=schemas.TurnPair(
                user=schemas.TurnContent(content="x" * 40_000),
                assistant=schemas.TurnContent(content="ok"),
            ),
        )


def test_ingest_effective_pair_for_turn_group():
    req = schemas.IngestRequest(
        turn_group=schemas.TurnGroup(
            user=schemas.TurnContent(content="hi", turn_idx=0),
            assistant=schemas.TurnContent(content="bye", turn_idx=3),
            intermediate=[
                schemas.TurnContent(content="tool_call", turn_idx=1),
                schemas.TurnContent(content="tool_result", turn_idx=2),
            ],
        ),
    )
    pair = req.effective_pair()
    assert pair.user.content == "hi"
    assert pair.assistant.content == "bye"


def test_turn_content_preserves_external_and_multimodal_provenance():
    turn = schemas.TurnContent(
        content="A: Look at this",
        external_id="D1:5",
        speaker="A",
        source_conversation_id="sample-1",
        source_session_id="session_1",
        source_task="locomo",
        image_caption="a red bicycle",
        image_urls=["https://example.test/bike.jpg"],
        image_query="red bicycle",
    )
    dumped = turn.model_dump(exclude_none=True)
    assert dumped["external_id"] == "D1:5"
    assert dumped["image_caption"] == "a red bicycle"
