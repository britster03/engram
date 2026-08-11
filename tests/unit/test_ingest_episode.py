"""Authoritative source preservation for conversational episode memories."""

from __future__ import annotations

from engram.ingest.worker import _episode_body, _source_provenance


def test_episode_body_preserves_source_turns_and_intermediate_tool_data() -> None:
    payload = {
        "turn_group": {
            "user": {
                "content": "The exact reason was art and self-expression.",
                "external_id": "D4:5",
                "speaker": "Caroline",
                "timestamp": "2023-06-27T10:37:00",
                "image_caption": "a hand-painted bowl",
            },
            "intermediate": [{
                "content": "lookup complete",
                "external_id": "tool-1",
                "tool_results": [{"result": "verbatim evidence"}],
            }],
            "assistant": {
                "content": "Thanks for sharing that precise detail.",
                "external_id": "D4:6",
                "speaker": "Melanie",
            },
        }
    }
    extraction = {
        "l0_abstract": "Caroline owns a sentimental bowl.",
        "resolved_text": "Caroline owns a bowl from her birthday.",
    }

    body = _episode_body(payload, extraction)

    assert body.splitlines()[0] == "Caroline owns a sentimental bowl."
    assert "## Resolved memory" in body
    assert "## Source turns" in body
    assert "The exact reason was art and self-expression." in body
    assert "Image caption: a hand-painted bowl" in body
    assert 'Tool results: [{"result": "verbatim evidence"}]' in body
    assert body.index("D4:5") < body.index("tool-1") < body.index("D4:6")
    assert body.endswith("\n")

    provenance = _source_provenance(payload)
    assert provenance["source_turn_ids"] == ["D4:5", "tool-1", "D4:6"]
    assert provenance["source_speakers"] == ["Caroline", "Melanie"]

