"""Activity enumeration cannot terminate on the last session's unrelated topics."""
import json
from types import SimpleNamespace

import pytest

from engram.models.core import CompletionResult
from engram.models.frontier import FrontierVerdict
from engram.retrieval.orchestrator import run_query
from tests.unit.test_adaptive_retrieval import RecordingFrontier, _context

QUESTION = "What activities does Melanie partake in?"
NOISE = "Melanie: I bought figurines yesterday. Congratulations on the adoption agency interviews."
ORIGINALS = [
    ("D5:4", "s005", "Melanie: I just signed up for a pottery class yesterday."),
    ("D9:1", "s009", "Melanie: We went camping with the family two weekends ago."),
    ("D1:14", "s001", "Melanie: Yeah, I painted that lake sunrise last year! Painting is fun."),
    ("D1:18", "s001", "Melanie: I'm off to go swimming with the kids."),
]


def record(identity, session, text):
    return {"event_id": identity, "session_id": session, "created_at": "2023-07-17",
        "payload": {"turn_pair": {"user": {"content": text, "turn_idx": 0,
        "timestamp": "2023-07-17"}}}}


class Frontier(RecordingFrontier):
    def __init__(self, wrong=False):
        super().__init__()
        self.wrong = wrong

    def answer(self, **kwargs):
        self.calls.append(kwargs["msc"])
        return FrontierVerdict("ANSWER", answer="Melanie bought figurines and discussed adoption." if self.wrong else
            "Her documented activities include pottery, camping, painting, and swimming; the retrieved sources may not cover every activity.")


class Reviewer:
    def complete(self, **kwargs):
        payload = json.loads(kwargs["user_prompt"])
        evidence = payload["evidence"]
        citations = [{"evidence_id": item["id"], "quote": item["text"]} for item in evidence]
        if "answer" in payload:
            supported = all(word in payload["answer"] for word in ["pottery", "camping", "painting", "swimming"])
            supported = supported and all(any(word in item["text"].lower() for item in evidence)
                                          for word in ["pottery", "camping", "painting", "swimming"])
            output = {"supported": supported, "citations": citations}
        else:
            # An overconfident positive review of noise cannot establish exhaustive scope.
            output = {"requirements": [{"requirement_id": r["id"], "status": "SUPPORTED",
                "citations": citations} for r in payload["requirements"]]}
        return CompletionResult(output=output, raw_text=json.dumps(output))


def context(wrong=False):
    frontier = Frontier(wrong)
    ctx = _context(None, frontier)
    ctx.cfg.retrieval.quality_mode = "enforce"
    ctx.core = Reviewer()
    past_reads = []
    def search(_query, **kwargs):
        if kwargs.get("only_session_id"):
            return [record("D19:1", "s019", NOISE)]
        past_reads.append(kwargs)
        return [record(*row) for row in ORIGINALS]
    ctx.session_cache = SimpleNamespace(get=lambda *a, **k: {
        "session_id": "s019", "compacted": NOISE,
        "turns": [{"role": "user", "content": NOISE, "turn_idx": 0, "timestamp": "2023-10-22"}],
    })
    ctx.control_plane = SimpleNamespace(search_session_events=search)
    return ctx, frontier, past_reads


def test_activities_continue_to_history_despite_positive_current_review():
    ctx, frontier, past_reads = context()
    result = run_query(ctx, session_id="s019", query=QUESTION)
    assert past_reads and past_reads[0]["exclude_session_id"] == "s019"
    assert "L2" in result.retrieval_metadata.levels_visited
    assert len(frontier.calls) == 1
    for word in ["pottery", "camping", "painting", "swimming"]:
        assert word in result.answer.lower() and word in frontier.calls[0].lower()
    assert "figurines" not in result.answer.lower()


@pytest.mark.parametrize("depth,wrong", [("L1", False), ("L4", True)])
def test_depth_cap_or_unsupported_output_does_not_return_noise(depth, wrong):
    ctx, _, past_reads = context(wrong)
    result = run_query(ctx, session_id="s019", query=QUESTION, max_depth=depth)
    assert "figurines" not in result.answer.lower()
    assert result.answerability == "INSUFFICIENT_EVIDENCE"
    if depth == "L1":
        assert not past_reads
