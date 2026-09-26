"""Tests for the L0 regex OR-gate and memory-hit fallback (§3.1)."""

from dataclasses import dataclass

from engram.retrieval.l0_gate import AlwaysClass0Classifier, run_l0_gate


@dataclass
class FakeEmbed:
    def embed(self, text: str) -> list[float]:
        return [0.0] * 384


class FakeNeo:
    def __init__(self, hit: dict | None = None) -> None:
        self._hit = hit

    def vector_search(self, *_args, **_kwargs) -> list[dict]:
        return [self._hit] if self._hit else []


def test_skip_returns_continue_unconditionally():
    decision = run_l0_gate(
        "hello",
        classifier=AlwaysClass0Classifier(),
        embed=FakeEmbed(),
        neo4j=FakeNeo(),  # type: ignore[arg-type]
        skip=True,
    )
    assert decision.decision == "CONTINUE"
    assert "l0_skip" in decision.reason


def test_deixis_matches_regex():
    decision = run_l0_gate(
        "You mentioned the project yesterday — what's the status?",
        classifier=AlwaysClass0Classifier(),
        embed=FakeEmbed(),
        neo4j=FakeNeo(),  # type: ignore[arg-type]
    )
    assert decision.decision == "CONTINUE"
    assert decision.reason.startswith("regex:")


def test_possessive_matches_regex():
    decision = run_l0_gate(
        "what is my wife's birthday?",
        classifier=AlwaysClass0Classifier(),
        embed=FakeEmbed(),  # would be 0 similarity
        neo4j=FakeNeo(),  # type: ignore[arg-type]
    )
    assert decision.decision == "CONTINUE"


def test_memory_hit_fallback_overrides():
    hit = {"source_uri": "mem://x", "score": 0.9, "l0_abstract": "a fact"}
    decision = run_l0_gate(
        "what is the capital of france",
        classifier=AlwaysClass0Classifier(),
        embed=FakeEmbed(),
        neo4j=FakeNeo(hit=hit),  # type: ignore[arg-type]
        memory_hit_threshold=0.75,
    )
    assert decision.decision == "CONTINUE"
    assert decision.reason.startswith("memory_hit:")
    assert decision.memory_hit == hit


def test_bypass_on_standalone_query():
    decision = run_l0_gate(
        "What is 2 + 2?",
        classifier=AlwaysClass0Classifier(),
        embed=FakeEmbed(),
        neo4j=FakeNeo(),  # type: ignore[arg-type]
    )
    assert decision.decision == "BYPASS"
    assert decision.reason == "no_match"
