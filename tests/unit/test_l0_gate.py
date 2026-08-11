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


class FixedClassifier:
    def __init__(self, probability: float, *, raises: bool = False) -> None:
        self.probability = probability
        self.raises = raises
        self.calls = 0

    def predict(self, _query: str) -> float:
        self.calls += 1
        if self.raises:
            raise RuntimeError("broken model")
        return self.probability


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


def test_skip_reason_can_identify_a_request_level_override():
    decision = run_l0_gate(
        "hello",
        classifier=AlwaysClass0Classifier(),
        embed=FakeEmbed(),
        neo4j=FakeNeo(),  # type: ignore[arg-type]
        skip=True,
        skip_reason="request.force_retrieval=true",
    )
    assert decision.decision == "CONTINUE"
    assert decision.reason == "request.force_retrieval=true"


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


def test_off_mode_never_calls_classifier():
    classifier = FixedClassifier(0.99, raises=True)
    decision = run_l0_gate(
        "What is 2 + 2?",
        classifier=classifier,
        embed=FakeEmbed(),
        neo4j=FakeNeo(),  # type: ignore[arg-type]
        classifier_mode="off",
    )
    assert decision.decision == "BYPASS"
    assert classifier.calls == 0


def test_shadow_mode_records_probability_without_routing():
    classifier = FixedClassifier(0.99)
    decision = run_l0_gate(
        "What is 2 + 2?",
        classifier=classifier,
        embed=FakeEmbed(),
        neo4j=FakeNeo(),  # type: ignore[arg-type]
        classifier_mode="shadow",
    )
    assert decision.decision == "BYPASS"
    assert decision.classifier_probability == 0.99
    assert classifier.calls == 1


def test_active_mode_classifier_routes_to_retrieval():
    decision = run_l0_gate(
        "What is 2 + 2?",
        classifier=FixedClassifier(0.8),
        embed=FakeEmbed(),
        neo4j=FakeNeo(),  # type: ignore[arg-type]
        classifier_mode="active",
    )
    assert decision.decision == "CONTINUE"
    assert decision.reason == "classifier:0.80"


def test_active_unavailable_or_broken_classifier_fails_open():
    unavailable = run_l0_gate(
        "What is 2 + 2?",
        classifier=FixedClassifier(0.0),
        embed=FakeEmbed(),
        neo4j=FakeNeo(),  # type: ignore[arg-type]
        classifier_mode="active",
        classifier_available=False,
    )
    broken = run_l0_gate(
        "What is 2 + 2?",
        classifier=FixedClassifier(0.0, raises=True),
        embed=FakeEmbed(),
        neo4j=FakeNeo(),  # type: ignore[arg-type]
        classifier_mode="active",
    )
    assert unavailable.decision == broken.decision == "CONTINUE"
    assert "fail_open" in unavailable.reason
    assert "fail_open" in broken.reason
