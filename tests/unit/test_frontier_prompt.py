from engram.models.providers.ollama_cloud import _FRONTIER_SYSTEM as OLLAMA_PROMPT
from engram.models.providers.openai_compat import _FRONTIER_SYSTEM as OPENAI_PROMPT
from engram.retrieval.orchestrator import _annotate


def test_frontier_prompts_require_concise_answers_and_safe_abstention() -> None:
    for prompt in (OLLAMA_PROMPT, OPENAI_PROMPT):
        assert "shortest span" in prompt
        assert "No information available." in prompt
        assert "Resolve relative dates" in prompt


def test_memory_annotation_exposes_bounded_temporal_context() -> None:
    annotation = _annotate(
        "ACTIVE",
        0.9,
        "mem://user/episodes/example.md",
        level="L1",
        temporal={
            "asserted_at": "2023-05-08T13:56:00",
            "valid_from": "2022-01-01",
            "phrase": "must not be exposed in the annotation",
        },
    )
    assert "asserted_at: 2023-05-08T13:56:00" in annotation
    assert "valid_from: 2022-01-01" in annotation
    assert "must not be exposed" not in annotation
