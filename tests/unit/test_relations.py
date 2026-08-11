"""Controlled relation-vocabulary normalization."""

from __future__ import annotations

from engram.relations import RelationVocabulary


class UnexpectedEmbedder:
    def embed(self, _text: str) -> list[float]:
        raise AssertionError("deterministic aliases must not invoke embeddings")


def test_employment_paraphrases_share_one_canonical_relation() -> None:
    vocab = RelationVocabulary()
    embed = UnexpectedEmbedder()

    assert vocab.canonicalise("works_at", embed) == "works_at"
    assert vocab.canonicalise("employed_by", embed) == "works_at"
    assert vocab.canonicalise("employedBy", embed) == "works_at"
    assert vocab.canonicalise("employed by", embed) == "works_at"


def test_past_employment_remains_distinct() -> None:
    vocab = RelationVocabulary()
    assert vocab.canonicalise("worked_at", UnexpectedEmbedder()) == "worked_at"


def test_prompt_working_on_relation_has_a_deterministic_canonical_label() -> None:
    vocab = RelationVocabulary()
    embed = UnexpectedEmbedder()

    assert vocab.canonicalise("works_on", embed) == "works_on"
    assert vocab.canonicalise("working_on", embed) == "works_on"
    assert vocab.canonicalise("working on", embed) == "works_on"
