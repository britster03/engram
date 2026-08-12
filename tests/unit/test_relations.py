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


def test_benchmark_relations_are_controlled_without_embedding_guesswork() -> None:
    vocab = RelationVocabulary()
    embed = UnexpectedEmbedder()

    for relation in (
        "attended",
        "contains",
        "created_by",
        "created_on",
        "gifted_by",
        "from_country",
        "helps_with",
        "is_a",
        "knows_for",
        "makes_feel",
        "married_for",
        "moved_from",
        "motivated_by",
        "participated_in",
        "received_on",
        "reminds_of",
        "symbolizes",
        "undergoing",
    ):
        assert vocab.canonicalise(relation, embed) == relation

    assert vocab.canonicalise("took part in", embed) == "participated_in"
    assert vocab.canonicalise("gifted_on", embed) == "received_on"
    assert vocab.canonicalise("given by", embed) == "gifted_by"
    assert vocab.canonicalise("moved from", embed) == "moved_from"
    assert vocab.canonicalise("stands for", embed) == "symbolizes"
    assert vocab.canonicalise("reminder_of", embed) == "reminds_of"
