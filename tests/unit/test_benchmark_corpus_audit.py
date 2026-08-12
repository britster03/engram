"""Semantic checks used by the read-only LoCoMo corpus auditor."""

from benchmarks.audit_locomo_corpus import (
    _caption_only_creation,
    _caption_only_creation_text,
    _caption_only_ownership_abstract,
    _caption_only_ownership_text,
)


def test_caption_object_in_abstract_but_only_in_caption_is_unsafe() -> None:
    assert _caption_only_ownership_abstract(
        "Caroline owns a necklace with a cross and a heart.",
        "caroline said take a look at this.",
        "a person holding a necklace with a cross and a heart",
    )


def test_unrelated_caption_does_not_invalidate_spoken_state() -> None:
    assert not _caption_only_ownership_abstract(
        "Melanie has been married for five years.",
        "melanie said she has been married for five years.",
        "a bride in a wedding dress holding a bouquet",
    )


def test_spoken_ownership_is_not_caption_only() -> None:
    assert not _caption_only_ownership_abstract(
        "Caroline owns a necklace.",
        "caroline said this necklace is special to me.",
        "a person holding a necklace",
    )


def test_varied_caption_ownership_text_is_unsafe() -> None:
    assert _caption_only_ownership_text(
        "Caroline owns a necklace featuring a cross and a heart.",
        "take a look at this. love the necklace, any special meaning?",
        "a person holding a necklace with a cross and a heart",
        {"caroline", "melanie"},
    )


def test_explicit_spoken_ownership_supports_captioned_item() -> None:
    assert not _caption_only_ownership_text(
        "Caroline owns a necklace featuring a heart.",
        "i own this necklace and wear it daily.",
        "a necklace featuring a heart",
        {"caroline"},
    )


def test_generic_making_cue_does_not_support_captioned_artifact_creation() -> None:
    assert _caption_only_creation(
        "bowl with a black and white flower design",
        "melanie",
        "making it is calming. look at this!",
        "a bowl with a black and white flower design",
        {"caroline", "melanie"},
    )


def test_explicit_deictic_creation_supports_captioned_artifact() -> None:
    assert not _caption_only_creation(
        "bowl with a flower design",
        "melanie",
        "i made this.",
        "a bowl with a flower design",
        {"melanie"},
    )


def test_caption_only_creation_in_retrieval_text_is_unsafe() -> None:
    assert _caption_only_creation_text(
        "Melanie shared a bowl with a black and white flower design that she made.",
        "making pottery is calming. look at this!",
        "a bowl with a black and white flower design",
        {"caroline", "melanie"},
    )


def test_made_feel_sentence_is_not_a_creation_assertion() -> None:
    assert not _caption_only_creation_text(
        "The painting made Caroline happy.",
        "that made me happy.",
        "a colorful painting",
        {"caroline"},
    )
