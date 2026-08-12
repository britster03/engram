"""Semantic checks used by the read-only LoCoMo corpus auditor."""

from benchmarks.audit_locomo_corpus import _caption_only_ownership_abstract


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
