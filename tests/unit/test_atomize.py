"""Deterministic pre-link atomization."""

from types import SimpleNamespace

import pytest

from engram.ingest.atomize import atomize_triplets
from engram.ingest.worker import _prepare_extraction


class _Embed:
    def embed(self, text: str) -> list[float]:
        return [float(len(text)), 1.0]


def test_atomize_splits_compound_object_and_preserves_provenance() -> None:
    result = atomize_triplets(
        [
            {
                "subject": "Caroline",
                "relation": "likes",
                "object": "painting and swimming",
                "confidence": 0.9,
                "temporal": {"asserted_at": "2023-05-08"},
            }
        ]
    )
    assert [row["object"] for row in result] == ["painting", "swimming"]
    assert all(row["atomized_from"] == "painting and swimming" for row in result)
    assert all(row["temporal"]["asserted_at"] == "2023-05-08" for row in result)


def test_atomize_is_idempotent() -> None:
    once = atomize_triplets(
        [{"subject": "a", "relation": "likes", "object": "b & c", "confidence": 1.0}]
    )
    assert atomize_triplets(once) == once


@pytest.mark.parametrize(
    ("relation", "value"),
    [
        ("owns", "bowl with a black and white flower design"),
        ("has", "necklace with a cross and a heart"),
        ("interested_in", "creating a more inclusive and understanding world"),
        ("interested_in", "promoting understanding and acceptance of others"),
    ],
)
def test_atomize_preserves_one_described_concept(relation: str, value: str) -> None:
    source = {"subject": "Caroline", "relation": relation, "object": value}

    assert atomize_triplets([source]) == [source]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("love, faith and strength", ["love", "faith", "strength"]),
        (
            "classics, stories from different cultures, and educational books",
            ["classics", "stories from different cultures", "educational books"],
        ),
    ],
)
def test_atomize_splits_explicit_comma_lists(value: str, expected: list[str]) -> None:
    result = atomize_triplets([{"subject": "necklace", "relation": "mentions", "object": value}])

    assert [row["object"] for row in result] == expected
    assert all(row["atomized_from"] == value for row in result)


def test_atomize_preserves_non_list_comma_value() -> None:
    source = {"subject": "user", "relation": "likes", "object": "Portland, Oregon"}

    assert atomize_triplets([source]) == [source]


def test_prepare_extraction_marks_scalar_objects_as_literals() -> None:
    prepared = _prepare_extraction(  # type: ignore[arg-type]
        SimpleNamespace(embed=_Embed()),
        {
            "resolved_text": "Caroline is excited and has empathy.",
            "l0_abstract": "Caroline is excited.",
            "triplets": [
                {
                    "subject": "Caroline",
                    "relation": "feels_excited",
                    "object": "excited",
                    "confidence": 0.9,
                },
                {
                    "subject": "Caroline",
                    "relation": "has_empathy",
                    "object": "true",
                    "confidence": 0.9,
                },
            ],
        },
        {},
    )

    assert [trip["object_kind"] for trip in prepared["triplets"]] == [
        "LITERAL",
        "LITERAL",
    ]


def test_prepare_extraction_drops_caption_only_ownership() -> None:
    prepared = _prepare_extraction(  # type: ignore[arg-type]
        SimpleNamespace(embed=_Embed()),
        {
            "resolved_text": "A stack of bowls was shared in an image.",
            "l0_abstract": "A photo showed a stack of bowls.",
            "triplets": [
                {
                    "subject": "Caroline",
                    "relation": "owns",
                    "object": "stack of bowls",
                    "object_kind": "ENTITY",
                    "confidence": 0.7,
                }
            ],
        },
        {
            "turn_pair": {
                "user": {"content": "The necklace was a gift."},
                "assistant": {
                    "content": "Take a look. [Image caption: a stack of bowls]",
                    "image_caption": "a stack of bowls",
                },
            }
        },
    )

    assert prepared["triplets"] == []


def test_prepare_extraction_keeps_spoken_ownership_with_a_caption() -> None:
    prepared = _prepare_extraction(  # type: ignore[arg-type]
        SimpleNamespace(embed=_Embed()),
        {
            "resolved_text": "Caroline owns a hand-painted bowl.",
            "l0_abstract": "Caroline owns a hand-painted bowl.",
            "triplets": [
                {
                    "subject": "Caroline",
                    "relation": "owns",
                    "object": "hand-painted bowl",
                    "object_kind": "ENTITY",
                    "confidence": 0.95,
                }
            ],
        },
        {
            "turn_pair": {
                "user": {
                    "content": "I own a hand-painted bowl. [Image caption: a hand-painted bowl]",
                    "image_caption": "a hand-painted bowl",
                },
                "assistant": {"content": "That is lovely."},
            }
        },
    )

    assert [trip["object"] for trip in prepared["triplets"]] == ["hand-painted bowl"]


def test_prepare_extraction_repairs_backward_caption_creation() -> None:
    prepared = _prepare_extraction(  # type: ignore[arg-type]
        SimpleNamespace(embed=_Embed()),
        {
            "resolved_text": "Melanie finds making pottery calming and shared a bowl.",
            "l0_abstract": "Melanie makes pottery.",
            "triplets": [
                {
                    "subject": "Melanie",
                    "relation": "created_by",
                    "object": "bowl with a black and white flower design",
                    "object_kind": "ENTITY",
                    "confidence": 0.7,
                }
            ],
        },
        {
            "turn_pair": {
                "user": {"content": "What made you try pottery?", "speaker": "Caroline"},
                "assistant": {
                    "content": "Making it is calming. Look at this!",
                    "speaker": "Melanie",
                    "image_caption": "a bowl with a black and white flower design",
                },
            }
        },
    )

    assert prepared["triplets"] == [
        {
            "subject": "bowl with a black and white flower design",
            "relation": "created_by",
            "object": "Melanie",
            "object_kind": "ENTITY",
            "confidence": 0.7,
            "explicit_correction": False,
            "relation_normalized": True,
        }
    ]


def test_prepare_extraction_maps_gift_provenance_without_inventing_creation() -> None:
    prepared = _prepare_extraction(  # type: ignore[arg-type]
        SimpleNamespace(embed=_Embed()),
        {
            "resolved_text": "The necklace was a gift from Caroline's grandma.",
            "l0_abstract": "Caroline's grandma gave her a necklace.",
            "triplets": [
                {
                    "subject": "necklace",
                    "relation": "created_by",
                    "object": "Caroline's grandma",
                    "object_kind": "ENTITY",
                    "confidence": 0.7,
                }
            ],
        },
        {
            "turn_pair": {
                "user": {
                    "content": "This necklace was a gift from my grandma.",
                    "speaker": "Caroline",
                },
                "assistant": {"content": "That is special.", "speaker": "Melanie"},
            }
        },
    )

    assert prepared["triplets"][0]["relation"] == "gifted_by"


def test_prepare_extraction_drops_unsupported_creation() -> None:
    prepared = _prepare_extraction(  # type: ignore[arg-type]
        SimpleNamespace(embed=_Embed()),
        {
            "resolved_text": "A necklace was discussed.",
            "l0_abstract": "A necklace was discussed.",
            "triplets": [
                {
                    "subject": "necklace",
                    "relation": "created_by",
                    "object": "Caroline's grandma",
                    "confidence": 0.7,
                }
            ],
        },
        {"turn_pair": {"user": {"content": "I like this necklace."}}},
    )

    assert prepared["triplets"] == []
