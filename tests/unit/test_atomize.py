"""Deterministic pre-link atomization."""

from types import SimpleNamespace

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
