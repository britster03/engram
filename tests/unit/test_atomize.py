"""Deterministic pre-link atomization."""

from engram.ingest.atomize import atomize_triplets


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
