from __future__ import annotations

import pytest

from engram.predicate_registry import (
    Cardinality,
    ConflictPolicy,
    ObjectType,
    normalize_claim_value,
    normalize_extracted_triplets,
)


def test_role_is_scalar_and_superseding() -> None:
    claim = normalize_claim_value("job title", "Director of Quality Engineering")

    assert claim.predicate == "HAS_ROLE"
    assert claim.object_type is ObjectType.ROLE
    assert claim.value == "Director of Quality Engineering"
    assert claim.policy.cardinality is Cardinality.SINGLE
    assert claim.policy.conflict_policy is ConflictPolicy.SUPERSEDE


def test_date_is_typed_scalar() -> None:
    claim = normalize_claim_value("start date", "2026-09-01")

    assert claim.predicate == "EFFECTIVE_DATE"
    assert claim.object_type is ObjectType.DATE
    assert claim.value == "2026-09-01"


def test_manager_and_assistant_manager_have_distinct_canonical_predicates() -> None:
    manager = normalize_claim_value("reports_to", "Rahul")
    assistant = normalize_claim_value("assistant manager", "Noel")

    assert manager.predicate == "HAS_MANAGER"
    assert assistant.predicate == "HAS_ASSISTANT_MANAGER"
    assert manager.object_type is ObjectType.ENTITY
    assert assistant.object_type is ObjectType.ENTITY


def test_unknown_predicate_does_not_manufacture_entity() -> None:
    claim = normalize_claim_value("favorite editor", "Neovim")

    assert claim.predicate == "FAVORITE_EDITOR"
    assert claim.object_type is ObjectType.STRING
    assert claim.policy.cardinality is Cardinality.MULTIPLE
    assert claim.policy.conflict_policy is ConflictPolicy.COEXIST


def test_extraction_is_atomized_typed_and_temporalized_before_commit() -> None:
    claims = normalize_extracted_triplets(
        [
            {
                "subject": "Angie Jones",
                "relation": "job title",
                "object": "Engineer and Director",
                "valid_from": "2026-09-01",
            }
        ]
    )

    assert [claim["object_value"] for claim in claims] == ["Engineer", "Director"]
    assert {claim["predicate"] for claim in claims} == {"HAS_ROLE"}
    assert {claim["object_type"] for claim in claims} == {"ROLE"}
    assert {claim["cardinality"] for claim in claims} == {"SINGLE"}
    assert all(claim["valid_from"] == "2026-09-01T00:00:00" for claim in claims)


def test_extraction_rejects_invalid_temporal_bound() -> None:
    with pytest.raises(ValueError, match="invalid valid_from"):
        normalize_extracted_triplets(
            [
                {
                    "subject": "Angie Jones",
                    "relation": "job title",
                    "object": "Director",
                    "valid_from": "next quarter",
                }
            ]
        )
