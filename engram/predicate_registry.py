"""Versioned predicate normalization and value typing policy.

The model may suggest arbitrary relation labels.  Canonical storage must never
infer cardinality, temporal behavior, or entity/scalar shape from those labels
inside the conflict resolver.  This registry is therefore the single policy
boundary applied before a claim enters PostgreSQL.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

PREDICATE_POLICY_VERSION = "2026-09-09.v1"


class ObjectType(StrEnum):
    ENTITY = "ENTITY"
    STRING = "STRING"
    ROLE = "ROLE"
    NUMBER = "NUMBER"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    DATETIME = "DATETIME"


class Cardinality(StrEnum):
    SINGLE = "SINGLE"
    MULTIPLE = "MULTIPLE"


class TemporalBehavior(StrEnum):
    CURRENT = "CURRENT"
    INTERVAL = "INTERVAL"
    TIMELESS = "TIMELESS"


class ConflictPolicy(StrEnum):
    SUPERSEDE = "SUPERSEDE"
    COEXIST = "COEXIST"
    FLAG = "FLAG"


@dataclass(frozen=True)
class PredicatePolicy:
    canonical_predicate: str
    object_type: ObjectType
    cardinality: Cardinality
    temporal_behavior: TemporalBehavior
    conflict_policy: ConflictPolicy


@dataclass(frozen=True)
class NormalizedClaimValue:
    predicate: str
    object_type: ObjectType
    value: Any
    policy: PredicatePolicy
    policy_version: str = PREDICATE_POLICY_VERSION


_POLICIES: dict[str, PredicatePolicy] = {
    "HAS_NAME": PredicatePolicy(
        "HAS_NAME",
        ObjectType.STRING,
        Cardinality.SINGLE,
        TemporalBehavior.TIMELESS,
        ConflictPolicy.SUPERSEDE,
    ),
    "HAS_ROLE": PredicatePolicy(
        "HAS_ROLE",
        ObjectType.ROLE,
        Cardinality.SINGLE,
        TemporalBehavior.INTERVAL,
        ConflictPolicy.SUPERSEDE,
    ),
    "HAS_BIRTH_DATE": PredicatePolicy(
        "HAS_BIRTH_DATE",
        ObjectType.DATE,
        Cardinality.SINGLE,
        TemporalBehavior.TIMELESS,
        ConflictPolicy.FLAG,
    ),
    "EFFECTIVE_DATE": PredicatePolicy(
        "EFFECTIVE_DATE",
        ObjectType.DATE,
        Cardinality.MULTIPLE,
        TemporalBehavior.TIMELESS,
        ConflictPolicy.COEXIST,
    ),
    "WORKS_AT": PredicatePolicy(
        "WORKS_AT",
        ObjectType.ENTITY,
        Cardinality.SINGLE,
        TemporalBehavior.INTERVAL,
        ConflictPolicy.SUPERSEDE,
    ),
    "HAS_MANAGER": PredicatePolicy(
        "HAS_MANAGER",
        ObjectType.ENTITY,
        Cardinality.SINGLE,
        TemporalBehavior.INTERVAL,
        ConflictPolicy.SUPERSEDE,
    ),
    "HAS_ASSISTANT_MANAGER": PredicatePolicy(
        "HAS_ASSISTANT_MANAGER",
        ObjectType.ENTITY,
        Cardinality.SINGLE,
        TemporalBehavior.INTERVAL,
        ConflictPolicy.SUPERSEDE,
    ),
    "MEMBER_OF": PredicatePolicy(
        "MEMBER_OF",
        ObjectType.ENTITY,
        Cardinality.MULTIPLE,
        TemporalBehavior.INTERVAL,
        ConflictPolicy.COEXIST,
    ),
    "LOCATED_IN": PredicatePolicy(
        "LOCATED_IN",
        ObjectType.ENTITY,
        Cardinality.SINGLE,
        TemporalBehavior.INTERVAL,
        ConflictPolicy.SUPERSEDE,
    ),
    "KNOWS": PredicatePolicy(
        "KNOWS",
        ObjectType.ENTITY,
        Cardinality.MULTIPLE,
        TemporalBehavior.INTERVAL,
        ConflictPolicy.COEXIST,
    ),
    "PREFERS": PredicatePolicy(
        "PREFERS",
        ObjectType.STRING,
        Cardinality.MULTIPLE,
        TemporalBehavior.INTERVAL,
        ConflictPolicy.COEXIST,
    ),
    "ATTENDS_SCHOOL": PredicatePolicy(
        "ATTENDS_SCHOOL",
        ObjectType.STRING,
        Cardinality.MULTIPLE,
        TemporalBehavior.TIMELESS,
        ConflictPolicy.COEXIST,
    ),
}

_ALIASES = {
    "NAME": "HAS_NAME",
    "IS_NAMED": "HAS_NAME",
    "ROLE": "HAS_ROLE",
    "JOB_TITLE": "HAS_ROLE",
    "TITLE": "HAS_ROLE",
    "POSITION": "HAS_ROLE",
    "BECAME": "HAS_ROLE",
    "BIRTH_DATE": "HAS_BIRTH_DATE",
    "DATE_OF_BIRTH": "HAS_BIRTH_DATE",
    "START_DATE": "EFFECTIVE_DATE",
    "ON_DATE": "EFFECTIVE_DATE",
    "WORKS_FOR": "WORKS_AT",
    "EMPLOYED_BY": "WORKS_AT",
    "MANAGER": "HAS_MANAGER",
    "REPORTS_TO": "HAS_MANAGER",
    "ASSISTANT_MANAGER": "HAS_ASSISTANT_MANAGER",
    "DEPUTY_MANAGER": "HAS_ASSISTANT_MANAGER",
    "BELONGS_TO": "MEMBER_OF",
    "LIVES_IN": "LOCATED_IN",
    "BASED_IN": "LOCATED_IN",
    "PREFERENCE": "PREFERS",
    "LIKES": "PREFERS",
    "ATTENDS": "ATTENDS_SCHOOL",
    "ATTENDED": "ATTENDS_SCHOOL",
    "STUDIED_AT": "ATTENDS_SCHOOL",
    "SCHOOL_NAME_IS": "ATTENDS_SCHOOL",
}

_DEFAULT_POLICY = PredicatePolicy(
    canonical_predicate="HAS_ATTRIBUTE",
    object_type=ObjectType.STRING,
    cardinality=Cardinality.MULTIPLE,
    temporal_behavior=TemporalBehavior.INTERVAL,
    # Unknown predicates are deliberately scalar and multi-valued. Different
    # values therefore coexist; flagging each later value as a conflict would
    # contradict MULTIPLE cardinality and turn ordinary actions (CREATES,
    # REVIEWS, REFERENCES) into false conflicts.
    conflict_policy=ConflictPolicy.COEXIST,
)

_COMPOUND_OBJECT = re.compile(r"\s+(?:and|&)\s+", re.IGNORECASE)
_NARRATIVE_COORDINATE = re.compile(
    r"^(?:(?:story|conversation)\s+)?(?:event|turn|message|record|chapter)[ _-]*\d+$",
    re.IGNORECASE,
)


def normalize_predicate(raw_predicate: str) -> PredicatePolicy:
    """Return the stable policy for an extractor-provided relation label."""

    token = re.sub(r"[^A-Z0-9]+", "_", raw_predicate.strip().upper()).strip("_")
    canonical = _ALIASES.get(token, token)
    if canonical in _POLICIES:
        return _POLICIES[canonical]
    # Unknown predicates remain distinguishable while using conservative
    # scalar/conflict semantics. They never manufacture an entity.
    return PredicatePolicy(
        canonical_predicate=canonical or _DEFAULT_POLICY.canonical_predicate,
        object_type=_DEFAULT_POLICY.object_type,
        cardinality=_DEFAULT_POLICY.cardinality,
        temporal_behavior=_DEFAULT_POLICY.temporal_behavior,
        conflict_policy=_DEFAULT_POLICY.conflict_policy,
    )


def normalize_claim_value(raw_predicate: str, raw_value: Any) -> NormalizedClaimValue:
    """Normalize predicate and coerce its object before conflict resolution."""

    policy = normalize_predicate(raw_predicate)
    value = _coerce_value(policy.object_type, raw_value)
    return NormalizedClaimValue(
        predicate=policy.canonical_predicate,
        object_type=policy.object_type,
        value=value,
        policy=policy,
    )


def normalize_extracted_triplets(
    triplets: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Atomize and type extractor output before the canonical transaction.

    The returned records contain the policy decision explicitly, allowing the
    commit activity to perform entity resolution and conflict application
    without asking the model or reinterpreting a raw predicate.
    """

    normalized_triplets: list[dict[str, Any]] = []
    for index, raw in enumerate(triplets):
        subject = _first(raw, "subject", "subject_name", "subject_entity")
        if _is_narrative_coordinate(subject):
            # Sequence labels locate a fact inside source material; they are
            # not durable domain entities. The EPISODE node already preserves
            # this context, so persisting the triplet would create noisy nodes
            # such as "story event 102" and "chapter 3" in the graph.
            continue
        predicate = _first(raw, "predicate", "relation", "property")
        if predicate is None or not str(predicate).strip():
            raise ValueError(f"triplet {index} is missing predicate/relation")
        raw_object = _first(raw, "object_value", "object", "value", "object_name")
        object_reference = _first(raw, "object_entity_id", "object_memory_id", "object_id")
        if raw_object is None and object_reference is None:
            raise ValueError(f"triplet {index} is missing object")
        object_for_policy = raw_object if raw_object is not None else object_reference
        if isinstance(object_for_policy, Mapping):
            object_for_policy = _first(
                object_for_policy,
                "value",
                "name",
                "label",
                "text",
                "id",
            )
        pieces: list[Any] = [object_for_policy]
        if isinstance(object_for_policy, str):
            split = [piece.strip(" ,.;:") for piece in _COMPOUND_OBJECT.split(object_for_policy)]
            split = [piece for piece in split if piece]
            if len(split) > 1:
                pieces = split

        for piece in pieces:
            typed = normalize_claim_value(str(predicate), piece)
            candidate = dict(raw)
            candidate["raw_predicate"] = str(predicate)
            candidate["predicate"] = typed.predicate
            candidate["object_type"] = typed.object_type.value
            candidate["predicate_policy_version"] = typed.policy_version
            candidate["cardinality"] = typed.policy.cardinality.value
            candidate["temporal_behavior"] = typed.policy.temporal_behavior.value
            candidate["conflict_policy"] = typed.policy.conflict_policy.value
            candidate["typed"] = True
            if typed.object_type is ObjectType.ENTITY:
                candidate["object"] = typed.value
                candidate.pop("object_value", None)
            else:
                candidate["object_value"] = typed.value
            for key in ("valid_from", "valid_until", "asserted_at"):
                if candidate.get(key) is not None:
                    candidate[key] = _normalise_temporal_bound(candidate[key], key=key)
            confidence = candidate.get("confidence")
            if confidence is not None and not 0 <= float(confidence) <= 1:
                raise ValueError(f"triplet {index} confidence must be between 0 and 1")
            normalized_triplets.append(candidate)
    return normalized_triplets


def _is_narrative_coordinate(value: Any) -> bool:
    if isinstance(value, Mapping):
        value = _first(value, "value", "name", "label", "text")
    if value is None:
        return False
    normalized = " ".join(str(value).replace("_", " ").split())
    return bool(_NARRATIVE_COORDINATE.fullmatch(normalized))


def _first(value: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if value.get(key) is not None:
            return value[key]
    return None


def _normalise_temporal_bound(raw_value: Any, *, key: str) -> str:
    if isinstance(raw_value, datetime):
        parsed = raw_value
    elif isinstance(raw_value, date):
        parsed = datetime(raw_value.year, raw_value.month, raw_value.day)
    else:
        value = str(raw_value).strip()
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as err:
            try:
                parsed_date = date.fromisoformat(value)
            except ValueError:
                raise ValueError(f"invalid {key}: {raw_value!r}") from err
            parsed = datetime(parsed_date.year, parsed_date.month, parsed_date.day)
    return parsed.isoformat()


def _coerce_value(object_type: ObjectType, raw_value: Any) -> Any:
    if object_type is ObjectType.ENTITY:
        value = str(raw_value).strip()
        if not value:
            raise ValueError("entity claim object cannot be empty")
        return value
    if object_type in {ObjectType.STRING, ObjectType.ROLE}:
        value = str(raw_value).strip()
        if not value:
            raise ValueError("scalar claim object cannot be empty")
        return value
    if object_type is ObjectType.BOOLEAN:
        if isinstance(raw_value, bool):
            return raw_value
        token = str(raw_value).strip().casefold()
        if token in {"true", "yes", "1"}:
            return True
        if token in {"false", "no", "0"}:
            return False
        raise ValueError(f"invalid boolean value: {raw_value!r}")
    if object_type is ObjectType.NUMBER:
        try:
            return Decimal(str(raw_value))
        except InvalidOperation as err:
            raise ValueError(f"invalid numeric value: {raw_value!r}") from err
    if object_type is ObjectType.DATE:
        if isinstance(raw_value, datetime):
            return raw_value.date().isoformat()
        if isinstance(raw_value, date):
            return raw_value.isoformat()
        return date.fromisoformat(str(raw_value).strip()).isoformat()
    if object_type is ObjectType.DATETIME:
        if isinstance(raw_value, datetime):
            return raw_value.isoformat()
        return datetime.fromisoformat(str(raw_value).strip().replace("Z", "+00:00")).isoformat()
    return raw_value


__all__ = [
    "PREDICATE_POLICY_VERSION",
    "Cardinality",
    "ConflictPolicy",
    "NormalizedClaimValue",
    "ObjectType",
    "PredicatePolicy",
    "TemporalBehavior",
    "normalize_claim_value",
    "normalize_extracted_triplets",
    "normalize_predicate",
]
