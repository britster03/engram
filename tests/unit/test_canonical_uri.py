from __future__ import annotations

from uuid import UUID

import pytest

from engram.uri import (
    UriError,
    canonical_memory_id,
    canonical_memory_uri,
    is_legacy_file_uri,
)


def test_canonical_uri_round_trip() -> None:
    memory_id = UUID("b9f931d6-2e85-4384-856f-d8334790d436")

    uri = canonical_memory_uri(memory_id)

    assert uri == "mem://memory/b9f931d6-2e85-4384-856f-d8334790d436"
    assert canonical_memory_id(uri) == memory_id


def test_file_shaped_uri_is_legacy() -> None:
    assert is_legacy_file_uri("mem://user/entities/angie-jones.md")
    assert not is_legacy_file_uri("mem://memory/b9f931d6-2e85-4384-856f-d8334790d436")


def test_noncanonical_uri_cannot_be_parsed_as_memory_id() -> None:
    with pytest.raises(UriError):
        canonical_memory_id("mem://projects/engram")
