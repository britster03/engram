from pathlib import Path

import pytest

from engram import uri as uri_mod


def test_uri_to_path(tmp_path: Path):
    p = uri_mod.uri_to_path("mem://user/entities/alice/overview.md", tmp_path)
    assert p == tmp_path / "user" / "entities" / "alice" / "overview.md"


def test_path_to_uri_roundtrip(tmp_path: Path):
    uri = "mem://user/entities/alice/overview.md"
    path = uri_mod.uri_to_path(uri, tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x")
    assert uri_mod.path_to_uri(path, tmp_path) == uri


def test_parent_uri():
    assert uri_mod.parent_uri("mem://user/entities/alice/") == "mem://user/entities"
    assert uri_mod.parent_uri("mem://user") is None


def test_normalize_rejects_bad_uris():
    with pytest.raises(uri_mod.UriError):
        uri_mod.normalize_uri("http://example.com")
    with pytest.raises(uri_mod.UriError):
        uri_mod.normalize_uri("mem://")


def test_semantic_filename_truncates():
    name = uri_mod.semantic_filename(
        "alice",
        "a very long summary " * 20,
        max_len=60,
    )
    assert name.endswith(".md")
    assert len(name) <= 63


def test_pair_id_deterministic():
    assert uri_mod.pair_id("sess", 1, 2) == uri_mod.pair_id("sess", 1, 2)
    assert uri_mod.pair_id("sess", 1, 2) != uri_mod.pair_id("sess", 1, 3)


def test_content_hash_stable():
    assert uri_mod.content_hash("foo") == uri_mod.content_hash("foo")
    assert uri_mod.content_hash("foo") != uri_mod.content_hash("bar")
