import pytest

from engram import frontmatter as fm


def test_parse_basic():
    text = (
        "---\n"
        "id: 4b9f-uuid\n"
        "node_type: ENTITY\n"
        "status: ACTIVE\n"
        "created_at: 2026-04-12T10:30:00Z\n"
        "schema_version: 1\n"
        "---\n"
        "L0 abstract sentence.\n\nBody goes here.\n"
    )
    m = fm.parse(text)
    assert m.frontmatter["id"] == "4b9f-uuid"
    assert m.frontmatter["node_type"] == "ENTITY"
    assert m.body.startswith("L0 abstract sentence.")


def test_roundtrip_preserves_keys():
    m = fm.MemoryFile(
        frontmatter={
            "id": "abc",
            "node_type": "FACT",
            "status": "ACTIVE",
            "created_at": "2026-04-12T10:30:00Z",
            "schema_version": 1,
        },
        body="body",
    )
    out = m.serialize()
    reparsed = fm.parse(out)
    assert reparsed.frontmatter == m.frontmatter
    assert reparsed.body.strip() == "body"


def test_rejects_missing_delimiter():
    with pytest.raises(fm.FrontmatterError):
        fm.parse("no frontmatter here")


def test_validate_required_keys():
    with pytest.raises(fm.FrontmatterError):
        fm.validate_required_keys({"id": "x", "node_type": "ENTITY"})

    fm.validate_required_keys(
        {
            "id": "x",
            "node_type": "ENTITY",
            "status": "ACTIVE",
            "created_at": "now",
            "schema_version": 1,
        }
    )


def test_validate_rejects_bad_enum():
    with pytest.raises(fm.FrontmatterError):
        fm.validate_required_keys(
            {
                "id": "x",
                "node_type": "BOGUS",
                "status": "ACTIVE",
                "created_at": "now",
                "schema_version": 1,
            }
        )
