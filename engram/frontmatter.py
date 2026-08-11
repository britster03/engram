"""YAML frontmatter parser/writer for memory `.md` files (§5.4.5.1)."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

import yaml

_FM_OPEN = "---\n"
_FM_CLOSE = "\n---\n"


class FrontmatterError(ValueError):
    """Raised on malformed frontmatter."""


def canonicalize_body(body: str) -> str:
    """Return the stable representation used for storage and content hashes.

    Frontmatter delimiters already supply the line break before the body. A
    memory body therefore has no leading blank delimiter lines and exactly
    one terminal newline. Canonicalizing before both serialization and
    hashing prevents replay identity from depending on incidental newlines.
    """
    normalized = body.lstrip("\n").rstrip("\n")
    return f"{normalized}\n" if normalized else ""


def content_hash(body: str) -> str:
    """Hash the canonical logical body stored beneath frontmatter."""
    return hashlib.sha256(canonicalize_body(body).encode("utf-8")).hexdigest()


@dataclass
class MemoryFile:
    frontmatter: dict[str, Any] = field(default_factory=dict)
    body: str = ""

    def serialize(self) -> str:
        fm = yaml.safe_dump(self.frontmatter, sort_keys=False, allow_unicode=True).rstrip()
        return f"---\n{fm}\n---\n{canonicalize_body(self.body)}"


def parse(text: str) -> MemoryFile:
    if not text.startswith(_FM_OPEN):
        raise FrontmatterError("file does not start with '---\\n'")
    end = text.find("\n---\n", len(_FM_OPEN))
    if end == -1:
        end = text.find("\n---", len(_FM_OPEN))
        if end == -1:
            raise FrontmatterError("missing closing '---' delimiter")
        body_start = end + len("\n---")
    else:
        body_start = end + len("\n---\n")
    fm_text = text[len(_FM_OPEN) : end]
    try:
        fm = yaml.safe_load(fm_text) or {}
    except yaml.YAMLError as err:
        raise FrontmatterError(f"invalid YAML in frontmatter: {err}") from err
    if not isinstance(fm, dict):
        raise FrontmatterError("frontmatter must be a YAML mapping")
    return MemoryFile(frontmatter=fm, body=text[body_start:].lstrip("\n"))


def validate_required_keys(fm: dict[str, Any]) -> None:
    """Enforce required core properties per §6.2 before Step 6 (§6.3)."""
    required = {"id", "node_type", "status", "created_at", "schema_version"}
    missing = required - fm.keys()
    if missing:
        raise FrontmatterError(f"frontmatter missing required keys: {sorted(missing)}")
    if fm["node_type"] not in {"ENTITY", "EVENT", "FACT", "DOCUMENT", "DIRECTORY", "SESSION_SUMMARY"}:
        raise FrontmatterError(f"invalid node_type: {fm['node_type']!r}")
    if fm["status"] not in {"ACTIVE", "HISTORICAL", "LOW_CONFIDENCE"}:
        raise FrontmatterError(f"invalid status: {fm['status']!r}")


# ----------------------------------------------------------------------
# Reserved-key metadata validation (§6.3)
# ----------------------------------------------------------------------

# metadata MAP reserved keys: if present, must match the declared type.
# Unreserved keys are permitted but not schema-validated.
_RESERVED_KEY_TYPES: dict[str, tuple[type, ...]] = {
    "schema_version": (int,),
    "temporal.asserted_at": (str,),       # ISO 8601
    "temporal.valid_from": (str, type(None)),
    "temporal.valid_until": (str, type(None)),
    "temporal.phrase": (str,),
    "normalize.canonical_name": (str,),
    "normalize.aliases": (list,),
    "provenance.extractor": (str,),
    "provenance.confidence": (float, int),
    "provenance.ingest_event_id": (str,),
}


def validate_metadata(fm: dict[str, Any]) -> None:
    """Validate reserved metadata keys against §6.3. Raises on violation.

    `schema_version` is a top-level key; temporal/normalize/provenance are
    nested maps. Any reserved key that is present must carry the declared
    type; unreserved keys are allowed.
    """
    # Top-level schema_version
    if "schema_version" in fm and not isinstance(fm["schema_version"], int):
        raise FrontmatterError("schema_version must be an int")

    for nested_key, _ in list(_RESERVED_KEY_TYPES.items()):
        if "." not in nested_key:
            continue
        namespace, leaf = nested_key.split(".", 1)
        namespace_map = fm.get(namespace)
        if namespace_map is None:
            continue
        if not isinstance(namespace_map, dict):
            raise FrontmatterError(f"{namespace} must be a mapping")
        if leaf in namespace_map:
            allowed = _RESERVED_KEY_TYPES[nested_key]
            if not isinstance(namespace_map[leaf], allowed):
                raise FrontmatterError(
                    f"reserved key {nested_key!r} must be {allowed}, "
                    f"got {type(namespace_map[leaf]).__name__}"
                )
