"""mem:// URI helpers — canonical identifier and filesystem locator for memories (§6.1)."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

from slugify import slugify

MEM_SCHEME = "mem://"
_URI_RE = re.compile(r"^mem://([a-zA-Z0-9][a-zA-Z0-9_\-./]*)/?$")


class UriError(ValueError):
    """Raised for malformed mem:// URIs."""


def is_mem_uri(s: str) -> bool:
    return bool(_URI_RE.match(s))


def normalize_uri(uri: str) -> str:
    """Ensure a mem:// URI is well-formed and ends with '/' for directories."""
    if not uri.startswith(MEM_SCHEME):
        raise UriError(f"not a mem:// URI: {uri!r}")
    body = uri[len(MEM_SCHEME):]
    body = body.strip("/")
    if not body:
        raise UriError("empty mem:// URI")
    if not re.match(r"^[a-zA-Z0-9][a-zA-Z0-9_\-./]*$", body):
        raise UriError(f"invalid characters in URI: {uri!r}")
    return f"{MEM_SCHEME}{body}"


def uri_to_path(uri: str, data_dir: str | Path) -> Path:
    """Translate mem://user/entities/alice/overview.md → data_dir/user/entities/alice/overview.md."""
    cleaned = uri.replace("mem:\\\\", "mem://").replace("mem:\\", "mem://")
    while cleaned.startswith("mem://mem://"):
        cleaned = cleaned[6:]
    cleaned = normalize_uri(cleaned) if is_mem_uri(cleaned) else f"{MEM_SCHEME}{cleaned.lstrip('/')}"
    body = cleaned[len(MEM_SCHEME):]
    return Path(data_dir) / body


def path_to_uri(path: str | Path, data_dir: str | Path) -> str:
    """Inverse of uri_to_path."""
    data_dir = Path(data_dir).resolve()
    path = Path(path).resolve()
    try:
        rel = path.relative_to(data_dir)
    except ValueError as err:
        raise UriError(f"{path} is not under {data_dir}") from err
    return f"{MEM_SCHEME}{rel.as_posix()}"


def parent_uri(uri: str) -> str | None:
    """Return the parent directory URI, or None if already at root."""
    uri = normalize_uri(uri)
    body = uri[len(MEM_SCHEME):]
    if "/" not in body:
        return None
    parent = body.rsplit("/", 1)[0]
    return f"{MEM_SCHEME}{parent}"


def uri_depth(uri: str) -> int:
    """Number of path segments below the root (mem://user → 1, mem://user/x → 2)."""
    uri = normalize_uri(uri)
    return len(uri[len(MEM_SCHEME):].split("/"))


def semantic_filename(entity_or_episode_slug: str, one_line_summary: str, max_len: int = 120) -> str:
    """Build the per-file semantic filename used by §6.1.2:
    {entity_slug}_{summary_slug}.md or {YYYY-MM-DD}_{summary_slug}.md.

    Caller is responsible for prepending a date for episode files.
    """
    entity_part = slugify(entity_or_episode_slug, separator="-", lowercase=True)
    summary_part = slugify(one_line_summary, separator="-", lowercase=True)
    combined = f"{entity_part}_{summary_part}"
    if len(combined) > max_len:
        summary_part = summary_part[: max(8, max_len - len(entity_part) - 1)]
        combined = f"{entity_part}_{summary_part}"
    return f"{combined}.md"


def content_hash(text: str) -> str:
    """SHA-256 hex digest of text. Used for idempotent filesystem writes (§5.4.5)."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def pair_id(session_id: str, user_turn_idx: int, assistant_turn_idx: int) -> str:
    """Deterministic pair_id per §5.2."""
    raw = f"{session_id}||{user_turn_idx}||{assistant_turn_idx}".encode()
    return hashlib.sha256(raw).hexdigest()
