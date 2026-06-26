"""Breadth-first tree rendering under a token budget (§7.6).

Used as input to L1 planning so the Core Model knows the coarse shape of the
filesystem before issuing `ls` / `overview` commands.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from engram import frontmatter as fm_mod
from engram.frontmatter import FrontmatterError
from engram.storage.filesystem import FilesystemStore
from engram.uri import path_to_uri

MIN_NODE_TOKENS = 20


@dataclass
class _Node:
    uri: str
    path: Path
    is_dir: bool
    abstract: str = ""
    depth: int = 0


def render_tree(
    fs: FilesystemStore,
    *,
    root_uri: str = "mem://",
    max_tokens: int = 2048,
    max_display_depth: int = 4,
) -> str:
    """Produce a compact breadth-first tree sketch under `max_tokens`.

    Always renders at least depth 1-2. Emits ellipsis markers for truncated
    subtrees so the Core Model knows what is missing.
    """
    root_path = fs.data_dir
    if not root_path.exists():
        return "(filesystem is empty)"
    # Level-0 root
    nodes_by_level: list[list[_Node]] = [[_Node(uri=root_uri, path=root_path, is_dir=True, depth=0)]]
    # Expand BFS
    for depth in range(max_display_depth):
        parents = nodes_by_level[-1]
        children: list[_Node] = []
        for parent in parents:
            if not parent.is_dir:
                continue
            try:
                items = sorted(parent.path.iterdir())
            except FileNotFoundError:
                continue
            for child in items:
                if child.name.startswith("."):
                    continue
                uri = path_to_uri(child, fs.data_dir)
                node = _Node(
                    uri=uri,
                    path=child,
                    is_dir=child.is_dir(),
                    depth=depth + 1,
                    abstract=_abstract_for(child),
                )
                children.append(node)
        if not children:
            break
        nodes_by_level.append(children)

    # Budget each level
    remaining = max_tokens
    lines: list[str] = []
    for depth, level in enumerate(nodes_by_level):
        if depth == 0:
            lines.append(f"{_indent(0)}{root_uri}  (root)")
            continue
        count = len(level) or 1
        per_node_budget = max(MIN_NODE_TOKENS, remaining // count)
        trimmed = 0
        for node in level:
            abstract = node.abstract.strip().replace("\n", " ")
            if per_node_budget < MIN_NODE_TOKENS:
                trimmed += 1
                continue
            tokens_for = min(per_node_budget, 80)
            if len(abstract) > tokens_for * 4:
                abstract = abstract[: tokens_for * 4].rsplit(" ", 1)[0] + "…"
            tag = "/" if node.is_dir else ""
            lines.append(f"{_indent(depth)}{node.uri}{tag}  — {abstract or '(no abstract)'}")
            remaining -= max(MIN_NODE_TOKENS, len(abstract) // 4)
            if remaining <= 0:
                trimmed = len(level) - (level.index(node) + 1)
                break
        if trimmed > 0:
            lines.append(
                f"{_indent(depth)}… ({trimmed} more at depth {depth}, use `ls` or `overview`)"
            )
            break
        if remaining <= 0:
            break
    return "\n".join(lines)


def _indent(depth: int) -> str:
    return "  " * depth


def _abstract_for(path: Path) -> str:
    """Pull the L0 abstract line out of a memory file's body, or return ''."""
    if path.is_dir():
        return ""
    if not path.name.endswith(".md"):
        return path.name
    try:
        text = path.read_text(encoding="utf-8")
        mf = fm_mod.parse(text)
    except (FrontmatterError, OSError):
        return ""
    body = mf.body.strip()
    if not body:
        return ""
    return body.splitlines()[0]
