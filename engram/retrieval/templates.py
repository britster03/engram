"""Cypher template runner (§8.5).

All non-trivial Cypher executed on behalf of the Core Model goes through this
module. Templates live in `templates/cypher/` as static .cypher files. Params
are bound by name; no string interpolation of user input occurs.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path
from typing import Any

from engram.storage.neo4j_store import Neo4jStore

_TEMPLATES_DIR = Path(__file__).parent.parent.parent / "templates" / "cypher"


# Default limits per template (overrideable by params["limit"]).
_DEFAULT_PARAMS: dict[str, dict[str, Any]] = {
    "t_children_of": {"limit": 50},
    "t_neighbours_by_relation": {"hops": 1, "relation": None, "limit": 25},
    "t_path_between": {"max_hops": 4},
    "t_temporal_filter": {"from": None, "until": None, "limit": 25},
    "t_history_chain": {"limit": 20},
    "t_find_by_uri_prefix": {"limit": 25},
    "t_cross_references": {"limit": 10},
    "t_top_k_vector": {"over_k": 15, "k": 10, "dormant_floor": 0.05, "prefix": None},
}


# Each template records its required parameters so the caller can be validated
# before a Cypher string ever reaches Neo4j.
_REQUIRED: dict[str, set[str]] = {
    "t_children_of": {"uri"},
    "t_neighbours_by_relation": {"node_uri"},
    "t_path_between": {"src_uri", "dst_uri"},
    "t_temporal_filter": {"node_uri"},
    "t_history_chain": {"node_uri"},
    "t_find_by_uri_prefix": {"prefix"},
    "t_cross_references": {"node_uri"},
    "t_top_k_vector": {"vec"},
}


# Maximum hops cap — even if a caller passes a larger value, we clamp it
# (security defence-in-depth per §8.5.2).
_HOPS_CAP = 4


class TemplateError(ValueError):
    pass


@cache
def _load(name: str) -> str:
    path = _TEMPLATES_DIR / f"{name}.cypher"
    if not path.exists():
        raise TemplateError(f"unknown template: {name!r}")
    return path.read_text(encoding="utf-8")


def run_template(
    neo4j: Neo4jStore,
    name: str,
    params: dict[str, Any] | None = None,
    *,
    timeout_s: int | None = None,
) -> list[dict[str, Any]]:
    """Load `name`, merge defaults, validate required params, execute."""
    if name not in _REQUIRED:
        raise TemplateError(f"template not whitelisted: {name!r}")
    merged: dict[str, Any] = dict(_DEFAULT_PARAMS.get(name, {}))
    if params:
        merged.update(params)
    missing = _REQUIRED[name] - merged.keys()
    if missing:
        raise TemplateError(f"template {name!r} missing params: {sorted(missing)}")
    # Clamp hops
    if "hops" in merged and isinstance(merged["hops"], int):
        merged["hops"] = min(merged["hops"], _HOPS_CAP)
    if "max_hops" in merged and isinstance(merged["max_hops"], int):
        merged["max_hops"] = min(merged["max_hops"], _HOPS_CAP)
    cypher = _load(name)
    return neo4j.run_template(cypher, merged, timeout_s=timeout_s)


def available_templates() -> list[str]:
    return sorted(_REQUIRED.keys())
