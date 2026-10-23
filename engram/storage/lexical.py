"""Versioned lexical projection and server-owned Lucene query construction."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any

BM25_INDEX = "canonical_bm25_v1"
TEXT_LIMIT = 32_000


def tenant_token(tenant_id: str) -> str:
    if not tenant_id or not tenant_id.strip():
        raise ValueError("lexical search requires an explicit tenant")
    return "t" + hashlib.sha256(tenant_id.encode("utf-8")).hexdigest()


def query_terms(query: str) -> list[str]:
    # No operator, field selector, wildcard or quote survives into Lucene
    # syntax. Query and document analysis still belongs to the index analyzer.
    return list(dict.fromkeys(re.findall(r"[^\W_]+", query.casefold(), re.UNICODE)))[:32]


def scoped_query(query: str, tenant_id: str) -> str | None:
    scope = tenant_token(tenant_id)
    terms = query_terms(query)
    if not terms:
        return None
    words = " OR ".join(f'"{term}"' for term in terms)
    return f"search_tenant:{scope} AND search_state:active AND search_text:({words})"


def projection_text(snapshot: Mapping[str, Any]) -> str:
    """Materialize supported current text, independently of model summaries."""
    node = snapshot.get("node_properties") or snapshot.get("properties") or {}
    version = snapshot.get("current_version") or {}
    parts = [str(node.get("canonical_name") or "")]
    parts.extend(
        str(a.get("alias") or "") for a in snapshot.get("aliases", []) if isinstance(a, Mapping)
    )
    # Put atomic assertions before document prose so a long body cannot evict
    # every exact fact. All text remains discovery data requiring hydration.
    for claim in snapshot.get("claims", []):
        if not isinstance(claim, Mapping) or claim.get("status") not in {"ACTIVE", "CURRENT"}:
            continue
        value = claim.get("object_value")
        if value is None:
            value = claim.get("object_entity_id")
        parts.append(
            f"{claim.get('predicate', '')} {json.dumps(value, ensure_ascii=False, default=str)}"
        )
    parts.extend(str(version.get(key) or "") for key in ("abstract", "body"))
    return "\n".join(part for part in parts if part)[:TEXT_LIMIT]
