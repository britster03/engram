"""PostgreSQL-canonical consolidation handlers used by Temporal V2 workers."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from engram import prompts
from engram.config import ConsolidationConfig
from engram.models.core import CoreModelProvider
from engram.models.request_context import model_request_session
from engram.storage.memory_repository import MemoryRepository


def consolidate_overview(
    *,
    node_id: str,
    repository: MemoryRepository,
    core: CoreModelProvider,
    cfg: ConsolidationConfig,
    tenant_id: str,
    overview_cache: Any | None = None,
) -> None:
    """Generate a revision-bound overview solely from canonical snapshots."""

    node = repository.resolve_memory_ref(node_id, tenant_id=tenant_id, include_historical=False)
    if node is None:
        raise ValueError(f"canonical overview scope not found: {node_id}")
    children = repository.get_children(node.id, tenant_id=tenant_id)
    child_abstracts: list[dict[str, str]] = []
    child_relations: list[dict[str, Any]] = []
    for child in children:
        snapshot = repository.get_current_state(child.id, tenant_id=tenant_id)
        if snapshot is None:
            continue
        version = snapshot.current_version
        child_abstracts.append(
            {
                "source_uri": child.canonical_uri,
                "abstract": (
                    version.abstract
                    if version and version.abstract
                    else (version.body[:500] if version else child.canonical_name or "")
                ),
            }
        )
        child_relations.extend(
            {
                "claim_id": str(claim.id),
                "subject_id": str(claim.subject_id),
                "predicate": claim.predicate,
                "object_entity_id": (
                    str(claim.object_entity_id) if claim.object_entity_id else None
                ),
                "object_value": claim.object_value,
                "status": claim.status,
            }
            for claim in snapshot.claims
        )
    prompt = prompts.render(
        "overview",
        directory_uri=node.canonical_uri,
        children_abstracts=child_abstracts,
        children_relations=child_relations,
        overview_max_tokens=cfg.overview_max_tokens,
    )
    with model_request_session(str(node.id)):
        result = core.complete(
            system_prompt=prompt,
            user_prompt="Return the canonical overview text.",
        )
    if isinstance(result.output, dict):
        text = str(result.output.get("overview") or result.output.get("summary") or "")
    elif isinstance(result.output, str):
        text = result.output
    else:
        text = result.raw_text
    if not text.strip():
        raise ValueError("overview model returned empty content")
    # Re-read after the model call. A concurrent child mutation changes the
    # scope revision and makes this generated result unsafe to publish.
    current = repository.get_memory(node.id, tenant_id=tenant_id)
    if current is None or current.revision != node.revision:
        raise RuntimeError("canonical overview scope changed during generation")
    repository.save_overview(
        scope_id=node.id,
        content=text.strip(),
        input_revision=current.revision,
        model_metadata={"generator": "muse-spark", "schema": "overview-v2"},
        tenant_id=tenant_id,
    )
    if overview_cache is not None:
        overview_cache.invalidate(tenant_id, node.canonical_uri)


def unmerge_entity(
    *,
    node_id: str,
    task_id: str,
    repository: MemoryRepository,
    core: CoreModelProvider,
    tenant_id: str,
) -> None:
    """Plan an entity split with Muse Spark, then commit it canonically."""

    node = repository.resolve_memory_ref(node_id, tenant_id=tenant_id, include_historical=False)
    if node is None:
        raise ValueError(f"canonical entity not found: {node_id}")
    if node.memory_type != "ENTITY":
        raise ValueError("only ENTITY memories can be unmerged")
    snapshot = repository.get_current_state(
        node.id,
        tenant_id=tenant_id,
        include_historical_claims=True,
        require_current_overview=False,
    )
    if snapshot is None:
        raise ValueError(f"canonical entity not found: {node_id}")
    source_extractions = [asdict(claim) for claim in snapshot.claims]
    prompt = prompts.render(
        "unmerge",
        merged_node={
            "memory_id": str(node.id),
            "canonical_uri": node.canonical_uri,
            "canonical_name": node.canonical_name,
            "l0_abstract": (
                snapshot.current_version.abstract
                if snapshot.current_version
                else node.canonical_name or ""
            ),
        },
        source_extractions=source_extractions,
    )
    with model_request_session(str(node.id)):
        result = core.complete(
            system_prompt=prompt,
            user_prompt="Return the split JSON.",
        )
    output = result.output if isinstance(result.output, dict) else {}
    raw_splits = output.get("splits") or []
    splits = [dict(item) for item in raw_splits if isinstance(item, dict)]
    if not splits:
        raise ValueError("unmerge produced no validated entity splits")
    repository.unmerge_entity(
        node.id,
        splits,
        tenant_id=tenant_id,
        mutation_key=f"consolidation:{task_id}:unmerge",
    )


def enqueue_stale_overviews(
    *,
    repository: MemoryRepository,
    control_plane: Any,
    tenant_id: str,
    limit: int = 200,
) -> int:
    """Enqueue scopes whose latest overview does not match current revision."""

    with repository.transaction(tenant_id=tenant_id) as conn:
        rows = conn.execute(
            "SELECT n.id FROM memory_nodes n WHERE n.tenant_id = ? "
            "AND n.status = 'ACTIVE' AND n.memory_type IN ('COLLECTION', 'PROJECT') "
            "AND NOT EXISTS (SELECT 1 FROM memory_overviews o "
            "WHERE o.tenant_id = n.tenant_id AND o.scope_id = n.id "
            "AND o.input_revision = n.revision) "
            "ORDER BY n.updated_at LIMIT ?",
            (tenant_id, limit),
        ).fetchall()
    return sum(
        1
        for row in rows
        if control_plane.enqueue_task(
            node_id=str(row["id"]),
            task_type="CONSOLIDATE_OVERVIEW",
            priority=6,
            tenant_id=tenant_id,
        )
    )


__all__ = ["consolidate_overview", "enqueue_stale_overviews", "unmerge_entity"]
