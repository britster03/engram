"""Exercise enforced retrieval with actual repository rows, not mock row schemas."""

from engram.retrieval.orchestrator import run_query
from engram.storage.memory_repository import MemoryRepository
from engram.tenancy import Tenant, TenantQuotas, set_current_tenant
from tests.postgres_support import PostgresTestStore
from tests.unit.test_adaptive_retrieval import RecordingFrontier, _context


def test_enforced_exact_lookup_is_tenant_scoped_and_handles_real_claim_shapes():
    store = PostgresTestStore()
    repo = MemoryRepository(store)
    repo.ensure_schema()
    ctx = _context(repo, RecordingFrontier())
    ctx.cfg.retrieval.quality_mode = "enforce"
    try:
        for tid, role in (("quality-first", "Engineer"), ("quality-second", "Doctor")):
            person = repo.create_memory(memory_type="ENTITY", canonical_name="Alice", tenant_id=tid)
            repo.add_alias(entity_id=person.id, alias="Alice", tenant_id=tid)
            repo.add_claim(
                subject_id=person.id,
                predicate="HAS_ROLE",
                object_value=role,
                object_type="ROLE",
                tenant_id=tid,
            )
        for tid, role, forbidden in (
            ("quality-first", "Engineer", "Doctor"),
            ("quality-second", "Doctor", "Engineer"),
        ):
            set_current_tenant(
                Tenant(
                    tenant_id=tid,
                    display_name=tid,
                    api_key_hashes=[],
                    quotas=TenantQuotas(),
                    status="ACTIVE",
                )
            )
            result = run_query(ctx, session_id=None, query="What is Alice's role?")
            assert result.answerability == "ANSWERABLE"
            assert role in result.answer and forbidden not in result.answer
            assert result.retrieval_metadata.answer_mode == "deterministic-canonical"
            assert result.retrieval_metadata.nodes_retrieved > 0
            assert not ctx.frontier.calls
    finally:
        set_current_tenant(None)
        store.close()
