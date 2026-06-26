# Features

This file is the current feature inventory for the source tree. The original
SDD remains the baseline design, but the implementation now includes later
extensions for multi-tenancy, administration, bulk ingest, KG visualization,
and provider support.

Legend:

- `yes`: implemented and covered by direct or integration tests.
- `partial`: implemented but limited by deployment mode, optional extras, or
  indirect coverage.
- `no`: intentionally not shipped or still a known gap.

## SDD-Critical Behavior

| Capability | Status | Evidence |
|---|---:|---|
| Filesystem-authoritative `mem://` storage | yes | `engram/storage/filesystem.py`, `engram/rebuild_kg.py` |
| SQLite event ledger and filesystem outbox | yes | `engram/storage/sqlite.py`, `engram/ingest/worker.py` |
| Durable ingest worker owns event processing | yes | `engram/ingest/durable_worker.py`, `engram/api/app.py` |
| API/session/chat/retry paths only enqueue or reset ingest rows | yes | `engram/api/routes/sessions.py`, `engram/api/routes/chat.py`, `engram/api/routes/events.py` |
| Crash recovery and stuck-event reconciliation | yes | `engram/consolidation/reconciliation.py`, `tests/integration/test_crash_recovery.py` |
| L0 to L4 retrieval cascade with `NEED_MORE` re-entry | yes | `engram/retrieval/orchestrator.py`, `engram/retrieval/l0_gate.py` |
| Tree rendering and bounded Cypher templates | yes | `engram/retrieval/tree_render.py`, `engram/retrieval/templates.py`, `templates/cypher/` |
| `t_path_between` template support | yes | `templates/cypher/t_path_between.cypher`, `tests/unit/test_templates.py` |
| Minimal Sufficient Context assembly and annotations | yes | `engram/retrieval/orchestrator.py` |
| Low-confidence triplets become `LOW_CONFIDENCE` `FACT` nodes | yes | `engram/ingest/worker.py`, `tests/integration/test_ingest_query_flow.py` |
| Conflict resolution with duplicate, contradiction, coexistence | yes | `engram/ingest/conflict.py`, `tests/unit/test_conflict.py` |
| Entity linking | yes | `engram/ingest/entity_linker.py` |
| Consolidation queue and task handlers | yes | `engram/consolidation/worker.py`, `engram/consolidation/tasks.py` |
| Session compaction and session close commit | yes | `engram/session/manager.py` |
| Durable `SESSION_SUMMARY` memory on session commit | yes | `engram/session/manager.py`, `tests/unit/test_session_manager.py` |
| Soft decay and retrieval-weight recomputation | yes | `engram/decay.py`, `tests/unit/test_decay.py` |
| Unmerge workflow | yes | `engram/ingest/unmerge.py`, `tests/integration/test_unmerge.py` |
| Rebuildable KG derived from filesystem | yes | `engram/rebuild_kg.py` |
| REST API for ingest, query, sessions, memories, events, consolidation | yes | `engram/api/app.py`, `engram/api/routes/` |
| Bearer-token auth | yes | `engram/api/auth.py` |
| Telemetry and health/readiness | yes | `engram/metrics.py`, `engram/api/app.py` |
| Migrations | yes | `engram/migrations/runner.py` |
| Training scripts and synthetic data | partial | `engram/training/`, optional training extras required |

## Post-SDD Extensions

| Capability | Status | Evidence |
|---|---:|---|
| Tenant registry and tenant-scoped API keys | yes | `engram/tenancy.py`, `engram/api/routes/admin.py` |
| Tenant isolation across SQLite, filesystem, KG, sessions, and rate limits | yes | `tests/integration/test_tenant_isolation.py`, `tests/unit/test_session_cache.py` |
| Tenant-scoped Redis session keys | yes | `engram/storage/redis_cache.py` uses `session:{tenant_id}:{session_id}` |
| Admin UI | yes | `engram/admin/routes.py`, `engram/admin/templates/`, `tests/e2e/test_admin_ui.py` |
| Bulk upload API and Admin UI panel | yes | `engram/api/routes/bulk_ingest.py`, `engram/admin/templates/dashboard/_ingest.html`, `tests/integration/test_bulk_and_kg.py` |
| KG graph API and Admin UI visualization | yes | `engram/api/routes/kg.py`, `engram/admin/templates/kg.html`, `engram/admin/static/js/graph-lite.js` |
| In-memory KG backend | yes | `engram/storage/memory_kg.py` |
| Ollama Cloud provider | yes | `engram/models/providers/ollama_cloud.py`, `config.ollama-cloud.yaml`, `tests/e2e/test_ollama_cloud_live.py` |
| Local Ollama OpenAI-compatible provider | yes | `engram/models/providers/__init__.py`, `engram/models/providers/openai_compat.py` |
| Local provider scaffold | yes | `engram/models/providers/local_provider.py` |
| `api_base` on Core and Frontier configs | yes | `engram/config.py` |
| Resilience wrappers for provider calls | yes | `engram/resilience.py`, provider tests |
| Redis-backed rate limiting and quotas | yes | `engram/api/rate_limit.py` |
| Leader election for singleton workers | yes | `engram/coordination.py` |
| Append-only audit log | yes | `engram/audit.py` |
| OpenTelemetry tracing hooks | yes | `engram/tracing.py` |
| Disposable embedding and overview caches | yes | `engram/cache.py`, `engram/deps.py` |
| Python and TypeScript client SDKs | partial | `clients/python/`, `clients/typescript/` |
| Deployment assets | partial | `deploy/`, deployment-specific validation required |

## Provider Matrix

| Provider | Core | Frontier | Notes |
|---|---:|---:|---|
| `openai` | yes | yes | OpenAI-compatible adapter. |
| `openai_compat` | yes | yes | Requires explicit `api_base`. |
| `ollama` | yes | yes | Local Ollama at `http://localhost:11434/v1`. |
| `ollama_cloud` | yes | yes | Default provider; direct Ollama API at `https://ollama.com/api`; reads `OLLAMA_API_KEY`. |
| `local` | yes | no | Core provider scaffold for local model use. |

## Cache And Storage Guarantees

- Filesystem memory files are authoritative.
- Neo4j and the in-memory KG are derived indexes.
- SQLite is the control plane for events, outboxes, bulk jobs, recovery state,
  consolidation tasks, tenants, and audit rows.
- The session cache stores active conversation state only and is tenant-scoped.
- Embedding and overview caches are post-SDD performance extensions. They are
  disposable, non-authoritative, and must not be used as durable memory.

## Known Limits

- BM25-assisted duplicate classification is not implemented in the current
  conflict resolver. The shipped resolver uses exact URI/cosine/Core-model
  conflict decisions.
- Hard-delete of memories is not exposed; memory retirement is soft-delete.
- Active/active multi-region replication is not shipped.
- Postgres control-plane storage is not shipped.
- Training scripts are runnable with optional extras, but this repo does not
  include a validated trained adapter artifact.
- Some retained historical docs may lag the newest Admin UI, bulk upload, KG
  visualization, and Ollama Cloud surfaces. Treat this file, root `README.md`,
  and `docs/ARCHITECTURE.md` as the current overview.
