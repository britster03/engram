# Feature completeness

This is the authoritative "what's actually in the code" reference.
Every row has been verified by grepping the repo at the time of writing.

For live-run evidence against real infra (OpenAI + Neo4j + Redis, CPU
training loss curve), see [VALIDATION.md](VALIDATION.md).

Legend:

- **✓**  Implemented, runnable, has tests.
- **~**  Implemented, runnable, tested indirectly (exercised via higher-level tests).
- **R**  Runnable but requires optional `[training]` extras (torch + transformers + peft).

## SDD coverage — §1–§16

| SDD § | Feature | Status | File |
|---|---|---|---|
| §1.4 | Three-model inference stack | ✓ | `engram/models/{core,frontier,embeddings}.py`, `engram/models/providers/anthropic_provider.py` |
| §1.5 | Dual (default) vs unified gating | ✓ | `engram/config.py::GatingConfig.configuration` |
| §2.1 | Four-layer architecture | ✓ | Interface (`api/`), Orchestration (`retrieval/`, `ingest/`, `consolidation/`, `session/`), Intelligence (`models/`), Storage (`storage/`) |
| §2.2 | Filesystem / Neo4j (or InMemoryKG) / SQLite / Redis boundaries | ✓ | `engram/storage/*.py` |
| §2.3 | Filesystem is authoritative; `rebuild-kg` reconstructs Neo4j | ✓ | `engram/rebuild_kg.py` |
| §2.4 | Query + ingest request flows | ✓ | See diagrams in `templates/cypher/*.svg` |
| §3.1 | L0 regex + classifier + memory-hit fallback | ✓ | `engram/retrieval/l0_gate.py`; classifier Protocol, `AlwaysClass0Classifier` default |
| §3.2 | L1 plan + vector search | ✓ | `engram/retrieval/orchestrator.py::_l1_plan`, `_execute_l1` |
| §3.3 | L2 graph traversal | ✓ | `_ln_plan` + Cypher templates |
| §3.4 | L3 directory overviews | ✓ | `overview` / `t_overview_for` commands in `_execute_commands` |
| §3.5 | L4 full documents + multi-hop | ✓ | `_format_ltm_blocks` + `cat` command |
| §4.1 | Orchestrator is a discrete component | ✓ | `engram/retrieval/orchestrator.py::run_query` |
| §4.2 | Fused plan-judge at L2+ | ✓ | `_ln_plan` (single Core Model call) |
| §4.3 | Frontier NEED_MORE re-entry | ✓ | `_answer_loop` with `max_reentries` |
| §4.3.2 | Streaming after verdict | ✓ | `FrontierLLMProvider.stream_answer`, `/api/v1/query?stream=true` (SSE) |
| §4.4 | MSC assembly | ✓ | `_assemble_msc` |
| §4.4.1 | Node annotations | ✓ | `_annotate` emits `[ACTIVE]`, `[HISTORICAL]`, `[LOW_CONFIDENCE: 0.45]` |
| §4.4.2 | Token budget split 10/30/50/10 | ✓ | `_fit_to_tokens`, `_fit_blocks_to_tokens` using `engram.tokens.count_tokens` |
| §5.1 | Event-sourced ingest with outbox | ✓ | `engram/ingest/worker.py` + SQLite `events` & `fs_outbox` |
| §5.2 | Turn pair + turn group ingest | ✓ | `engram/api/schemas.py::IngestRequest.effective_pair()` |
| §5.3-5.4 | All 7 ingest pipeline steps | ✓ | `engram/ingest/worker.py::process_event` |
| §5.4.5.1 | YAML frontmatter schema | ✓ | `engram/frontmatter.py` |
| §5.5 | Crash recovery / reconciliation | ✓ | `engram/consolidation/reconciliation.py::run_once` |
| §5.6 | Failure modes | ✓ | `tests/integration/test_crash_recovery.py` |
| §5.7 | Entity linking safer defaults | ✓ | `engram/ingest/entity_linker.py` (0.92 cosine + 0.5 overlap, default new entity) |
| §6.1 | `mem://` filesystem | ✓ | `engram/uri.py`, `engram/storage/filesystem.py` |
| §6.1.3 | `.manifest` files | ✓ | `FilesystemStore.write_manifest`, `handle_regenerate_manifest` |
| §6.1.4 | `overview.md` files | ✓ | `handle_consolidate_overview` |
| §6.2 | Node schema | ✓ | Properties set in `_index_neo4j` |
| §6.3 | Reserved-key metadata validation | ✓ | `engram/frontmatter.py::validate_metadata`, called pre-step-6 |
| §6.4 | Edge schema + 5 edge types | ✓ | `Neo4jStore.merge_edge`, `InMemoryKnowledgeGraph.merge_edge` |
| §6.4.2 | Relation label controlled vocabulary | ✓ | `engram/prompts/relations.yaml`, `engram/relations.py`, `handle_normalize` |
| §6.5 | DUPLICATE / CONTRADICTION / CO_EXISTENCE conflict resolution | ✓ | `engram/ingest/conflict.py` |
| §6.5.1 | Orphan detection on supersession | ✓ | `_mark_orphan_historical` in `conflict.py` |
| §6.6 | Neo4j indexes | ✓ | `Neo4jStore.ensure_indexes`, `INDEX_STATEMENTS` |
| §7.2 | Consolidation task queue | ✓ | SQLite `consolidation_tasks` + unique pending index |
| §7.3 | All 7 task types | ✓ | `engram/consolidation/tasks.py` (CONSOLIDATE_OVERVIEW, REGENERATE_MANIFEST, PROPAGATE_OVERVIEW, ATOMIZE, NORMALIZE, TEMPORALIZE, INTEGRATE) plus UNMERGE |
| §7.4 | Triggers: on-write, on-session-commit, daily cron | ✓ | ingest step 7, session compaction re-ingest, `run_once` daily stale scan |
| §7.5 | Debounce + backpressure | ✓ | unique pending index + ingest 503 on queue saturation |
| §7.6 | Breadth-first tree rendering | ✓ | `engram/retrieval/tree_render.py` |
| §8.1 | CoreModelProvider contract | ✓ | `engram/models/core.py` |
| §8.2 | All 10 prompt templates | ✓ | `engram/prompts/*.j2`: gate_write, extract, l1_plan, ln_plan, dedup, entity_link, overview, session_compact, unmerge |
| §8.3 | Session compaction + re-ingest | ✓ | `engram/session/manager.py::compact_session` |
| §8.4 | CLI-style commands | ✓ | `_execute_commands` handles find, ls, cat, overview, rel, history, template |
| §8.5 | Cypher template library | ✓ | 8 templates in `templates/cypher/`, loader in `engram/retrieval/templates.py` |
| §8.5.2 | Read-only + bounded + status-filter + timeout | ✓ | Every template filtered + clamped in `templates.py::run_template` |
| §8.6 | Unmerge | ✓ | `engram/ingest/unmerge.py` + `task_type=UNMERGE` async handler |
| §9 | Session lifecycle + cache | ✓ | `engram/session/manager.py` |
| §10 | Soft memory decay | ✓ | `engram/decay.py` |
| §11 | REST API (all 15 endpoints) | ✓ | `engram/api/routes/*.py` |
| §12.1 | Bearer-token auth | ✓ | `engram/api/auth.py` |
| §12.2 | Multi-tenancy (tenant_id at every layer) | ✓ | `engram/tenancy.py` + tenant filters in storage + Cypher |
| §12.3 | Neo4j writer/reader role separation | ~ | Config supports it; Community Edition collapses to one role. Documented in `PRODUCTION.md`. |
| §13.2 | Telemetry | ✓ | `engram/metrics.py` + `/metrics` endpoint |
| §13.5 | Schema migrations | ✓ | `engram/migrations/runner.py` + `engram migrate` CLI |
| §14.1 | Gating classifier training | R | `engram/training/gate_classifier.py` — runnable with synthetic data; requires torch/transformers |
| §14.2 | Core Model training (SFT + DPO) | R | `engram/training/{core_sft,core_dpo}.py`; data format validated by tests |
| §14.2.6 | Continuous improvement loop | ~ | trace_collector script shipped; the monthly re-train cadence is an operator choice |
| §15 | Configuration schema | ✓ | `config.yaml` + `engram/config.py` |
| §16.1 | SQLite schemas | ✓ | Inline in `engram/storage/sqlite.py::SCHEMA_SQL` |
| §16.2 | Prompt file index | ✓ | `engram/prompts/` matches §16.2 exactly |
| §16.3 | Cypher template files | ✓ | `templates/cypher/` matches §16.3 exactly |

## Scale + operations features (beyond the SDD)

| Feature | Status | File |
|---|---|---|
| Resilience primitives (retry + circuit breaker + timeout) | ✓ | `engram/resilience.py` |
| Durable ingest worker (SQLite-polling, safe for multi-replica) | ✓ | `engram/ingest/durable_worker.py` |
| Graceful degradation (Neo4j / embeddings / Core / Frontier failures) | ✓ | wrapped paths in `engram/retrieval/orchestrator.py` |
| Real tokenizer (tiktoken + char fallback) | ✓ | `engram/tokens.py` |
| Body-size middleware + Pydantic field caps | ✓ | `engram/api/body_limit.py`, `engram/api/schemas.py` |
| Placeholder-secret guard at config load | ✓ | `engram/config.py::_detect_placeholders` |
| Async `/unmerge` via consolidation queue | ✓ | `handle_unmerge` |
| Redis-backed rate limiter w/ in-memory fallback | ✓ | `engram/api/rate_limit.py` |
| SSE streaming on `/query` | ✓ | `app.py::_sse_query_stream` |
| Structured JSON logging + request-ID propagation | ✓ | `engram/logging_setup.py`, `engram/api/request_id.py` |
| `/livez` vs `/readyz` split | ✓ | `engram/api/app.py` |
| Lifespan-managed workers (start + graceful stop) | ✓ | `_lifespan` in `app.py` |
| **Multi-tenancy** (tenant_id everywhere) | ✓ | `engram/tenancy.py`, filters in storage + Cypher |
| Admin API for tenant CRUD + key rotation | ✓ | `engram/api/routes/admin.py` |
| Leader election (Redis lease) for singleton workers | ✓ | `engram/coordination.py` |
| Distributed caches (embedding + overview) | ✓ | `engram/cache.py` |
| Append-only audit log | ✓ | `engram/audit.py` |
| Tenant-scoped quotas in rate limiter | ✓ | `RateLimitMiddleware._bucket_for_tenant` |
| OpenTelemetry tracing (FastAPI + HTTPX + Redis) | ✓ | `engram/tracing.py` — enabled when `OTEL_EXPORTER_OTLP_ENDPOINT` is set |
| **In-memory KG backend** (first-class alt to Neo4j) | ✓ | `engram/storage/memory_kg.py`; toggle via `knowledge_graph.backend=memory` |
| **CLI admin** (tenant create/list/mint-key/suspend/resume) | ✓ | `engram admin <action>` |

## Deployment artifacts

| Artifact | Status | Path |
|---|---|---|
| Single-host Docker Compose (dev) | ✓ | `docker-compose.yml` |
| Production Docker Compose (app + nginx + prom + grafana) | ✓ | `docker-compose.prod.yml` |
| Production Dockerfile | ✓ | `Dockerfile` |
| nginx TLS config (SSE-friendly, body caps, edge rate limit) | ✓ | `deploy/nginx.conf` |
| systemd unit + decay timer | ✓ | `deploy/systemd/` |
| Prometheus scrape + alert rules | ✓ | `deploy/prometheus/` |
| Grafana dashboard | ✓ | `deploy/grafana/dashboards/engram.json` |
| Kubernetes raw manifests (Deployment/HPA/PDB/Ingress/NetPol/ServiceMonitor/CronJob) | ✓ | `deploy/k8s/` |
| Helm chart | ✓ | `deploy/helm/engram/` |
| TLS cert README (certbot recipe + self-signed) | ✓ | `deploy/tls/README.md` |

## Client SDKs

| Client | Status | Path |
|---|---|---|
| Python SDK (sync client, retry, streaming, admin API) | ✓ | `clients/python/engram_client/` |
| TypeScript SDK (fetch-based, streaming, admin API) | ✓ | `clients/typescript/src/` |

## Training pipeline (optional — [training] extras)

| Feature | Status | File |
|---|---|---|
| Synthetic data generator for 10 tasks | ✓ | `engram/training/synthetic_data.py` |
| Trace collector from production logs | ✓ | `engram/training/trace_collector.py` |
| Gate classifier training script | R | `engram/training/gate_classifier.py` |
| Core SFT training script | R | `engram/training/core_sft.py` |
| Core DPO training script | R | `engram/training/core_dpo.py` |
| `engram train {synth,gate,sft,dpo}` CLI | ✓ | `engram/cli.py::cmd_train` |
| Data format validation (covers both SFT and classifier shapes) | ✓ | `engram.training.synthetic_data.validate_record` |
| Unit tests for generator output | ✓ | `tests/unit/test_synthetic_data.py` |

## What is **not** implemented (honest)

These are intentional omissions — not "coming soon" fantasy features:

- **Postgres control plane**: Pluggable backend is designed (see `SCALING.md`)
  but `PostgresStore` isn't shipped. SQLite-on-RWX handles multi-replica in
  practice. Write it when SQLite actually becomes the bottleneck on your
  workload.
- **Multi-region active/active**: Requires cross-region filesystem + Neo4j
  replication — deployment-specific, out of scope here.
- **Hard-delete of memories**: Only soft-delete via `/retire` is exposed.
  GDPR DSAR-style hard deletion needs a product-level workflow.
- **Live-API integration tests in CI**: All 148 pytest cases use the
  in-memory KG + deterministic providers because CI should not hit
  billable endpoints. A repeatable live smoke test exists outside of
  pytest at [`scripts/validation/openai_live_test.py`](../scripts/validation/openai_live_test.py),
  which has been run against real OpenAI + real Neo4j + real Redis; see
  [VALIDATION.md](VALIDATION.md) for the measured results.
- **Validated training artefacts**: The training scripts compile and
  their data loaders are tested, but I have not trained a Qwen3.5 adapter
  and verified answer quality. That's a multi-GPU-day experiment, not a
  codebase concern.
- **Web admin dashboard**: Admin operations are CLI + REST only. Wiring
  Grafana + a custom panel is the cheapest path; no first-party UI ships here.
