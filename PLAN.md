# Engram MVP — Build Plan

Source spec: [`Engram_SDD.pdf`](Engram_SDD.pdf) v1.0 (April 2026).

**Status (2026-04-21):** every SDD feature implemented, 56 tests green,
docs/ directory complete. See [docs/](docs/) for the maintained reference.

## SDD coverage map

| SDD § | Feature | Implementation |
|---|---|---|
| §1.4 | Three-model inference stack | `engram/models/{core,frontier,embeddings}.py` + `providers/anthropic_provider.py` |
| §1.5 | Dual/unified gating | Dual (default) wired; unified documented as deployment swap |
| §2.1 | Four-layer architecture | Interface / Orchestration / Intelligence / Storage — see ARCHITECTURE.md |
| §2.2 | Storage architecture | `engram/storage/{sqlite,neo4j_store,filesystem,redis_cache}.py` |
| §2.3 | Trust + authoritativeness | `engram/rebuild_kg.py` reconstructs Neo4j from filesystem + extractions |
| §2.4 | High-level request flows | `engram/api/app.py`, `engram/retrieval/orchestrator.py`, `engram/ingest/worker.py` |
| §3.1 | L0 gate | `engram/retrieval/l0_gate.py` — regex + classifier Protocol + memory-hit fallback |
| §3.1.1 | Regex patterns | `_REGEX_PATTERNS` in `l0_gate.py` |
| §3.1.2 | Memory-hit fallback | `run_l0_gate(..., memory_hit_threshold=0.75)` |
| §3.1.3 | L0 skip | `retrieval.l0_skip=true` honoured |
| §3.2 | L1 plan + vector | `engram/prompts/l1_plan.j2`, `orchestrator._l1_plan/_execute_l1` |
| §3.2.1 | L1 output schema | reflected in prompt + orchestrator defaults |
| §3.2.2 | AGFS / KG / HYBRID | encoded in prompt |
| §3.2.3 | Short-circuit at L1 | `session_sufficient → session_answer_context` path |
| §3.3 | L2 graph traversal | Cypher templates + fused plan-judge |
| §3.4 | L3 overviews | `overview` / `t_overview_for` command + `FilesystemStore.read_overview` |
| §3.5 | L4 full docs + multi-hop | `cat` command + `t_path_between`, `t_neighbours_by_relation` |
| §4.1 | Orchestrator responsibilities | `engram/retrieval/orchestrator.py::run_query` |
| §4.2 | Plan-Judge fusion | `_ln_plan` (single call emits judgment + next commands) |
| §4.3 | Frontier re-entry | `_answer_loop` with `max_reentries` + suggested queries |
| §4.3.2 | Streaming after verdict | `FrontierLLMProvider.stream_answer` (Anthropic impl streams) |
| §4.4 | MSC assembly | `_assemble_msc` with node annotations |
| §4.4.1 | Node annotations | `_annotate` emits `[ACTIVE]`, `[HISTORICAL]`, `[LOW_CONFIDENCE: 0.45]` |
| §4.4.2 | Token budget 10/30/50/10 | `_fit_to_tokens` + `_fit_blocks_to_tokens` |
| §5.1 | Event-sourced + outbox | `engram/ingest/worker.py` + SQLite `events` and `fs_outbox` |
| §5.2 | Turn pair + turn group | `schemas.IngestRequest.effective_pair()` + `_turn_pair` |
| §5.3–5.4 | 7-step pipeline | `worker.process_event` |
| §5.4.5.1 | YAML frontmatter schema | `engram/frontmatter.py` |
| §5.5 | Crash recovery | `engram/consolidation/reconciliation.py::run_once` |
| §5.6 | Failure modes | Tested in `tests/integration/test_crash_recovery.py` |
| §5.7 | Entity linking safer defaults | `engram/ingest/entity_linker.py` (0.92 cosine + 0.5 overlap, default new-entity) |
| §6.1 | mem:// filesystem | `engram/uri.py` + `storage/filesystem.py` |
| §6.2 | Node schema | Properties set in `_index_neo4j` |
| §6.3 | Reserved-key validation | `frontmatter.validate_metadata` called pre-step-6 |
| §6.4 | Edge schema | `Neo4jStore.merge_edge` + orchestrator uses |
| §6.4.2 | Relation-label vocab | `engram/prompts/relations.yaml` + `engram/relations.py` + NORMALIZE task |
| §6.5 | Conflict resolution | `engram/ingest/conflict.py` rule-based tier + dedup prompt |
| §6.5.1 | Orphan detection | `_mark_orphan_historical` after supersession |
| §6.6 | Indexes | `Neo4jStore.ensure_indexes()` |
| §7.1–7.2 | Consolidation task queue | `SqliteStore.enqueue_task` + `consolidation_tasks` table |
| §7.3 | All 7 task types | `engram/consolidation/tasks.py` handlers |
| §7.4 | Triggers incl. daily cron | ingest step 7 + session commit + daily directory-stale scan |
| §7.5 | Debounce + backpressure | unique pending index + ingest 503 on queue saturation |
| §7.6 | Breadth-first tree render | `engram/retrieval/tree_render.py` |
| §8.1 | CoreModelProvider | `engram/models/core.py` |
| §8.2 | All 10 prompt templates | `engram/prompts/*.j2` |
| §8.3 | Session compaction + re-ingest | `engram/session/manager.py::compact_session` |
| §8.4 | CLI-style commands | `_execute_commands` + `_alias_to_template` |
| §8.5 | Cypher template library | `templates/cypher/` + `engram/retrieval/templates.py` |
| §8.6 | Unmerge | `engram/ingest/unmerge.py` |
| §9 | Session management | `engram/session/manager.py` |
| §10 | Soft memory decay | `engram/decay.py` |
| §11 | REST API | `engram/api/routes/` |
| §12 | Security (auth, tenancy, RBAC) | bearer auth + documented RBAC swap for Enterprise |
| §13.2 | Telemetry | `engram/metrics.py` + `/metrics` endpoint |
| §13.5 | Schema migrations | `engram/migrations/` |
| §14 | Model training | `engram/training/` scaffolds (optional, deferred by design) |
| §15 | Configuration | `config.yaml` + `engram/config.py` |
| §16 | Appendices (SQLite / prompts / Cypher) | covered; see respective modules |

## Progress log

| Date | Milestone |
|---|---|
| 2026-04-21 | Walking skeleton complete (18 tests) |
| 2026-04-21 | Full cascade, sessions, Cypher templates, auto-compaction |
| 2026-04-21 | Consolidation worker, conflict resolution, entity linking, unmerge |
| 2026-04-21 | Reconciliation, decay, rate limiting, metrics, rebuild-kg CLI (41 tests) |
| 2026-04-21 | Hardening pass: lifespan workers, backpressure, metrics emission, batch ingest, full unmerge, all 7 consolidation task types, RELATES_TO rebuild, schema migrations (56 tests) |
| 2026-04-21 | SDD gap closure: dedup prompt, relations vocab, reserved-key validation, orphan detection, daily stale scan, full CLI command dispatch, turn-group ingest, MSC token budget split, streaming, training scaffolding |
| 2026-04-21 | Documentation tree: ARCHITECTURE, DATA_MODEL, FLOWS, API, RUNBOOK, DEV_GUIDE, TRAINING |
| 2026-04-21 | **Production hardening** (95 tests): reliability primitives (retry + circuit breaker + timeout) in `engram/resilience.py`; Anthropic provider uses `@resilient` with 3-attempt exponential backoff and malformed-JSON retry; durable ingest worker (`engram/ingest/durable_worker.py`) replaces FastAPI `BackgroundTasks`; graceful degradation for Neo4j / embeddings / Core Model / Frontier failures; real tiktoken-based tokenizer (`engram/tokens.py`) for honest §4.4.2 budget split; body-size middleware (`engram/api/body_limit.py`) + Pydantic field caps + placeholder-secret guard at config load; async `/unmerge` via `task_type=UNMERGE` handler; Redis-backed rate limiter with in-memory fallback; SSE streaming on `/query` when `stream=true`; JSON structured logging with request-ID context propagation (`engram/logging_setup.py`, `engram/api/request_id.py`); `/livez` + `/readyz` split; lifespan-managed workers. |
| 2026-04-21 | **Deployment artifacts**: multi-stage `Dockerfile`, `docker-compose.prod.yml` (app + nginx + Prometheus + Grafana + Neo4j + Redis), nginx TLS config with SSE-friendly proxy_buffering off, systemd unit with sandboxing (`deploy/systemd/engram.service`), systemd timer for daily decay, Prometheus alert rules (`deploy/prometheus/alerts.yml`), Grafana dashboard (`deploy/grafana/dashboards/engram.json`), TLS cert README. |
| 2026-04-21 | **Operator documentation**: `docs/PRODUCTION.md` with pre-flight checklist, incident playbook, capacity planning, upgrade procedure, explicit scope limits (single-tenant; not public multi-tenant SaaS). |
| 2026-04-22 | **Scale platform**: full multi-tenancy (`engram/tenancy.py` — TenantRegistry, hashed API keys, tenant-scoped URIs); `tenant_id` on every SQLite row, Neo4j node property, Cypher template; filesystem sub-routing; Admin API (`/api/v1/admin/*`) + `engram admin` CLI; leader-elected background workers via Redis lease (`engram/coordination.py`); distributed caches (`engram/cache.py`); append-only audit log (`engram/audit.py`); tenant-scoped rate-limiter quotas; OpenTelemetry tracing (`engram/tracing.py`); first-class **in-memory KG** backend (`engram/storage/memory_kg.py`) as alternative to Neo4j; Kubernetes manifests + Helm chart; Python + TypeScript client SDKs; Locust load-test harness. |
| 2026-04-22 | **Training pipeline**: synthetic data generator for every Core-Model task (`engram/training/synthetic_data.py`, 60k+ records); updated SFT / gate-classifier scripts with lazy ML-dep imports + synthetic-data compatibility; `engram train {synth,gate,sft,dpo}` CLI; data-format validation with unit tests. |
| 2026-04-22 | **Honest audit**: `docs/FEATURES.md` — every claim verified against code; `SCALING.md` with scale axes, multi-tenancy, Kubernetes, SDKs; 130 tests green. Zero "fake"/"stub"/"mock" names in production code. |
