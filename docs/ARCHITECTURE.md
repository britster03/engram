# Architecture

Engram is an AI memory management system. It gives conversational agents
durable long-term memory through a **retrieval cascade** that enforces the
**Minimal Sufficient Context (MSC)** principle: deliver exactly enough
context to the frontier LLM, no more.

This document is the high-level map. See [FLOWS.md](FLOWS.md) for detailed
sequences and [DATA_MODEL.md](DATA_MODEL.md) for the persisted shapes.

## Four layers

| Layer | Components | File paths |
|---|---|---|
| Interface | REST API, CLI | `engram/api/`, `engram/cli.py` |
| Orchestration | Retrieval orchestrator, session manager, ingest worker, consolidation worker, reconciliation worker | `engram/retrieval/`, `engram/session/`, `engram/ingest/`, `engram/consolidation/` |
| Intelligence | Gating classifier (L0), Core Model, Frontier LLM, embedding service | `engram/models/` |
| Storage | Filesystem (`mem://`), Neo4j (KG), SQLite (control plane), Redis (session cache) | `engram/storage/` |

The layers talk to each other through narrow contracts: the API calls into
orchestration; orchestration calls the intelligence + storage layers; no
layer reaches past its neighbour. Each contract is typed by a Pydantic or
`dataclass` DTO.

## Storage boundaries

Engram uses **three storage backends with strict responsibilities**:

| Backend | Stores | Authoritative? |
|---|---|---|
| **Filesystem** (`./data/mem`) | Every memory's body (`*.md`), directory overviews (`overview.md`), manifests (`.manifest`) | **Yes** — source of truth |
| **Neo4j** | Node topology, `l0_embedding` vector index, fulltext index over `l0_abstract`, semantic edges | No — derived index |
| **SQLite (WAL)** | Event ledger, ingest outbox, consolidation task queue, extractions, linked_entities, migration metadata | Control plane only |
| **Redis** | Session cache keyed by `session:{session_id}` | Ephemeral |

**Boundary rule (§2.2):** if the data is used to filter/sort/join, it lives
as a KG property. If a model consumes it as natural-language context, it
lives on disk. If it tracks processing state, it lives in SQLite. Every
piece of data has exactly one authoritative home.

## Three-model inference stack

```
            Gating Model        Core Model         Frontier LLM
          (BGE-Small 33M)  (Qwen3.5-0.8B or API)  (Claude / GPT-4 / …)
                 │                  │                    │
            embeddings          planning, extraction,    final answer
          + L0 classifier       dedup, overview          (ANSWER | NEED_MORE)
```

* **Gating model**: `engram/models/embeddings.py` (BGE-Small-EN-v1.5, 384-d).
* **Core model**: abstract provider at `engram/models/core.py`; Anthropic
  adapter at `engram/models/providers/anthropic_provider.py`. Swap
  implementations by changing `core_model.provider` in `config.yaml`.
* **Frontier LLM**: `engram/models/frontier.py` (abstract) and the same
  Anthropic adapter. Supports both buffered `answer()` and `stream_answer()`
  per §4.3.2.

## Request lifecycles (50-ft view)

### Ingest (write path)

```
POST /api/v1/ingest
     │
     ▼
SQLite event ledger (sync, idempotent on pair_id)
     │
     ▼ (background task)
Write-path gate → S-R-O extraction → entity linking
     │
     ▼
Filesystem write (authoritative) → fs_outbox=WRITTEN
     │
     ▼
Dedup/conflict → KG merge → fs_outbox=INDEXED
     │
     ▼
Enqueue CONSOLIDATE_OVERVIEW + REGENERATE_MANIFEST + PROPAGATE_OVERVIEW
```

### Query (read path)

```
POST /api/v1/query
     │
     ▼
L0 binary gate (regex + optional classifier + memory-hit fallback)
     │            └── BYPASS → frontier directly
     ▼ CONTINUE
L1 plan (Core Model) + vector search
     │
     ▼
L2 graph traversal (Cypher templates, fused plan-judge)
     │
     ▼
L3 directory overviews
     │
     ▼
L4 full documents + multi-hop
     │
     ▼
MSC assembly (10/30/50/10 token-budget split)
     │
     ▼
Frontier LLM → ANSWER | NEED_MORE (re-enter up to max_reentries)
```

See [FLOWS.md](FLOWS.md) for the annotated step-by-step.

## Background workers

| Worker | Cadence | Responsibility |
|---|---|---|
| Ingest worker | On-demand (FastAPI `BackgroundTasks`) + reconciliation replay | Drive events through steps 2–7 |
| Consolidation worker | Daemon thread, 10s poll | Regenerate overview.md / manifest / propagate / atomize / normalize / temporalize / integrate |
| Reconciliation worker | Daemon thread, 60s interval + startup | Requeue stuck events; scan stale directory overviews |
| Decay cron | On-demand via `engram decay` (schedule externally) | Recompute `retrieval_weight` for every ACTIVE node |

Workers are started by the FastAPI `lifespan` context manager
(`engram/api/app.py`) and stopped cleanly on shutdown.

## Fault tolerance

* **Event-sourced ingest with outbox** (§5.1): every ingest persists to the
  Event Ledger synchronously before any downstream work begins. Every step
  is idempotent so replay is safe.
* **Filesystem-authoritative** (§2.3): Neo4j loss is recoverable via
  `engram rebuild-kg`. Filesystem loss requires restoring from snapshot.
* **Reconciliation worker** (§5.5): scans the Event Ledger and outboxes for
  stuck states every 60 seconds and requeues them.
* **Backpressure** (§7.5): ingest returns 503 with `Retry-After` when
  `consolidation_tasks.queue_depth > max_backlog`.

## Security posture

* Bearer-token auth on every endpoint except `/api/v1/health` and `/metrics`.
* Token-bucket rate limiting on `/api/v1/query` and `/api/v1/ingest`.
* Cypher is whitelisted: the Core Model never emits raw Cypher; it picks a
  parameterised template from `templates/cypher/`. Each template is bounded
  (LIMIT, hops cap) and status-filtered by default.
* Neo4j reader/writer role separation is a configuration swap on Enterprise
  Edition; Community uses a single admin user (see `config.yaml`).

## Extensibility

* **Add a Core Model task**: drop a new Jinja template under
  `engram/prompts/`, add a call site in the caller module, and register a
  handler in the stub if you want tests to exercise it.
* **Add a Cypher template**: drop a `.cypher` file under
  `templates/cypher/`, register its required params in
  `engram/retrieval/templates.py`. The orchestrator runs them under the
  read-only timeout budget.
* **Add an LLM provider**: implement `CoreModelProvider` /
  `FrontierLLMProvider` and expose it through `build_core_provider` /
  `build_frontier_provider`.
* **Add a consolidation task type**: add a handler in
  `engram/consolidation/tasks.py`, dispatch it from
  `engram/consolidation/worker.py`, and whitelist the task_type in
  `engram/api/routes/consolidation.py`.

See [DEV_GUIDE.md](DEV_GUIDE.md) for the concrete recipes.
