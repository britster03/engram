# Architecture

Engram is a durable memory service for conversational agents. The original SDD
defines a filesystem-authoritative memory system with event-sourced ingest and
a shallow-to-deep retrieval cascade. The current implementation keeps those
invariants and adds multi-tenancy, Admin UI workflows, bulk upload, KG
visualization, and additional provider options.

See [FEATURES.md](FEATURES.md) for implementation status and [DATA_MODEL.md](DATA_MODEL.md)
for persisted shapes.

## Runtime Layers

| Layer | Responsibilities | Main code |
|---|---|---|
| Interface | REST API, OpenAI-compatible chat endpoint, Admin UI, CLI, clients | `engram/api/`, `engram/admin/`, `engram/cli.py`, `clients/` |
| Orchestration | Retrieval cascade, Temporal workflows/activities, sessions, consolidation, reconciliation, decay | `engram/retrieval/`, `engram/temporal/`, `engram/session/`, `engram/consolidation/`, `engram/decay.py` |
| Intelligence | Embeddings, Core Model tasks, Frontier LLM answers, provider adapters | `engram/models/`, `engram/prompts/` |
| Storage | Filesystem, PostgreSQL control plane, Neo4j, Redis/session state | `engram/storage/`, `engram/cache.py` |

The API layer does not process ingest events directly. In production it commits
the event and Temporal dispatch outbox atomically to PostgreSQL. Dedicated
Temporal workers own extraction, filesystem writes, KG indexing, consolidation,
reconciliation, and decay.

## Storage Boundaries

| Store | Contents | Authority |
|---|---|---|
| Filesystem under `data/mem` | Memory Markdown, frontmatter, overviews, manifests | Authoritative memory body and metadata |
| PostgreSQL | Events, outboxes, bulk jobs, recovery state, consolidation tasks, tenants, audit rows, workflow dispatches | Authoritative control plane in every environment |
| Neo4j | Tenant-scoped nodes, edges, vector index, full-text index, traversal graph | Derived index, rebuildable |
| In-memory KG | Test/dev alternative to Neo4j | Derived index, rebuildable |
| Redis session cache | Active session state keyed as `session:{tenant_id}:{session_id}` | Ephemeral session state |
| Redis or memory performance caches | Embedding vectors and rendered overviews | Disposable optimization only |

Boundary rule: natural-language memory content lives on disk, queryable graph
metadata lives in the KG, process state lives in PostgreSQL, and active session
state lives in the session cache. No performance cache is source of truth.

## Cache Policy

The SDD Phase 1 rule is "session-only caching". The current implementation
keeps the required session cache and adds two post-SDD performance caches:

- `EmbeddingCache`: exact text plus model tag to embedding vector. This avoids
  recomputing embeddings for repeated strings.
- `OverviewCache`: tenant plus directory URI to rendered overview text. This
  avoids repeated overview reads/renders during retrieval.

Both caches are tenant-safe and disposable. Clearing them should only make the
next request slower. It must not remove a memory, change a KG edge, affect
tenant ownership, or change correctness.

## Ingest Flow

```text
POST /api/v1/ingest, /api/v1/sessions/*/message, /api/v1/chat/completions,
or /api/v1/ingest/bulk
  -> atomically record PostgreSQL event + Temporal dispatch
  -> dispatcher starts the idempotent Temporal ingest workflow
  -> write-path gate
  -> extraction
  -> entity linking
  -> filesystem write and outbox update
  -> conflict resolution
  -> KG index update
  -> consolidation task enqueue
```

Session and chat paths commit completed turns to the ledger before their
non-streaming response, or in the streaming generator's finalization. Retry
paths create a new workflow generation; they do not run extraction inline.

Low-confidence triplets follow the SDD confidence tiers:

- `confidence >= 0.6`: create/update semantic edges.
- `0.3 <= confidence < 0.6`: write a `FACT` memory with `LOW_CONFIDENCE`
  status.
- `confidence < 0.3`: ignore the triplet.

## Retrieval Flow

```text
POST /api/v1/query or /api/v1/chat/completions
  -> L0 gate
  -> L1 Core plan and vector search
  -> L2 bounded graph traversal through templates
  -> L3 directory overviews
  -> L4 full documents and multi-hop commands
  -> Minimal Sufficient Context assembly
  -> Frontier answer
  -> optional NEED_MORE re-entry
```

The Core Model never emits raw Cypher. It selects bounded templates from
`templates/cypher/`, including path, neighborhood, temporal, history, prefix,
cross-reference, and vector-search templates.

## Sessions

Sessions live in the tenant-scoped session cache while active. A session can be
compacted during a long conversation. On close, Engram writes a durable
`SESSION_SUMMARY` memory and re-enqueues raw turns for durable ingest. The
session cache entry can then be deleted without losing long-term memory.

## Tenant Isolation

Tenant context is resolved from bearer tokens and bound to the request. The
tenant id scopes:

- PostgreSQL events, outboxes, bulk jobs, tenants, audit rows, and idempotency
- filesystem roots under `data/mem/{tenant_id}`
- KG node and edge properties plus every query template
- Redis session keys
- rate-limit buckets and quotas
- Admin UI/API tenant operations

The external `mem://` URI remains tenant-relative. The same `mem://` URI in
two tenants maps to separate filesystem paths and separate KG records.

## Workers And Recovery

| Worker | Trigger | Responsibility |
|---|---|---|
| Ingest workflow | PostgreSQL dispatch outbox | Process durable events with bounded Temporal retries. |
| Consolidation workflow | PostgreSQL task/dispatch transaction | Run overview, manifest, propagation, atomization, normalization, temporalization, integration, and unmerge tasks. |
| Reconciliation workflow | Temporal interval schedule | Repair missing dispatches without duplicating active workflows. |
| Stale-overview workflow | Temporal interval schedule | Discover and enqueue stale directory overviews. |
| Decay workflow | Temporal cron schedule | Recompute tenant-safe retrieval weights. |

When Temporal is disabled, local development uses the legacy in-process pollers
against the same PostgreSQL control plane. Those pollers are not part of the
production deployment.

## Provider Architecture

Core and Frontier providers are selected independently in config:

- `openai`
- `openai_compat`
- `openai_responses`
- `ollama`
- `local` for Core-only local-provider scaffolding

`api_base` is part of both Core and Frontier config. The default and production
configs use `openai_responses` with `muse-spark-1.3-contributor` at
`https://opencode.ai/zen/go/v1` and read `OPENCODE_GO_API_KEY`.

## Admin And Visualization

The Admin UI is served by the FastAPI app under `/admin/*` and uses the same
tenant/admin boundaries as the API. It includes:

- login and dashboard status
- ingest panel
- bulk upload with dry-run and rejected-row reporting
- sessions and memories views
- chat with retrieval traces
- KG graph visualization with type/status filters and hard result limits

The KG visualization uses vendored JavaScript assets under
`engram/admin/static/js/`; it does not depend on a CDN.

## Operational Interfaces

- Health: `/api/v1/health`, `/livez`, `/readyz`
- Metrics: `/metrics`
- PostgreSQL migrations: `python -m engram.cli migrate` (the
  `postgres-migrate` alias is retained)
- Schema/index init: `python -m engram.cli init`
- Smoke test: `python -m engram.cli smoke`
- KG rebuild: `python -m engram.cli rebuild-kg`
- Decay: `python -m engram.cli decay`
- Admin CLI: `python -m engram.cli admin ...`
