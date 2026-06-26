# Engram

Engram is a FastAPI AI memory service based on the original
[Engram_SDD.pdf](Engram_SDD.pdf) v1.0. It gives agents durable long-term
memory through an event-sourced ingest pipeline, an authoritative `mem://`
filesystem store, a derived knowledge graph, and a shallow-to-deep retrieval
cascade that assembles Minimal Sufficient Context for a frontier model.

The current codebase also includes post-SDD extensions: first-class
multi-tenancy, an Admin UI, bulk upload, KG visualization, tenant-scoped rate
limits, disposable performance caches, and Ollama Cloud inference support.

## Current Capabilities

- Durable ingest: API paths commit event rows; the durable ingest worker is the
  only component that processes received ingest events.
- Filesystem-authoritative memory: Markdown files under `data/mem` are source
  of truth; Neo4j or the in-memory KG is a rebuildable derived index.
- Retrieval cascade: L0 gate, L1 planning and vector search, L2 graph
  traversal, L3 overviews, L4 full documents and multi-hop retrieval, with
  frontier `NEED_MORE` re-entry.
- SDD-visible memory behavior: low-confidence triplets become `FACT` nodes
  with `LOW_CONFIDENCE` status, and session close writes `SESSION_SUMMARY`
  memories.
- Tenant isolation: tenant context scopes SQLite rows, filesystem paths, KG
  nodes and edges, session cache keys, rate limits, admin operations, and
  idempotency.
- Admin surfaces: REST API, CLI, and browser Admin UI for login, dashboard,
  ingest, bulk upload, sessions, memories, retrieval traces, chat, and KG
  visualization.
- Provider support: Ollama Cloud by default, local Ollama,
  OpenAI-compatible providers, and a local provider scaffold.
- Operations: migrations, health/readiness checks, metrics, JSON logging,
  tracing hooks, reconciliation, consolidation, decay, and KG rebuild.

## Quickstart

```bash
python3.10 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'

cp .env.example .env
# Fill in ENGRAM_API_KEY, ENGRAM_ADMIN_KEY, OLLAMA_API_KEY,
# and NEO4J_ADMIN_PASSWORD. Do not commit .env.

docker compose up -d
python -m engram.cli migrate
python -m engram.cli init

uvicorn engram.api.app:app --host 127.0.0.1 --port 8000
```

Useful local URLs:

- API health: `http://127.0.0.1:8000/api/v1/health`
- Admin login: `http://127.0.0.1:8000/admin/login`
- Dashboard: `http://127.0.0.1:8000/admin/dashboard`
- Chat: `http://127.0.0.1:8000/admin/chat`
- KG visualization: `http://127.0.0.1:8000/admin/kg`
- Metrics: `http://127.0.0.1:8000/metrics`

## Ollama Cloud Inference

The default `config.yaml` uses Ollama Cloud for both Core and Frontier
inference:

```bash
export OLLAMA_API_KEY=...
uvicorn engram.api.app:app --host 127.0.0.1 --port 8000
```

The provider alias is `ollama_cloud`, the default base is
`https://ollama.com/api`, and the default cloud model is `kimi-k2.7-code:cloud`.
Local Ollama remains the `ollama` provider and maps to
`http://localhost:11434/v1`.

## API Surface

The public API is rooted at `/api/v1` and includes:

- `/health`, `/config`, `/ingest`, `/query`
- `/chat/completions`
- `/sessions/*`
- `/memories/*`
- `/events/{event_id}/retry`
- `/ingest/bulk` and `/ingest/bulk/{job_id}`
- `/kg/graph`
- `/consolidation/*`
- `/admin/*` tenant and audit endpoints

Bulk upload supports JSONL/NDJSON, CSV turn pairs, and ZIP uploads containing
`.txt` or `.md` documents. KG graph results are tenant-scoped and bounded by
depth and limit parameters.

## Testing And QA

```bash
.venv/bin/pytest -q -m 'not e2e'
.venv/bin/pytest -q
```

The e2e collection includes API and browser coverage. The Ollama Cloud live
smoke test skips unless `OLLAMA_API_KEY` is present in the environment.
Recent QA artifacts are kept under `qa-artifacts/` and include browser
screenshots/logs for login, dashboard, ingest, bulk upload, memories, sessions,
chat retrieval traces, and KG visualization.

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): current architecture and
  storage/caching boundaries.
- [docs/FEATURES.md](docs/FEATURES.md): implemented features, SDD coverage,
  post-SDD extensions, and known limits.
- [docs/API.md](docs/API.md): REST API reference. Some retained docs may lag
  the newest surfaces; treat the three files above as the canonical current
  overview.
- [docs/DATA_MODEL.md](docs/DATA_MODEL.md): memory file, KG, and SQLite data
  model.
- [docs/FLOWS.md](docs/FLOWS.md): ingest and retrieval sequence diagrams.
- [docs/PRODUCTION.md](docs/PRODUCTION.md), [docs/RUNBOOK.md](docs/RUNBOOK.md),
  and [docs/SCALING.md](docs/SCALING.md): deployment and operations notes.
- [docs/TRAINING.md](docs/TRAINING.md): optional model-training pipeline.

## Cache Policy

The SDD requires session caching for active conversation state. The current
implementation keeps that session cache and also adds two disposable,
non-authoritative performance caches:

- embedding cache: exact text plus model tag to embedding vector
- overview cache: tenant plus directory URI to rendered overview text

Deleting those performance caches must not delete memory or change tenant
ownership. They only avoid repeated computation and filesystem reads.
