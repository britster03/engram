# Engram

Engram is a FastAPI AI memory service based on the original
[Engram_SDD.pdf](Engram_SDD.pdf) v1.0. It gives agents durable long-term
memory through a PostgreSQL-canonical ingest pipeline, Temporal workflows,
a derived knowledge graph, and an ordered retrieval cascade that assembles
Minimal Sufficient Context for a frontier model. Development and production
use the same canonical pipeline; Markdown files are offline exports.

The current codebase also includes post-SDD extensions: first-class
multi-tenancy, an Admin UI, bulk upload, KG visualization, tenant-scoped rate
limits, disposable performance caches, and OpenCode Go inference support.

## Current Capabilities

- Durable ingest: API paths commit event rows and a transactional dispatch
  outbox; Temporal activities process accepted ingest events.
- PostgreSQL-canonical memory: source versions, typed claims, evidence and
  overview lineage live in PostgreSQL. Neo4j is a rebuildable projection.
- Canonical consolidation: episodes, collections and projects request
  revision-checked overviews. Prompt, model, quality policy and budget changes
  get distinct immutable artifacts. Overview queries reverify selected claim
  IDs in PostgreSQL instead of treating summary prose as evidence.
- Ordered retrieval: general knowledge, current session, past-session originals,
  canonical facts, semantic discovery and graph traversal. Overview queries
  use consolidated claim hints with PostgreSQL verification.
- SDD-visible memory behavior: low-confidence triplets become `FACT` nodes
  with `LOW_CONFIDENCE` status, and session close writes `SESSION_SUMMARY`
  memories.
- Tenant isolation: tenant context scopes PostgreSQL rows, filesystem paths, KG
  nodes and edges, session cache keys, rate limits, admin operations, and
  idempotency.
- Admin surfaces: REST API, CLI, and browser Admin UI for login, dashboard,
  ingest, bulk upload, sessions, memories, retrieval traces, chat, and KG
  visualization.
- Provider support: Muse Spark through OpenCode Go by default, plus
  OpenAI-compatible providers and a local provider scaffold.
- Operations: migrations, health/readiness checks, metrics, JSON logging,
  tracing hooks, reconciliation, consolidation, decay, and KG rebuild.

## Quickstart

Prerequisites:

- Python 3.10 or newer
- Docker with Compose support
- PostgreSQL 16 (local or remotely reachable)
- An Ollama Cloud API key for hosted inference

Manual setup:

```bash
python3.10 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'

cp .env.example .env
```

Open `.env` and fill in the required values. `.env` is gitignored and must not
be committed.

```bash
# Generate values to paste into .env.
printf 'ENGRAM_API_KEY=engram-dev-%s\n' "$(openssl rand -hex 16)"
printf 'ENGRAM_ADMIN_KEY=engram-admin-%s\n' "$(openssl rand -hex 16)"
printf 'ENGRAM_SECRET_KEY=%s\n' "$(openssl rand -hex 32)"
```

Set these fields in `.env`:

- `ENGRAM_API_KEY`: default local API access key
- `ENGRAM_ADMIN_KEY`: Admin UI and admin API key
- `ENGRAM_SECRET_KEY`: stable Admin UI session signing key
- `OPENCODE_GO_API_KEY`: Muse Spark inference key for OpenCode Go
- `NEO4J_ADMIN_PASSWORD`: local Neo4j password
- `ENGRAM_DATABASE_URL`: PostgreSQL DSN for the mandatory control plane

Load the local environment before running CLI or API commands:

```bash
set -a
source .env
set +a

docker compose --project-name engram-dev up -d
docker compose ps

python -m engram.cli migrate
python -m engram.cli init
python -m engram.cli health

# Run each process in its own terminal with the same environment/config:
python -m engram.temporal.worker --queue all
python -m engram.temporal.dispatcher
uvicorn engram.api.app:app --host 127.0.0.1 --port 8000
```

Automated local bootstrap is also available:

```bash
./scripts/quickstart.sh
```

Fill in `.env` before running the script. It creates a virtualenv, installs
dependencies, starts PostgreSQL/Neo4j/Redis/Temporal in the `engram-dev` Compose
project, applies migrations, and starts the API, dispatcher and all worker
queues. It constructs a local PostgreSQL DSN on port 5433 without overwriting
your existing database credentials. Set `ENGRAM_DEV_DATABASE_URL` explicitly
to use another development database. Production uses `scripts/deploy.sh` and
`config.prod.yaml`; development never stops production processes.

API-only startup cannot process the durable queues. See
[the canonical runtime guide](docs/CONSOLIDATION_FLOW.md) for migration order,
remaining limits and the automatic-chat-memory setting.

Useful local URLs:

- API health: `http://127.0.0.1:8000/api/v1/health`
- Admin login: `http://127.0.0.1:8000/admin/login`
- Dashboard: `http://127.0.0.1:8000/admin/dashboard`
- Chat: `http://127.0.0.1:8000/admin/chat`
- KG visualization: `http://127.0.0.1:8000/admin/kg`
- Metrics: `http://127.0.0.1:8000/metrics`

The bootstrap `ENGRAM_API_KEY` creates the default local tenant on first boot.
To create additional tenant-scoped Engram API keys, use the admin CLI after
loading `.env`:

```bash
python -m engram.cli admin create-tenant dev --display-name "Local Dev"
python -m engram.cli admin mint-key dev
```

Generated tenant keys are printed once and stored only as hashes.

## Muse Spark Inference

The default `config.yaml` uses Muse Spark through OpenCode Go's Responses API
for both Core and Frontier inference:

```bash
export OPENCODE_GO_API_KEY=...
uvicorn engram.api.app:app --host 127.0.0.1 --port 8000
```

The provider is `openai_responses`, the base is
`https://opencode.ai/zen/go/v1`, and the model is
`muse-spark-1.3-contributor`. Ollama Cloud is not supported.

## API Surface

The public API is rooted at `/api/v1` and includes:

- `/health`, `/config`, `/ingest`, `/query`
- `/chat/completions`
- `/sessions/*`
- `/memories/*`
- `/events/{event_id}/retry`
- `/ingest/bulk` and `/ingest/bulk/{job_id}`
- `/kg/graph`, `/kg/projects`, `/kg/code-map`, and `/kg/node-details`
- `/consolidation/*`
- `/admin/*` tenant and audit endpoints

Bulk upload supports JSONL/NDJSON, CSV turn pairs, and ZIP uploads containing
`.txt`/`.md` documents. A ZIP can also contain `.py`, `.ts`, `.tsx`, `.js`, or
`.jsx` source files when `project_name` is supplied. Those files are parsed into
a tenant-scoped Code map of folders, files, symbols, imports, calls, and
inheritance; re-uploading the same project name refreshes only that project's
structural graph. KG results are bounded by depth and limit parameters.

## Testing And QA

```bash
.venv/bin/pytest -q -m 'not e2e'
.venv/bin/pytest -q
```

The e2e collection includes API and browser coverage. The Muse Spark live
smoke test skips unless `OPENCODE_GO_API_KEY` is present in the environment.
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
- [docs/DATA_MODEL.md](docs/DATA_MODEL.md): memory file, KG, and PostgreSQL data
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
