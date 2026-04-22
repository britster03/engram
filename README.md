# Engram

Production implementation of the **AI Memory Management System** defined in
[`Engram_SDD.pdf`](Engram_SDD.pdf) v1.0.

Engram gives conversational agents durable long-term memory through a
retrieval cascade that enforces the **Minimal Sufficient Context** principle:
deliver exactly enough context to the frontier LLM, and no more.

## What's inside

* Full ingest pipeline: event-sourced with outbox, idempotent, crash-safe.
* Full retrieval cascade: L0 gate → L1 plan + vector search → L2 graph
  traversal → L3 overviews → L4 full documents + multi-hop, with fused
  plan-judge calls at L2+ and frontier re-entry on NEED_MORE.
* Three-model inference stack: BGE-Small-EN embeddings, swappable Core
  Model (Anthropic API by default; local Qwen3.5-0.8B via LoRA supported),
  any frontier LLM.
* Conflict resolution with DUPLICATE / CONTRADICTION / CO_EXISTENCE + SUPERSEDES.
* Entity linking with cosine + name-overlap thresholds + LLM disambiguation.
* Memory consolidation with overview generation, manifest regeneration,
  ancestor propagation, atomization, normalization, temporalization, and
  integration task types.
* Session management with automatic compaction and re-ingest of
  uncompacted turns (§8.3.2).
* Soft memory decay with percentile-normalised recency / frequency /
  centrality.
* Reconciliation worker for stuck-state recovery and directory staleness.
* REST API matching §11.1, bearer-token auth, per-bucket rate limiting,
  Prometheus metrics at `/metrics`.
* CLI: `engram init | migrate | health | smoke | rebuild-kg | decay`.

## Quickstart (dev)

```bash
# 1. Python + venv
python3.10 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'

# 2. Secrets
cp .env.example .env
# Fill in ENGRAM_API_KEY, CORE_MODEL_API_KEY, FRONTIER_LLM_API_KEY,
# NEO4J_ADMIN_PASSWORD — placeholders like "change-me-*" are refused at boot.

# 3. Backing services
docker compose up -d
docker compose ps   # wait healthy

# 4. Initialise schemas + indexes
python -m engram.cli migrate
python -m engram.cli init

# 5. Run the API
uvicorn engram.api.app:app --port 8000

# 6. Smoke-test end-to-end
python -m engram.cli smoke
```

## Production deployment

```bash
# Place TLS certs in deploy/tls/ (see deploy/tls/README.md)
# Put real secrets in .env.prod
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
docker compose exec engram engram migrate
docker compose exec engram engram init
curl -fsS https://engram.example.com/readyz | jq
```

Full production runbook at [docs/PRODUCTION.md](docs/PRODUCTION.md) — covers
pre-flight checklist, incident playbook, capacity planning, and the scope
limits of this deployment (single-tenant, single-region — see the doc for
what is and isn't in scope).

## Documentation

A new engineer should be able to read [`docs/`](docs/) cover-to-cover and
understand the system. Start with [`docs/README.md`](docs/README.md).

* [ARCHITECTURE](docs/ARCHITECTURE.md) — four-layer architecture, storage boundaries, component wiring.
* [DATA_MODEL](docs/DATA_MODEL.md) — mem:// filesystem, KG schema, SQLite tables, conflict rules, decay formulas.
* [FLOWS](docs/FLOWS.md) — ingest pipeline and retrieval cascade sequence diagrams.
* [API](docs/API.md) — every REST endpoint with request/response examples.
* [RUNBOOK](docs/RUNBOOK.md) — deploy, backup, restore, migrate, rebuild, decay, alerting.
* [DEV_GUIDE](docs/DEV_GUIDE.md) — code layout, extension recipes, test strategy, design invariants.
* [TRAINING](docs/TRAINING.md) — bootstrap mode, trace collection, Gate classifier, Core SFT, DPO.

## Testing

```bash
.venv/bin/pytest -q
```

All tests run without Docker, without API keys, without network —
LLM / embeddings / Neo4j are replaced by deterministic stubs (see
`tests/integration/stubs.py` and `tests/integration/fake_neo4j.py`).

For real end-to-end validation against live services, run
`python -m engram.cli smoke` (requires Docker up + API keys in `.env`).

## Configuration

Canonical config: [`config.yaml`](config.yaml) (mirrors SDD §15.1 exactly).
Secrets come from environment variables referenced as `${VAR}`. The config
loader refuses to start if any required secret is unset.

## License & attribution

Implements the architecture specified in `Engram_SDD.pdf` (v1.0, April 2026).
