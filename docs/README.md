# Engram Documentation

Start here if you're new to the codebase. These docs complement the
authoritative [`Engram_SDD.pdf`](../Engram_SDD.pdf).

| Doc | What it covers | When to read it |
|---|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Four-layer architecture, storage boundaries, component wiring | First — gets you oriented |
| [DATA_MODEL.md](DATA_MODEL.md) | mem:// filesystem, KG node/edge schema, SQLite tables | Before touching storage code |
| [FLOWS.md](FLOWS.md) | Ingest (7 steps) and retrieval cascade (L0→L4) sequence diagrams | Before touching the ingest worker or orchestrator |
| [API.md](API.md) | Every REST endpoint, request/response examples, auth, rate limits | When integrating a client |
| [RUNBOOK.md](RUNBOOK.md) | Deploy, backup, restore, migrate, rebuild-kg, decay, dashboards | When operating the service |
| [PRODUCTION.md](PRODUCTION.md) | Pre-flight checklist, incident playbook, capacity, upgrades | When going live |
| [SCALING.md](SCALING.md) | Multi-tenancy, leader election, Kubernetes, Helm, client SDKs | When scaling beyond single-node |
| [FEATURES.md](FEATURES.md) | Authoritative "what's actually implemented" table | When verifying what's real vs aspirational |
| [VALIDATION.md](VALIDATION.md) | Record of live runs against real infra (OpenAI, Neo4j, gate classifier training) | When you need proof something actually works |
| [DEV_GUIDE.md](DEV_GUIDE.md) | Code layout, adding prompts / Cypher templates / consolidation tasks, test strategy | When extending Engram |
| [TRAINING.md](TRAINING.md) | Bootstrap mode, synthetic data, Gate classifier, Core model SFT + DPO | When training local models |

## Quick pointers

- Canonical config: [`config.yaml`](../config.yaml) mirrors SDD §15.
- Entry points: `engram.cli:main` (CLI) and `engram.api.app:app` (FastAPI).
- Authoritative data: filesystem under `./data/mem`. Neo4j is a **derived index**; SQLite is **control plane**.
- Test suite: `.venv/bin/pytest` — 56 tests covering unit + stub-based integration flows.
