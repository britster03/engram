# Engram Documentation

Start here if you're new to the codebase. These docs complement the original
[`Engram_SDD.pdf`](../Engram_SDD.pdf); the production plan records subsequent agreed changes.

For the current production direction, start with the [production delivery plan](ORDERED_RETRIEVAL_PLAN.md) and [repository/product audit](PRODUCTION_PRODUCT_AUDIT.md), revised 2026-09-29. They distinguish implemented behavior from planned work and supersede conflicting historical architecture or delivery claims.

| Doc | What it covers | When to read it |
|---|---|---|
| [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) | Implemented retrieval, consolidation and durable chat; migration steps, checks and remaining release gates | First when assessing this implementation |
| [ORDERED_RETRIEVAL_PLAN.md](ORDERED_RETRIEVAL_PLAN.md) | Preserved retrieval order, complete product lifecycle, privacy, UX, milestones and release gates | First when planning production work |
| [RETRIEVAL_CONSOLIDATION_DESIGN.md](RETRIEVAL_CONSOLIDATION_DESIGN.md) | Evidence/temporal contracts, required BM25/vector RRF, source lineage, safe summary publication, Temporal coalescing and focused acceptance tests | When implementing retrieval or consolidation |
| [PRODUCTION_PRODUCT_AUDIT.md](PRODUCTION_PRODUCT_AUDIT.md) | Current component status, prioritized findings, evidence and validation limits | Before assuming a capability exists |
| [CONSOLIDATION_FLOW.md](CONSOLIDATION_FLOW.md) | Actual canonical/Temporal flow, legacy distinction and confirmed limitations | Before changing existing consolidation handlers |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Four-layer architecture, storage boundaries, component wiring | First — gets you oriented |
| [DATA_MODEL.md](DATA_MODEL.md) | mem:// filesystem, KG node/edge schema, control-plane tables | Before touching storage code |
| [FLOWS.md](FLOWS.md) | Ingest (7 steps) and retrieval cascade (L0→L4) sequence diagrams | Before touching the ingest worker or orchestrator |
| [TOKEN_COMPACTION.md](TOKEN_COMPACTION.md) | Environment-controlled 64K session compaction, bounded summaries, durable tasks, tokenizer readiness, and validation | When configuring compaction or deploying its workers |
| [API.md](API.md) | Every REST endpoint, request/response examples, auth, rate limits | When integrating a client |
| [RUNBOOK.md](RUNBOOK.md) | Deploy, backup, restore, migrate, rebuild-kg, decay, dashboards | When operating the service |
| [PRODUCTION.md](PRODUCTION.md) | Pre-flight checklist, incident playbook, capacity, upgrades | When going live |
| [SQLITE_AND_PRODUCTION_DEPLOYMENT_PLAN.md](SQLITE_AND_PRODUCTION_DEPLOYMENT_PLAN.md) | Historical decision record for the completed PostgreSQL-only conversion | For migration history only |
| [SCALING.md](SCALING.md) | Multi-tenancy, leader election, Kubernetes, Helm, client SDKs | When scaling beyond single-node |
| [FEATURES.md](FEATURES.md) | Authoritative "what's actually implemented" table | When verifying what's real vs aspirational |
| [VALIDATION.md](VALIDATION.md) | Record of live runs against real infra (OpenAI, Neo4j, gate classifier training) | When you need proof something actually works |
| [DEV_GUIDE.md](DEV_GUIDE.md) | Code layout, adding prompts / Cypher templates / consolidation tasks, test strategy | When extending Engram |
| [TRAINING.md](TRAINING.md) | Bootstrap mode, synthetic data, Gate classifier, Core model SFT + DPO | When training local models |

## Quick pointers

- Canonical config: [`config.yaml`](../config.yaml) mirrors SDD §15.
- Entry points: `engram.cli:main` (CLI) and `engram.api.app:app` (FastAPI).
- Production canonical memory and the control plane are in **PostgreSQL**.
  Neo4j, vectors and summaries are derived discovery/presentation structures.
  Filesystem memory descriptions in older documents refer to legacy compatibility paths.
- Test suite: `.venv/bin/pytest` covers unit, adapter, integration, and E2E layers.
