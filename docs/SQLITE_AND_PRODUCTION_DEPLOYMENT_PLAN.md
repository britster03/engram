# SQLite Decision and Production Deployment Plan

> Historical decision record. Full removal was completed on 2026-09-09.
> PostgreSQL is now mandatory for production, development, and tests. The
> adapter, fallback configuration, legacy migration runner, and import command
> described below no longer exist.

## Decision

SQLite is not part of the production architecture. PostgreSQL is the only
supported production control plane, and Temporal is the only supported
production background-work coordinator.

SQLite should remain temporarily in the repository for:

1. fast, dependency-free unit and integration tests;
2. single-process local development when Temporal is disabled;
3. importing an existing SQLite ledger during a PostgreSQL cutover;
4. validating backward compatibility during the transition.

Deleting SQLite immediately would remove useful tests and the supported
migration source while leaving many modules coupled to `SqliteStore` types.
Keeping it as a production option, however, would be unsafe: SQLite cannot
coordinate independently scaled API, dispatcher, and Temporal-worker pods,
and a shared network filesystem does not make SQLite a distributed database.

## Current dependency analysis

| Area | Production requirement | SQLite disposition |
| --- | --- | --- |
| Events, extractions, outbox, linked entities | PostgreSQL transactionality | Remove SQLite from production configuration |
| Consolidation queue | PostgreSQL plus transactional Temporal dispatch | Remove SQLite from production configuration |
| Tenants and audit records | PostgreSQL | Keep SQLite adapter only for local tests |
| Temporal dispatch records | PostgreSQL only | No SQLite implementation is needed |
| Unit/integration tests | Fast isolated store | Keep until PostgreSQL test fixtures are inexpensive |
| Legacy data import | Read-only SQLite source | Keep importer for at least two stable releases |
| Legacy local worker | Single-process development only | Keep behind `temporal.enabled=false` |
| Neo4j rebuild | Configured control plane | Must never open SQLite implicitly in production |

## Target architecture

```text
API replicas
   │
   ├── PostgreSQL control plane
   │      ├── events/extractions/outboxes
   │      ├── consolidation tasks
   │      ├── tenants/audit
   │      └── Temporal dispatch outbox
   │
   ├── Redis session/cache service
   ├── Neo4j rebuildable search index
   └── shared canonical memory filesystem (RWX/snapshot-backed)

Dispatcher replicas ── PostgreSQL outbox ──► Temporal
Temporal workers   ◄──────────────────────── Temporal
Temporal schedules ── reconciliation / stale overview / decay
```

SQLite and the legacy pollers do not run anywhere in this production graph.

## Phase 0 — production safety fixes

Status: implemented in this change.

- Reject `temporal.enabled=true` unless the control-plane backend is PostgreSQL.
- Use PostgreSQL for production, staging, Helm, and raw Kubernetes configs.
- Create event/task and Temporal-dispatch rows in the same PostgreSQL transaction.
- Replace remaining SQLite-only linked-entity writes with backend-specific UPSERTs.
- Make `rebuild-kg` and tenant administration use the configured control plane.
- Make the legacy `engram migrate` command refuse PostgreSQL and direct operators
  to `engram postgres-migrate`.
- Separate API, Temporal worker, dispatcher, and maintenance responsibilities.
- Run PostgreSQL migrations before production rollout.
- Bind single-host management ports to loopback by default.
- Run application containers with a read-only root filesystem, dropped Linux
  capabilities, no privilege escalation, and bounded tmpfs mounts.

## Phase 1 — remove SQLite inheritance from production code

Target: next release.

1. Rename `AppState.sqlite` to `AppState.control_plane`.
2. Replace concrete `SqliteStore` annotations with repository protocols.
3. Split repositories by concern:
   - `EventRepository`
   - `ExtractionRepository`
   - `ConsolidationRepository`
   - `TenantRepository`
   - `AuditRepository`
   - `WorkflowDispatchRepository`
4. Make `PostgresStore` implement the protocols directly instead of inheriting
   from `SqliteStore`.
5. Remove SQL-dialect translation (`?` to `%s`, `datetime('now')` rewriting).
6. Add PostgreSQL integration tests for every repository operation using an
   ephemeral database in CI.

Exit criteria:

- no production module imports `engram.storage.sqlite`;
- no PostgreSQL path executes SQL containing `PRAGMA`, `julianday`, or
  `INSERT OR REPLACE`;
- PostgreSQL integration tests exercise ingest through consolidation;
- `rebuild-kg`, tenant CLI, retries, bulk ingest, and audit retention pass
  against PostgreSQL.

## Phase 2 — narrow SQLite to an explicit development adapter

Target: after Phase 1 is stable.

1. Move SQLite to an optional development dependency/module boundary.
2. Require an explicit `ENGRAM_PROFILE=local-sqlite` or equivalent local config;
   do not default production-like commands to SQLite.
3. Run a smaller adapter contract suite against SQLite and the full suite
   against PostgreSQL.
4. Emit a startup warning when SQLite is selected outside test/local mode.
5. Keep the read-only SQLite importer and migration verification utilities.

## Phase 3 — optional full SQLite removal

After at least two stable releases and all customer cutovers:

1. remove the legacy worker and SQLite migration runner;
2. archive the SQLite importer as a standalone conversion tool;
3. switch local development to a disposable PostgreSQL container;
4. delete the SQLite adapter and its adapter-only tests.

This phase is optional. SQLite has low maintenance cost once isolated, so the
decision should be based on support burden rather than architectural purity.

## Production rollout plan

1. Back up PostgreSQL and the canonical filesystem using a coordinated
   snapshot timestamp. Neo4j is secondary but should also be dumped when the
   recovery-time objective requires it.
2. Deploy an immutable application image by digest or release tag.
3. Run the forward-only Alembic migration and stop on any failure.
4. Roll out Temporal workers. During workflow-code changes, preserve Temporal
   determinism or use worker versioning before introducing incompatible code.
5. Roll out dispatcher replicas and verify all three Temporal schedules.
6. Roll out API replicas only after PostgreSQL, Redis, Neo4j, and the shared
   filesystem pass readiness checks.
7. Submit a canary ingest and verify this chain:
   `RECEIVED -> workflow dispatch -> COMPLETE -> consolidation COMPLETE`.
8. Verify cross-tenant isolation, retry a deliberately failed event, and test
   `INDEXED` crash recovery.
9. Monitor backlog, failed workflows, activity latency, PostgreSQL saturation,
   filesystem capacity/inodes, and model-provider errors through the soak window.

## Required operational controls

- PostgreSQL point-in-time recovery and tested restore procedure.
- Snapshot-backed RWX canonical filesystem with an explicit RPO/RTO.
- Temporal persistence backup according to the chosen deployment model.
- External secret management; no committed or command-line secrets.
- Network policies/firewall rules for PostgreSQL, Temporal, Redis, and Neo4j.
- TLS at the public edge and TLS/mTLS for off-cluster dependencies.
- Pod disruption budgets and at least two API, worker, and dispatcher replicas
  where the backing services and filesystem are highly available.
- Alerts for dispatch age, failed workflows, queue depth, worker absence,
  PostgreSQL connection exhaustion, and disk capacity.

## Acceptance gate

A release is production-ready only when all of the following are true:

- no production configuration contains `backend: sqlite`;
- migrations complete before application rollout;
- API readiness verifies the configured control plane, Redis, Neo4j, and filesystem;
- Temporal worker and dispatcher replicas are running on the configured queues;
- reconciliation, stale-overview, and decay schedules exist with overlap disabled;
- a restore drill reconstructs the service within the declared RTO;
- a staging soak completes without stuck dispatches or tenant crossover.
