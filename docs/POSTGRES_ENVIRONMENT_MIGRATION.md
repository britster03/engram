# PostgreSQL Control-Plane Migration by Environment

> Historical cutover record. The repository is now PostgreSQL-only. Do not run
> the removed `migrate-control-plane.sh` or legacy import commands referenced in
> the original procedure below.

Engram memory files remain filesystem-authoritative. This migration moves only
the durable control plane: events, outboxes, extraction data, consolidation
tasks, tenancy, audit data, bulk-job data, and Temporal dispatch records.

## Environment profiles

| Environment | Config | Database | Temporal |
| --- | --- | --- | --- |
| Development | `config.dev.postgres.yaml` | `engram_dev` | Optional for rapid local work |
| CI | Test-only DSN | Disposable `engram_test` | Required for Temporal-specific tests |
| Staging | `config.staging.yaml` | `engram_staging` | Required |
| Production | `config.prod.yaml` | `engram` | Required |

Never point two environments at the same Postgres database, Neo4j database,
Redis namespace, Temporal namespace, or filesystem data directory.

## Safe cutover procedure

1. Stop legacy API, ingest, consolidation, and reconciliation workers.
2. Confirm that no event or consolidation task is still `PROCESSING`.
3. Snapshot the SQLite database and memory filesystem.
4. Apply the forward-only schema migration to the empty target, then run
   read-only preflight:

   ```bash
   ./scripts/migrate-control-plane.sh --environment staging \
     --env-file .env.staging --source /backups/event_ledger.db --prepare
   ```

5. The preflight must report `target_empty: true`. Apply only after the
   environment-specific approval and maintenance window:

   ```bash
   ./scripts/migrate-control-plane.sh --environment staging \
     --env-file .env.staging --source /backups/event_ledger.db \
     --apply --confirm staging
   ```

6. Validate the JSON report. Source and target counts must match for every
   imported table. `workflow_dispatch_count` can be non-zero because pending
   events/tasks are intentionally recreated as Temporal dispatches.
7. Start the Temporal worker and dispatcher before reopening writes.

## Rollback

Before reopening write traffic, failure means: stop the Postgres/Temporal
application workers, restore the prior SQLite configuration, and restart only
the legacy workers. Keep the SQLite snapshot read-only for at least 90 days.
Never run SQLite and Postgres workers against the same memory filesystem.
