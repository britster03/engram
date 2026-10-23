# Temporal Production Deployment

This deployment is durable single-host production, not host-fault-tolerant HA.
It uses PostgreSQL for canonical source memory and the control plane, Temporal's
PostgreSQL persistence, Neo4j as a rebuildable index, and Redis for caches.
Development uses the same pipeline in an isolated database/Temporal namespace.
See [canonical consolidation](CONSOLIDATION_FLOW.md) for migration 0009 and
coordinated API/worker rollout requirements.

## First deployment

```bash
# Edit the existing .env, then keep it owner-readable only.
chmod 600 .env
./scripts/deploy.sh
```

`deploy.sh` runs the preflight checks, builds the application image, starts the
core API/Temporal stack, and waits for health checks. By default it excludes
Nginx, Prometheus, and Grafana. Add `--with-nginx` to start only the HTTPS
edge proxy, or `--full` to also start Prometheus and Grafana. Both Nginx modes
require trusted `deploy/tls/fullchain.pem` and `deploy/tls/privkey.pem` files.
Use `--no-build` to reuse an already-built `engram:latest` image.

Temporal UI binds to port 8080 on the host. PostgreSQL, Temporal gRPC, Neo4j,
and Redis remain un-published on the internal Compose network. Restrict UI
access to trusted LAN/VPN clients with the host firewall.

The Compose project runs one-shot Temporal schema and namespace setup before the
dedicated API, Temporal worker, and dispatcher services. The dispatcher creates
or updates Temporal schedules for reconciliation, stale-overview scanning, and
decay; the Temporal worker executes them on the maintenance task queue.
Do not start the legacy in-process workers when `temporal.enabled` is true.

The standalone `engram-reconciler` and `engram-decay` services are retained
under the `legacy-maintenance` Compose profile for controlled rollback only.
Do not run that profile while Temporal maintenance schedules are active.

## Control-plane migrations

PostgreSQL is the only supported control plane. The Compose migration service
runs forward-only Alembic migrations before API, worker, and dispatcher
rollouts. Existing installations should take a verified database backup before
deploying a new migration revision.

## Backup and recovery

Back up canonical PostgreSQL and Temporal persistence with a tested restore
procedure. Neo4j can be rebuilt from canonical PostgreSQL. Preserve historical
legacy files separately until their explicit import is verified. Test recovery
with a separate Compose project before reopening workers or writes.

Temporal workflows carry opaque event/task identifiers only. Conversation
payloads remain in PostgreSQL.

## Ingestion terminal-state recovery

Updated ingestion, code-ingestion and consolidation workflows keep retrying
terminal bookkeeping until PostgreSQL accepts it. A Temporal patch marker
preserves replay of executions that already scheduled the old bounded activity.
The dispatcher also checks acknowledged `STARTED` dispatches every 30 seconds,
using the exact recorded run ID. Failed/canceled/terminated/timed-out executions
become explicit failed database work, without overwriting committed events or
newer generations. A completed workflow without durable aggregate completion is
reported as failed for investigation and explicit retry. Running workflows are
left active. Visibility/network failures do not authorize a new workflow; their
checks rotate so an unavailable record cannot starve other repairs. Retain
Temporal execution history longer than the maximum expected database outage;
expired visibility cannot reliably establish an execution's terminal status.

Code retries retain the original job ID, artifact envelope and dedicated task
queue. Failure recording updates both the upload event and dispatch together.
Retrying an execution while its dispatch is still active returns a conflict;
terminal bookkeeping or status reconciliation must complete first.
