# Temporal Production Deployment

This deployment is durable single-host production, not host-fault-tolerant HA.
It uses PostgreSQL as Engram's control plane, Temporal's PostgreSQL persistence,
Neo4j as a rebuildable index, Redis for sessions/caches, and a named local volume
for authoritative memory files.

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

Back up the PostgreSQL volume/logical dumps, `engram_data` memory volume, and
Neo4j dump together. Restore PostgreSQL before starting workers; restore memory
files before rebuilding Neo4j. Test recovery with a separate Compose project.

Temporal workflows carry opaque event/task identifiers only. Conversation
payloads remain in PostgreSQL.
