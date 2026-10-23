# Runbook

Operational procedures for deploying, backing up, restoring, migrating,
and monitoring Engram.

Production operators must use [PRODUCTION.md](PRODUCTION.md) and
[TEMPORAL_PRODUCTION.md](TEMPORAL_PRODUCTION.md). PostgreSQL is required in
every environment.

## Local development

```bash
# 1. Environment
cp .env.example .env
vim .env   # fill API/model/database/Neo4j secrets

# 2. Lightweight backing services. Start PostgreSQL 16 separately and set
# ENGRAM_DATABASE_URL to its DSN.
docker compose up -d
docker compose ps

# 3. Python env
python3.10 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'

# 4. Migrate PostgreSQL and create Neo4j indexes
python -m engram.cli migrate
python -m engram.cli init

# 5. Run the API
uvicorn engram.api.app:app --port 8000

# 6. Smoke-test
python -m engram.cli smoke
```

## Backup

The **filesystem is authoritative for memory bodies**, while PostgreSQL is
authoritative for control-plane state. Back up both consistently. Neo4j is a
rebuildable projection.

Daily snapshot recommendation:

```bash
# Filesystem
tar czf backups/mem-$(date +%F).tgz ./data/mem

# PostgreSQL custom-format backup
pg_dump "$ENGRAM_DATABASE_URL" --format=custom \
  --file="backups/engram-$(date +%F).dump"

# Neo4j — optional; can always be rebuilt from the filesystem
docker compose exec neo4j neo4j-admin database dump --to-path=/backups neo4j
```

Recovery Point Objective is the snapshot interval unless PostgreSQL continuous
archiving is configured. Redis session data is ephemeral.

## Restore

### Complete disaster — only filesystem snapshot available

```bash
# 1. Restore the filesystem
tar xzf mem-2026-04-21.tgz -C ./data/mem

# 2. Restore the PostgreSQL control plane
pg_restore --clean --if-exists --no-owner \
  --dbname="$ENGRAM_DATABASE_URL" backups/engram-2026-09-09.dump

# 3. Apply forward migrations and rebuild Neo4j from the filesystem
python -m engram.cli migrate
python -m engram.cli rebuild-kg
```

`rebuild-kg` walks every `.md` in `./data/mem`, re-inserts the node with
its embedding, re-establishes CONTAINS edges, and (pass 2) replays
RELATES_TO edges from the PostgreSQL `extractions` and `linked_entities`
tables.

### Partial — Neo4j is corrupted, PostgreSQL + filesystem healthy

```bash
python -m engram.cli rebuild-kg
```

### Partial — Redis is gone

No action needed. Sessions evaporate (TTL-based); new sessions are created
on demand.

## PostgreSQL schema migrations

```bash
python -m engram.cli migrate
```

Alembic migrations live under `engram/migrations/alembic/versions/`. They are
forward-only. Take a verified PostgreSQL backup before upgrading; rollback is
performed by restoring that backup, not by running a destructive downgrade.

## Soft memory decay

```bash
# Manual run (from cron)
python -m engram.cli decay
```

Recommended schedule: daily at 03:00 local time (override via
`decay.schedule` in `config.yaml`). The CLI reads the config, picks the
appropriate preset (`personal_conversation` / `coding_agent` /
`knowledge_base`), and batch-writes `retrieval_weight` values.

## Observability

### Metrics

Prometheus scrape target: `GET http://engram:8000/metrics`

Key alerts:

| Alert | Threshold | What to check |
|---|---|---|
| Ingest pipeline failing | `engram_ingest_events_total{final_status="FAILED"}` rising | Check Core Model provider health; inspect `events.error_message` |
| Consolidation saturated | `engram_consolidation_queue_depth > 5000` sustained | Check Core Model rate limits; may need to increase `max_concurrent_tasks` |
| L0 gate false-negatives | `engram_l0_gate_decisions{decision="BYPASS"}` share grows while user complaints grow | Gate is returning BYPASS on queries that need memory; consider training the classifier or tuning regex patterns |
| Shallow-bias miss | `engram_query_depth_predicted_vs_reached{predicted="L1",reached="L4"}` growing | Core Model under-predicting depth; fine-tune needed (§14.2.6) |
| Frontier re-entries | `engram_reentries_per_query` p95 > 1 | MSC assembly missing coverage; check decay / dormant_floor |

### Logs

Structured JSON logs at INFO by default. Every Core Model call and every
worker transition is logged with `task_type`, `prompt hash`, tokens,
latency, and success/failure. Pipe to your log aggregator of choice.

### Health

```bash
curl http://engram:8000/api/v1/health | jq
```

`.status` is `healthy` only when all four components are reachable.

## Configuration changes

Config is loaded once at process boot. Changes to `config.yaml` require a
service restart. Secrets come from environment variables referenced in the
YAML via `${VAR}` syntax; rotating a secret means updating the env and
restarting the process.

## Capacity planning

Rough sizing for the single-node deployment (§13.1):

| Dimension | Rule of thumb |
|---|---|
| Memory nodes per GB of Neo4j page cache | ~250k with 384-dim vectors and ~200-byte abstracts |
| PostgreSQL rows per ingest | At least one event plus outbox/dispatch and extracted-state rows |
| Redis keys per concurrent session | 1; value size ~1kB/turn |
| Filesystem bytes per memory | ~1–4 kB for ENTITY/FACT; overview.md up to 8 kB |

Scaling path (§13.1) when the single-node limit is hit:

1. Neo4j Causal Cluster — writer/reader role separation becomes
   enforceable at the database level.
2. Redis Sentinel or Redis Cluster for session cache HA.
3. NFS + rsync, or S3-backed filesystem, for the authoritative mem:// store.
4. Multiple stateless API replicas behind a load balancer; swap the
   per-process token bucket for a Redis-backed distributed bucket.
