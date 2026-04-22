# Runbook

Operational procedures for deploying, backing up, restoring, migrating,
and monitoring Engram.

## Deploy (single node)

```bash
# 1. Environment
cp .env.example .env
vim .env   # fill ENGRAM_API_KEY, CORE_MODEL_API_KEY, FRONTIER_LLM_API_KEY,
           # NEO4J_ADMIN_PASSWORD

# 2. Backing services
docker compose up -d
docker compose ps           # wait for both services healthy

# 3. Python env
python3.10 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'

# 4. Create indexes + migrate schema
python -m engram.cli migrate          # applies any pending migrations
python -m engram.cli init             # creates Neo4j indexes + SQLite schemas

# 5. Run the API
uvicorn engram.api.app:app --port 8000

# 6. Smoke-test
python -m engram.cli smoke
```

## Backup

The **filesystem is authoritative**. A consistent snapshot of `./data/mem`
is the primary backup. Neo4j is derivable via `rebuild-kg`. SQLite can be
backed up with a simple file copy while WAL is enabled (use `.backup`
command or online backup tools for high-traffic deployments).

Daily snapshot recommendation:

```bash
# Filesystem
tar czf backups/mem-$(date +%F).tgz ./data/mem

# SQLite (WAL-aware copy)
sqlite3 ./data/event_ledger.db ".backup ./backups/event_ledger-$(date +%F).db"

# Neo4j — optional; can always be rebuilt from the filesystem
docker compose exec neo4j neo4j-admin database dump --to-path=/backups neo4j
```

Recovery Point Objective: up to your snapshot interval. Committed memories
are durable on disk; pending work (in-flight SQLite events, Redis session
cache) is the only loss window.

## Restore

### Complete disaster — only filesystem snapshot available

```bash
# 1. Restore the filesystem
tar xzf mem-2026-04-21.tgz -C ./data/mem

# 2. Start backing services fresh
docker compose up -d

# 3. Init + rebuild Neo4j from the filesystem
python -m engram.cli init
python -m engram.cli rebuild-kg
```

`rebuild-kg` walks every `.md` in `./data/mem`, re-inserts the node with
its embedding, re-establishes CONTAINS edges, and (pass 2) replays
RELATES_TO edges from the `extractions` + `linked_entities` tables in
SQLite.

### Partial — Neo4j is corrupted, SQLite + filesystem healthy

```bash
python -m engram.cli rebuild-kg
```

### Partial — Redis is gone

No action needed. Sessions evaporate (TTL-based); new sessions are created
on demand.

## Schema migrations (§13.5)

```bash
python -m engram.cli migrate
```

Migration scripts live under `engram/migrations/scripts/`. Each module
exposes `SCHEMA_VERSION: int` and `def upgrade(ctx: MigrationContext) -> None`.
The runner:

1. Sets `meta.maintenance_mode = 1` so clients see 503 from `/ingest`.
2. Runs each pending migration in version order.
3. Updates `meta.schema_version` after each.
4. Clears `maintenance_mode`.

Rollback strategy: restore the pre-migration filesystem + SQLite snapshot.
The SDD is explicit that this is the recovery model — we do not support
schema downgrades.

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
| SQLite events per ingest/s | 1:1; WAL mode comfortably handles 5k writes/s |
| Redis keys per concurrent session | 1; value size ~1kB/turn |
| Filesystem bytes per memory | ~1–4 kB for ENTITY/FACT; overview.md up to 8 kB |

Scaling path (§13.1) when the single-node limit is hit:

1. Neo4j Causal Cluster — writer/reader role separation becomes
   enforceable at the database level.
2. Redis Sentinel or Redis Cluster for session cache HA.
3. NFS + rsync, or S3-backed filesystem, for the authoritative mem:// store.
4. Multiple stateless API replicas behind a load balancer; swap the
   per-process token bucket for a Redis-backed distributed bucket.
