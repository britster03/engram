# Production Deployment

This document is the operator's runbook. Pair it with
[RUNBOOK.md](RUNBOOK.md) (day-to-day ops) and [ARCHITECTURE.md](ARCHITECTURE.md)
(what the system looks like).

## Scope of "production-ready"

What this codebase is ready for, today:

- **Single-tenant** deployments (one API key, one operator, one organisation).
- **Multi-tenant** deployments: tenant isolation enforced at the filesystem
  (per-tenant sub-directory), SQLite (`tenant_id` column on every row),
  Neo4j (`tenant_id` property on every node, filtered in every Cypher
  template), session cache (`session:{tenant_id}:{session_id}` keys).
  Per-tenant rate limits + quotas via the admin API. Admin CRUD +
  key rotation audited in an append-only log. See [SCALING.md](SCALING.md).
- **Air-gapped / offline** deployments via the Ollama backend — point
  `core_model.provider=ollama` at a local Ollama server and no outbound
  HTTPS is required. Hosted inference defaults to Ollama Cloud.
- Single primary region. Backups replicated off-host.
- 1–2 gunicorn workers behind nginx TLS, or a Kubernetes deployment (3+
  replicas with HPA). Leader-election via Redis lease keeps the
  consolidation + reconciliation workers as singletons across replicas.
- Throughput up to ~20 queries/s and ~200 ingests/s on a 4-vCPU / 16 GB
  host with Neo4j co-located. Empirically verified against remote model APIs
  + real Neo4j in [VALIDATION.md](VALIDATION.md).

What it is **not** ready for:

- **Public SaaS launch** — the multi-tenancy plumbing is there, but a SaaS
  needs additional product surface that this repo does not ship: billing
  / usage metering (tokens consumed per tenant → invoicing), a self-serve
  signup flow, tenant-scoped admin dashboards, SLA enforcement,
  abuse/spam content moderation, a privacy review, and a DSAR pipeline
  for hard-delete per user (only soft-delete via `/retire` is exposed today).
  Everything below the product surface — isolation, quotas, audit — is in
  place.
- **Multi-region active/active**. Requires cross-region filesystem +
  Neo4j replication (S3 cross-region replication, Neo4j AuraDB
  multi-region, etc.). Not boxed up here — needs cloud-specific
  integration.
- **Adversarial content at scale** — the Cypher template whitelist,
  per-tenant DB scoping, input-size caps, and defensive validators
  (e.g. entity-linker rejects non-candidate URIs) are defence-in-depth,
  but LLM prompt-injection mitigations in the extract / L1 / LN prompts
  haven't been red-teamed. A motivated attacker feeding crafted turn
  pairs could probably warp their own tenant's KG (but not others', per
  the tenant boundary). Add content moderation + an adversarial test
  corpus before opening the `/ingest` surface to untrusted submitters.

## Pre-flight checklist

Before the first boot:

- [ ] Real secrets in `.env.prod`. The config loader refuses placeholders
      (`change-me`, `CHANGE_ME`, etc).
- [ ] TLS certs in `deploy/tls/` (see `deploy/tls/README.md`).
- [ ] DNS record points at this host.
- [ ] Filesystem at `./data/mem` (or whatever `ENGRAM_DATA_DIR` names) has
      at least 50 GB free and is on a volume that is snapshot-backed.
- [ ] Ollama Cloud account has quota sufficient for the expected call volume.
      Rough ratio: each query consumes 2–5 Core Model calls plus 1 Frontier
      call; each ingest consumes 2–4 Core Model calls.
- [ ] Neo4j admin password rotated from the `docker-compose.yml` default.
- [ ] Grafana admin password set via `GRAFANA_ADMIN_PASSWORD` env var (not
      committed).
- [ ] Prometheus alertmanager destination configured (Slack / PagerDuty).
- [ ] Backup cron scheduled (see "Backup / restore" below).
- [ ] Logrotate entry in place for gunicorn access logs.

## Boot procedure

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
docker compose exec engram engram migrate
docker compose exec engram engram init
docker compose ps       # all services Healthy
```

Validate end-to-end:

```bash
curl -fsS https://engram.example.com/livez
curl -fsS https://engram.example.com/readyz | jq
curl -fsS -H "Authorization: Bearer ${ENGRAM_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"session_id":"s1","turn_pair":{"user":{"content":"I just joined Meta","turn_idx":0},"assistant":{"content":"Nice.","turn_idx":1}}}' \
  https://engram.example.com/api/v1/ingest
# (wait a few seconds for the durable worker)
curl -fsS -H "Authorization: Bearer ${ENGRAM_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"query":"Where does the user work?"}' \
  https://engram.example.com/api/v1/query | jq .answer
```

## Architecture on a host

```
           ┌──────────┐
 client ──▶│  nginx   │ 443 (TLS termination, per-IP rate limit,
           │          │      body size caps, SSE-friendly buffering)
           └────┬─────┘
                │ 8000 upstream
                ▼
           ┌──────────┐
           │  engram  │ gunicorn + uvicorn worker
           │   app    │   - REST API
           │          │   - durable ingest worker (daemon thread)
           │          │   - consolidation worker (daemon thread)
           │          │   - reconciliation worker (daemon thread)
           └─┬────┬───┘
             │    └────────────────────────┐
       ┌─────▼─────┐  ┌──────────┐  ┌─────▼─────┐  ┌──────────────┐
       │   Neo4j   │  │  Redis   │  │  SQLite   │  │ Filesystem   │
       │ (KG idx)  │  │ (session │  │ (ledger)  │  │ (mem:// au-  │
       │           │  │  cache)  │  │           │  │  thoritative)│
       └───────────┘  └──────────┘  └───────────┘  └──────────────┘
```

A single gunicorn worker is the supported production configuration. Adding
workers is **safe for the API surface** (requests are stateless) but the
current durable ingest worker / consolidation worker / reconciliation worker
are per-process daemon threads. Multi-worker support requires either:

1. Running the background workers in a separate container (supervisor
   process) that shares `./data/` with the API workers, or
2. Leader election (Redis SET NX) so only one worker runs the background
   loops.

For Phase 1 we recommend approach (1) as the incremental path.

## Resilience

**Durable ingest.** The API endpoint does exactly one synchronous write: an
INSERT into the SQLite event ledger. If this process crashes immediately
after the 202 response, the next process to boot picks the event up on the
first poll (default 1 second). Reconciliation catches rarer stuck states
every 60 seconds.

**LLM retries + circuit breakers.** Every outbound model call goes through
`engram/resilience.py::resilient`. Breakers are per-provider (one for
`core`, one for `frontier`); 5 failures in a rolling window open the
breaker for 30s.

**Graceful degradation.** When Neo4j or the embedding service is
unavailable, vector search returns empty and the cascade continues from
session context alone. When the Core Model is unavailable, the
orchestrator falls back to a minimal plan (raw-query vector search + session
context) and terminates the cascade. When the Frontier LLM is unavailable,
`/query` returns HTTP 200 with a user-facing "temporarily unavailable"
answer and metadata — never 500.

**Backpressure.** `/api/v1/ingest` returns 503 with `Retry-After: 30` when
the consolidation queue depth exceeds `max_backlog` (default 10 000).

## Backup / restore

### Daily backup (recommended cron @ 02:30 local)

```bash
DATE=$(date +%F)
# Filesystem (authoritative)
tar --exclude='*.tmp' -czf /var/backups/engram/mem-${DATE}.tar.gz /var/lib/engram/mem

# SQLite — use the online backup API (WAL-aware)
sqlite3 /var/lib/engram/event_ledger.db \
  ".backup /var/backups/engram/ledger-${DATE}.db"

# Neo4j (optional — always re-derivable)
docker compose exec -T neo4j neo4j-admin database dump \
  --to-path=/backups neo4j

# Ship to off-host storage
aws s3 cp /var/backups/engram/ s3://engram-backups/$(hostname)/${DATE}/ --recursive
```

Retain 30 dailies, 6 weeklies, 12 monthlies.

### Restore

| Lost | Action |
|---|---|
| Redis | Nothing. Sessions are ephemeral. |
| Neo4j | `engram rebuild-kg` (walks the filesystem + replays extractions). |
| SQLite | Restore the latest snapshot. Re-run `engram migrate` + `engram init`. Consolidation backlog from between snapshot and crash is rebuilt by the reconciliation worker. |
| Filesystem | Restore from snapshot, then `engram rebuild-kg`. Committed memories from between snapshot and crash are lost — this is why the filesystem is authoritative and the snapshot interval drives the RPO. |

## Observability

### Logs

Gunicorn access logs are printed to stderr in the format:

```
<ip> <time> "GET /api/v1/query HTTP/1.1" 200 <bytes> <latency_s> "<x-request-id>"
```

Application logs are JSON per-line (set `ENGRAM_LOG_FORMAT=plain` to switch
to human-readable). Every log record carries a `request_id` field; the
same ID is propagated in the `X-Request-ID` response header so you can
correlate client-side issues with server logs.

### Metrics

`/metrics` is scraped by Prometheus at `deploy/prometheus/prometheus.yml`.
Alert rules are at `deploy/prometheus/alerts.yml`. Core alerts:

| Alert | When it fires | What to do |
|---|---|---|
| `EngramInstanceDown` | `/metrics` unreachable for 2m | Check `docker compose ps`, restart the engram container |
| `EngramIngestFailureSpike` | > 5% FAILED events for 5m | Inspect recent `events.error_message`; usually a Core Model outage |
| `EngramConsolidationQueueBacklog` | queue > 5k for 10m | Increase `max_concurrent_tasks` or investigate slow overview generation |
| `EngramShallowBiasMiss` | Core Model under-predicts depth > 15% for 30m | Collect traces, retrain (see docs/TRAINING.md) |
| `EngramFrontierReentryRate` | p90 > 1 re-entry for 30m | MSC coverage is bad — check decay / dormant_floor and data quality |
| `EngramQueryLatencyP95` | query p95 > 8s for 10m | Cascade is going deep or LLM is slow. Check `/metrics` `engram_query_latency_seconds{phase}` |

### Dashboard

Grafana is provisioned with `deploy/grafana/dashboards/engram.json`. Access
at `http://<host>:3000` (default admin/admin — change immediately).

## Playbook: common incidents

### Ingest failure spike

1. Check `engram_ingest_events_total{final_status="FAILED"}` — is it a burst
   or a steady elevated rate?
2. Check Ollama Cloud status. If it's an outage: the model provider circuit
   breaker should be open (visible on `/readyz`). Wait it out.
3. If Ollama Cloud is up: tail the JSON logs for `event=CoreModelError` — is
   there a schema change in the Core Model response? If so, look for a
   recent prompt template change and revert.
4. Use `POST /api/v1/events/{event_id}/retry` to replay individual failed
   events. The durable worker picks them up on the next poll.

### Query latency regression

1. Look at `engram_query_latency_seconds{phase}` breakdown — which phase
   regressed?
   - `frontier_answer_*` — frontier is slow. Check Ollama Cloud status.
   - `l1_plan` / `l2_plan` — Core Model is slow.
   - `l1_execute` / `vector_search` — Neo4j is slow. Check
     `docker compose logs neo4j` and vector index health.
   - `l4_read` — filesystem I/O. Check disk space / IOPS.
2. `engram_query_depth_predicted_vs_reached` — is the cascade reaching
   deeper levels more often? Might be a coverage issue.

### Neo4j flap

1. `/readyz` will show `neo4j: false` and serve 503. Nginx stops routing to
   the container (health probe). `/livez` still returns 200 so Kubernetes
   (if used) doesn't kill the pod.
2. Restart Neo4j. On recovery, the breakers auto-close.
3. If corruption is suspected: validate with `engram rebuild-kg --tenant
   TENANT_ID --dry-run`, then run the same command without `--dry-run`.

### Consolidation queue saturated

1. `/api/v1/ingest` starts returning 503 (backpressure). Clients should
   honour `Retry-After`.
2. Check `engram_consolidation_task_duration_seconds{task_type}` — which
   task is slow? Usually `CONSOLIDATE_OVERVIEW` (Core Model call).
3. Short-term: increase `consolidation.max_concurrent_tasks` in
   `config.yaml` and restart.
4. Long-term: the overview generation prompt may be too broad. Profile the
   Core Model calls.

## Security posture

- Bearer-token auth on every endpoint except `/livez`, `/readyz`, and
  `/metrics` (restricted to internal CIDRs in nginx).
- TLS 1.2+ enforced at the edge.
- Body-size middleware rejects oversized payloads before the API sees
  them.
- Field-level length caps in Pydantic schemas.
- Cypher template whitelist is the single source of Cypher; the Core
  Model never emits raw Cypher.
- Neo4j admin-only auth in Community Edition; Enterprise RBAC is a drop-in
  swap via `knowledge_graph.{writer,reader}_username` config.
- Secrets live in `.env.prod` / environment variables only; the config
  loader refuses to boot with placeholder secrets.
- `systemd` unit ships with `NoNewPrivileges`, `ProtectSystem=strict`,
  `ReadWritePaths=/var/lib/engram` — the process can't write anywhere
  unexpected.

## Capacity hints

| Resource | Rough ceiling on a 4-vCPU / 16 GB host |
|---|---|
| Query QPS | ~20 (LLM-bound) |
| Ingest QPS | ~200 (SQLite-bound; async LLM consumption separate) |
| KG nodes | ~1M with Neo4j page cache of 512 MB |
| Filesystem | 10k memories ≈ 40 MB |
| SQLite WAL size | ~200 MB sustained with heavy ingest; checkpoint tunable |

For each ~10× increase in traffic, plan one of:

1. Split background workers out of the API process.
2. Move Neo4j to a dedicated host (or Neo4j AuraDB).
3. Move Redis to a dedicated host (or ElastiCache).
4. Add nginx workers / replicas.

## Upgrade procedure

```bash
# 1. Snapshot
./scripts/backup.sh

# 2. Pull the new code and build
git pull
docker compose -f docker-compose.yml -f docker-compose.prod.yml build

# 3. Apply migrations ON THE OLD SCHEMA BOOT (runs pending scripts + clears
#    maintenance flag on success)
docker compose -f docker-compose.yml -f docker-compose.prod.yml run --rm engram engram migrate

# 4. Hot-swap
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# 5. Verify
curl -fsS https://engram.example.com/readyz | jq
```

Rollback: restore the pre-upgrade snapshot (filesystem + SQLite) and
redeploy the previous container image. Neo4j re-derives from the restored
filesystem via `engram rebuild-kg`.
