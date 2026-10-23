# Scaling

How Engram scales from a single-node personal deployment to a
multi-tenant, multi-replica Kubernetes service. Read
[PRODUCTION.md](PRODUCTION.md) first if you're operating a single
instance — this document picks up where that leaves off.

## Scale axes

| Axis | Small | Medium | Large |
|---|---|---|---|
| Tenants | 1 (`_default`) | 10–100 | 100+ |
| API replicas | 1 | 3–5 | 10+ with HPA |
| Neo4j | Community single-node | Neo4j AuraDB or self-managed Enterprise | Causal Cluster / Aura Enterprise |
| Redis | Single-node | Single-node with AOF | Redis Sentinel or ElastiCache |
| Storage | Local volume | Shared RWX (EFS / Filestore / NFS) | Same + tiered cold backup |
| Workers | Local legacy pollers | Temporal worker replicas | Autoscaled/versioned Temporal workers |

## Multi-tenancy

Every production row in PostgreSQL, every node in Neo4j, and every directory in the
filesystem carries a `tenant_id`. The `_default` tenant is created on
boot for backward-compat with single-tenant deployments.

### Tenant lifecycle

```
         create                 suspend                 delete
                                                  (hard delete not exposed)
  ────────────►  ACTIVE  ────────────►  SUSPENDED  ─────✗
                   ▲  │                     │
                   │  │   resume            │
                   └──┴─────────────────────┘
```

### Create a tenant

```bash
curl -fsS -X POST https://engram.example.com/api/v1/admin/tenants \
  -H "Authorization: Bearer $ENGRAM_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "acme-corp",
    "display_name": "Acme Corporation",
    "quotas": {
      "requests_per_minute": 300,
      "ingest_per_minute": 1200,
      "max_memories": 500000,
      "max_monthly_tokens": 10000000
    }
  }'
```

Response (once; you cannot retrieve the key again):

```json
{
  "tenant_id": "acme-corp",
  "api_key": "engram_uQV…",
  "api_key_count": 1,
  "status": "ACTIVE",
  "quotas": { … }
}
```

Mint additional keys for the same tenant with
`POST /api/v1/admin/tenants/{id}/keys`. Revoke by hash:
`DELETE /api/v1/admin/tenants/{id}/keys/{hash}`.

### Tenant isolation

Each layer enforces its own boundary:

* **Filesystem**: `{data_dir}/{tenant_id}/...`. File handles cannot
  traverse out of a tenant's root because the path is computed from
  the ambient tenant context at every call.
* **Neo4j**: every node has `tenant_id`; every Cypher template (both
  ours and ones you add) filters by `$tenant_id` in the WHERE clause.
  The per-tenant uniqueness constraint `(tenant_id, source_uri)` keeps
  URIs independent across tenants.
* **PostgreSQL**: every table has `tenant_id`; every query
  filters by it. The consolidation queue dedup index is keyed on
  `(tenant_id, node_id, task_type)`.
* **Session cache**: keys are `session:{tenant_id}:{session_id}` so two
  tenants can reuse the same session ID without sharing state.
* **Audit log**: every row tagged with `tenant_id` and indexed on it.
  Admin tailing is scoped by default.

### Per-tenant quotas

Rate limiting (`engram/api/rate_limit.py`) resolves the bearer token
to a tenant and uses that tenant's `quotas.{requests,ingest}_per_minute`
as the bucket capacity. Unknown tokens fall back to the fleet default.
Setting `capacity <= 0` means "unlimited".

Backpressure (`consolidation.max_backlog`) is evaluated per-tenant — a
noisy tenant hitting their backlog cannot starve others.

## Horizontal scaling

### Run multiple API replicas

Three properties make this safe:

1. **Transactional dispatch**: the API creates the PostgreSQL aggregate and
   Temporal dispatch row in one transaction before returning 202.
2. **Temporal workers**: ingest, consolidation, reconciliation, and decay are
   executed outside API pods with durable retries and idempotent identifiers.
3. **Redis-backed rate limiter**: token buckets are atomic across
   replicas (Lua script, compare-and-set on the Redis key). Fleet
   limits hold even when multiple workers serve the same tenant.

```
              ┌──────────────────────────┐
              │         ingress           │
              └──────────┬───────────────┘
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
  │  api pod 1  │ │  api pod 2  │ │  api pod 3  │
  │             │ │             │ │             │
  └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
         │               │               │
         └───────────────┴───────────────┘
                         │
               Neo4j · Redis · PostgreSQL
                         │
                  Temporal workers
```

SQLite-on-RWX is explicitly unsupported. A network filesystem does not turn
SQLite into a safe distributed control plane.

### Shared filesystem

The filesystem under `data_dir` is authoritative and must be readable +
writable by every replica. Use:

* **AWS EFS** (`efs-csi` storage class, ReadWriteMany)
* **GCP Filestore** (`standard-rwx`)
* **Azure Files** (`azurefile-csi`)
* Self-managed **NFS** with the `nfs-csi` driver

Writes are atomic via temp+rename+fsync, so network FS with strong
close-to-open consistency is sufficient. Avoid object-backed FUSE
mounts (S3FS / GCSFuse) — they don't honour rename semantics.

### Neo4j at scale

* Community single-node: fine up to ~1M nodes and ~20 QPS.
* **Neo4j AuraDB** (recommended production): managed, zero-ops,
  supports writer/reader role separation (which §12.3 of the SDD
  describes as a defense-in-depth requirement).
* Self-managed **Causal Cluster** (Enterprise): when you need to stay
  on-prem or have sustained >200 QPS.

Whichever you pick, point `knowledge_graph.uri` at the new endpoint
and set `writer_username` / `reader_username` to different principals.
`engram rebuild-kg` stays the recovery primitive.

### Redis at scale

Single-node Redis works up to ~5 k ops/s. Beyond that:

* **Redis Sentinel** for HA within one region.
* **Redis Cluster** / **AWS ElastiCache Cluster** for sharded throughput.
* For multi-region failover, **Redis Enterprise** or **Upstash Redis
  Global**.

The session cache, rate limiter, and lease backends all speak the
standard Redis wire protocol — no code changes needed for any of the
above.

## Kubernetes deployment

The repository contains two Kubernetes representations, but only the Helm
chart supplies production rollout ordering:

### Path A — raw manifests (`deploy/k8s/`)

The raw `deploy/k8s` bundle is a staging/reference configuration. It has no
ordered migration hook and is not the production release mechanism. Use it
only to inspect resources or bootstrap a non-production environment.

### Path B — Helm chart (`deploy/helm/engram/`)

```bash
helm install engram deploy/helm/engram \
  --namespace engram --create-namespace \
  --set image.repository=ghcr.io/your-org/engram \
  --set image.tag=${ENGRAM_RELEASE_TAG} \
  --set secrets.existingSecret=engram-secrets \
  --set temporal.address=temporal-frontend.temporal.svc.cluster.local:7233 \
  --set neo4j.uri=bolt://neo4j.engram.svc.cluster.local:7687 \
  --set redis.url=redis://redis.engram.svc.cluster.local:6379 \
  --set ingress.host=engram.example.com
```

Create `engram-secrets` beforehand through External Secrets, Sealed Secrets,
or Vault. It must contain `ENGRAM_API_KEY`, `ENGRAM_ADMIN_KEY`,
`ENGRAM_SECRET_KEY`, `OPENCODE_GO_API_KEY`, `NEO4J_ADMIN_PASSWORD`, and
`ENGRAM_DATABASE_URL`. The pre-install/pre-upgrade migration hook intentionally
requires an external secret so credentials exist before Helm starts migrations.

### HPA inputs

Default metrics:

| Metric | Target |
|---|---|
| CPU utilization | 65% |
| Memory utilization | 75% |
| `engram_query_latency_seconds_p95` (custom) | 2 s |

The custom latency metric requires the Prometheus Adapter. Install
`kube-prometheus-stack` + `prometheus-adapter` to get it.

### Pod Disruption Budget

`minAvailable: 2` ensures voluntary disruptions (node drains, rolling
upgrades) never take the service below 2 pods. Combined with
`terminationGracePeriodSeconds: 60` and a 15-second preStop sleep,
in-flight requests complete before the container exits.

## Observability at scale

### Tracing

Set `OTEL_EXPORTER_OTLP_ENDPOINT` and the app auto-instruments FastAPI,
HTTPX outbound model calls, and Redis. Custom spans wrap every
pipeline stage via `engram.tracing.span(...)`. Traces carry
`tenant_id` as a span attribute so Tempo / Honeycomb / Jaeger views
slice cleanly per tenant.

### Metrics

Already-exposed series (`/metrics`):

```
engram_query_latency_seconds{phase}
engram_ingest_pipeline_stage_seconds{stage}
engram_query_depth_predicted_vs_reached{predicted,reached}
engram_reentries_per_query
engram_l0_gate_decisions{decision,reason}
engram_ingest_events_total{final_status}
engram_core_model_calls_total{task,provider}
engram_frontier_tokens_total{direction}
engram_consolidation_queue_depth
engram_kg_node_count
engram_kg_edge_count
```

### Logs

Structured JSON via `engram.logging_setup`. Each log record carries
`request_id` (propagated end-to-end) and will include `tenant_id` once
the middleware stack is extended — straightforward addition.

Ship with Vector / Fluent Bit / Datadog agent — standard shippers all
work because the output is plain JSON on stderr.

## Client SDKs

* **Python**: `clients/python/engram_client/` — `pip install engram-client`
* **TypeScript**: `clients/typescript/src/index.ts` — `npm install engram-client`

Both support: ingest, query (buffered + streaming), sessions,
memories, and the admin surface for tenant / key management. Both
retry 429 / 5xx with exponential backoff and honour `Retry-After`.

## Load testing

```bash
pip install locust
locust -f scripts/loadtest.py \
  --host https://engram.example.com \
  --headless --users 200 --spawn-rate 10 --run-time 15m \
  --api-key $ENGRAM_API_KEY
```

Ships with a four-stage shape (warm-up → steady → spike → steady →
cool-down) and an automatic stop condition on sustained >2% error
rate.

## Capacity hints

These are measured on a baseline 4-vCPU / 16 GB / SSD host with one
Engram replica and co-located Neo4j + Redis.

| Resource | Ceiling |
|---|---|
| Query QPS | 20 (bottleneck: LLM latency) |
| Ingest QPS | Environment-specific; PostgreSQL and model-provider bound |
| KG nodes @ 512 MB page cache | ~1M |
| Filesystem: 10k memories | ~40 MB |
| PostgreSQL WAL | Size from measured ingest rate and PITR retention |
| Memory / pod | 1–4 GB depending on local embedding use |

Triple the query QPS by:

1. Adding API replicas (linear up to Neo4j's ceiling).
2. Tuning `retrieval.max_depth` lower for short queries.
3. Enabling the L0 classifier so trivial queries BYPASS the cascade.

Triple the ingest QPS by:

1. Increasing Temporal worker replicas and activity concurrency.
2. Scaling PostgreSQL IOPS/connections and the model-provider quota.
3. Separating worker task queues into independently scaled deployments.

## What's explicitly out of scope

Even with everything above, Engram is not yet:

- **Region-active/active**. Cross-region replication for the
  filesystem + Neo4j is a deployment-specific integration (S3 + cross-
  region replication, AuraDB multi-region). Not boxed up here.
- **Fully SaaS self-serve**. The admin API is present but billing /
  usage metering / customer dashboards are a product surface you'd
  build on top.
- **Encrypted at rest beyond the platform default**. Whatever cloud
  volumes + Neo4j storage encryption the platform provides is what
  you get. Column-level encryption for the audit log or extractions
  tables is a future addition.
- **GDPR DSAR tooling**. Tenant-scoped deletion is supported; user-
  level deletion within a tenant is not wired through the whole
  KG yet (the HISTORICAL status primitives are there; wiring a DSAR
  pipeline is the remaining work).

Each of these is planned-for but needs product + legal context before
implementation.
