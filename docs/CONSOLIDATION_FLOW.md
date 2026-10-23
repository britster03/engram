# Canonical consolidation in development and production

All standard profiles (`config.yaml`, `config.opencode-go.yaml`,
`config.dev.postgres.yaml`, staging, production, Kubernetes and Helm) use
PostgreSQL-canonical memory and Temporal. The filesystem compatibility code
remains for isolated tests and historical migration tooling. It is not selected
by these profiles. Markdown files are not runtime memory authority.

## Processes and local startup

Populate `.env` using `.env.example`, then run:

```bash
./scripts/quickstart.sh
```

The script starts an isolated `engram-dev` Compose project containing PostgreSQL,
Neo4j, Redis, PostgreSQL-backed Temporal and its UI. It initializes the Temporal
schemas and `engram-dev` namespace, applies Engram migrations, initializes Neo4j
indexes, and starts the API, all Temporal worker queues, and dispatcher.
Production uses `scripts/deploy.sh` with `config.prod.yaml` and namespace
`engram-prod`. It does not publish PostgreSQL or Temporal gRPC on the host.

Development selects a local database on port 5433 using `ENGRAM_DB_PASSWORD`.
Set `ENGRAM_DEV_DATABASE_URL` deliberately to select another development
database. The script does not overwrite an existing `.env` database URL or stop
production processes. PID files and logs are under `data/dev-runtime`.
API-only startup cannot process queued ingestion, projection or consolidation.

## Source mutation to overview publication

1. An explicit ingest/session message commits its event and dispatch in
   PostgreSQL. Gate and extraction activities persist bounded source artifacts.
2. Canonical commit writes source versions, typed claims, evidence, hierarchy
   changes and graph-projection work transactionally. PostgreSQL triggers
   invalidate affected source/ancestor revisions in this transaction.
3. Active episodes, collections and projects request overview tasks. Episodes
   provide an automatic scope for conversational ingestion; collection/project
   grouping is optional and still supports broader overviews. Scope IDs are
   tenant-scoped stable memory UUIDs, not a shared filesystem directory.
4. The first dirty write schedules work after the configured fixed debounce
   window (30 seconds). Further writes increment `requested_generation` without
   moving its deadline. One active `(tenant_id, node_id, task_type)` task exists.
5. The dispatcher leases due outbox rows and starts a Temporal workflow using
   the task ID. The activity binds the task's tenant and captures
   `processing_generation`. Conversation payloads stay outside workflow history.
6. The handler collects active canonical descendants with bounded database
   fanout, records source revisions/version IDs, and checks the input budget.
   Deleted/historical child nodes are excluded from current input.
7. With `consolidation.quality_mode: enforce`, the model selects supplied claim
   IDs. The server renders exact canonical values and qualifiers. Generated
   prose cannot add an unsupported statement to the published overview.
8. Publication locks the scope and verifies its revision/status and all source
   dependencies. Changed input rejects publication. The immutable artifact is
   keyed by tenant, scope, input revision and generator policy.
9. The policy fingerprints the prompt source, model identity/settings, quality
   mode and budgets. A policy change can generate another artifact for unchanged
   sources; retries of the same policy/source revision preserve its artifact.
10. Completion detects changes arriving during a run and atomically creates a
    successor task and dispatch when required. Transient errors use Temporal
    retries; exhausted/permanent failures remain visible as failed tasks.

Overview queries use current, policy-matching artifacts to discover claim IDs.
Every selected claim is reloaded and verified in PostgreSQL before it can
support an answer. Summary prose is not promoted to canonical evidence.
A missing/stale artifact requests regeneration and original canonical facts
remain available to retrieval.

## Claim support verification

With `canonical_memory.evidence_mode: enforce`, conversational commit now reviews
the final normalized claims against their original messages before opening its
SQL write transaction. The Core model makes a separate structured review request;
only `SUPPORTED`, `DIRECT`, `POSITIVE` statements that the writer can represent
become active claims. Contradicted candidates are rejected; ambiguous, inferred,
reported, hypothetical and signed negative relationships are deferred. Original
event payloads are retained, and canonical episode/entity text uses the original
messages rather than unchecked generated paraphrases. Abstracts are rendered from
the accepted claim values.

Reviews are immutable artifacts in the existing `ingest_artifacts` table. Their
keys bind tenant, event, full source payload, final extraction and verification
policy. The policy fingerprints the review prompt, model settings, predicate
policy and verification budgets. The canonical transaction rechecks that binding
and filters claims itself; an extraction cache hit cannot bypass this guard.
Retries after a failed SQL commit reuse the matching persisted review. Model
outages or malformed/incomplete decisions fail the activity for normal Temporal
retry; budget violations are explicit non-retryable failures. No source text is
silently shortened to fit verification.

Defaults under `canonical_memory` are 200 final candidates, batches of 8, 24,000
input tokens and 2,048 output tokens per review request. Configure these using
`claim_verification_max_claims`, `claim_verification_batch_size`,
`claim_verification_input_tokens` and `claim_verification_output_tokens`.
Verification can add several model requests to an ingestion. Model-based support
classification is fallible and requires representative live-model evaluation.
Legacy evidence mode retains its existing behavior.

An opt-in smoke check is available after loading the intended model credentials:

```bash
ENGRAM_TEST_CLAIM_VERIFIER_LIVE=1 .venv/bin/pytest -q tests/e2e/test_claim_verification_live.py
```

This exercises a small set of semantic examples with the real provider; it does
not replace evaluation on the deployment's intended documents and languages.

Inspect `GET /api/v1/events/{event_id}/verification` for per-candidate decisions,
reasons and accepted/rejected/deferred counts. `committed: false` means the report
was persisted but its canonical transaction has not committed. Event `COMPLETE`
can legitimately contain zero accepted claims; it also preserves the original
source. A report does not establish that a source assertion is true in the world.

## Migration and rollout

Coordinate a stop, migration and restart of API, dispatcher and all workers.
Strict ingestion now pins a processing plan before the gate: prompt sources,
model settings, implementation/predicate policy, evidence policy and an explicit
`extraction_policy_version`. Gate/extraction/typing artifacts use policy-derived
keys. A retry cannot mix policies or adopt unversioned cached extractions.
Uncommitted pre-upgrade workflows that have already passed the gate can fail with
`MissingProcessingArtifact`; after terminal bookkeeping, explicitly reprocess
their source. Already committed events replay their existing canonical mutation;
use the reprocess endpoint to reassess historical results deliberately.

Apply Alembic through `0009_ingestion_policy` before starting updated workers/API:

```bash
python -m engram.cli migrate
```

The CLI uses the database selected by its loaded profile/environment file.
Migration 0008 changes overview uniqueness to include generation policy and
adds episode invalidation. Migration 0009 adds tenant-scoped source generations,
one pending/active generation per source, and active-only logical claim
uniqueness so withdrawn results can be regenerated without rewriting history.
Use a coordinated stop/migrate/restart for the API,
dispatcher and workers; `scripts/deploy.sh` coordinates this stop/restart and
reruns its migration jobs. Do not mix older writers with the new schema.
No source-memory rows or existing immutable overview content are deleted.
Policy-aware maintenance requests regeneration of old-policy artifacts.
`python -m engram.cli smoke` now waits for real Temporal ingest/projection work;
start the dispatcher and workers first.

Switching a legacy installation to canonical mode does not import Markdown
memories. Existing legacy source files and graph-only memories need an explicit,
reviewed import; do not delete them or assume control-plane migration imported
their contents. Keep development/staging/production databases and namespaces
separate.

## Independent settings and remaining limits

- `canonical_memory.automatic_user_memory_enabled` remains explicitly false in
  all profiles. Explicit ingest/session messages are supported. Ordinary chat
  history is durable, but user-message fact extraction is a separate opt-in.
  Validate the gate/extractor with the intended model before enabling it.
- Session compaction and explicit session-close summaries remain separate from
  canonical overview generation. The default compaction trigger is 64,000
  tokens. Closing a browser tab is not an explicit session commit.
- Input defaults are 100 active descendants and 12,000 prompt tokens. Larger
  scopes fail with a typed non-retryable budget error. Automatic partitioning
  is not implemented; organize oversized collections/projects into smaller
  scopes. Silent partial-input summarization is not used.
- Claim selection can omit useful facts. Coverage and omitted claim IDs are
  recorded. Live-model quality, representative load, erasure/retention, backup
  restoration, failover and HA still require deployment validation.
- Task `COMPLETE`, dispatch `COMPLETE` and source/overview freshness are distinct.
  Check task errors and canonical revisions/dependencies, not just queue depth.
