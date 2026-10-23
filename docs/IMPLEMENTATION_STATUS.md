# Retrieval, consolidation and durable chat implementation

Updated 2026-09-29. This record describes the implementation, not a production-readiness certification. The accepted release boundary is **tenant/API-key access**. Private-user login and shared-project grants are deferred. Existing uncommitted work was preserved; no deployment or development/production database migration was performed.

## Implemented behavior

### Retrieval

The order remains **L0 → current session → sufficiency → past sessions → PostgreSQL facts → BM25/vector → graph**. Semantic search retains the public `L3_VECTOR` label for API compatibility.

- Native Neo4j/Lucene BM25 queries use the versioned `canonical_bm25_v1` full-text index. Mandatory hashed tenant and active-state terms are inside the lexical query before top-K. Query tokenization prevents callers from injecting Lucene operators or field selectors. PostgreSQL session `ts_rank_cd` remains a separate history search mechanism; it is not called BM25.
- Canonical projection text includes names, aliases, active typed claims and bounded current-version text. Corrections refresh the subject projection. Deleted nodes lose indexed text and embeddings; revision fences reject older projection updates. BM25 does not depend on generated overviews.
- BM25 and vector discovery run inside one bounded stage, with eight total discovery slots, a shared channel deadline and independent channel outcomes. IDs are deduplicated and hydrated/verified in PostgreSQL before RRF. One eligible identity gets at most one vote per channel; ties are deterministic. Native Neo4j queries receive transaction timeouts.
- Embedding failure during projection leaves updated lexical text available and the vector dispatch retryable. Discovery metadata reports channel status, timings and fusion method. PostgreSQL canonical neighbor fallback remains available when discovery supplies no eligible candidates.
- Explicit composite predicate questions require all detected predicates before being considered answerable. Registered multi-value predicates do not become conflicts merely because they have multiple values. A confidence filter leaving no evidence returns insufficient evidence.
- Verified graph paths carry bundle membership. Paths involving several resolved endpoints must include those endpoints. Context packing retains whole claims and connected path bundles, including their IDs and qualifiers. If packing removes evidence, final answerability is reassessed and qualified.
- `engram backfill-search --tenant TENANT --limit 100 [--cursor CURSOR]` queues one resumable, idempotent page through the existing projection outbox. It does not wipe Neo4j. The existing offline `rebuild-kg` path also reconstructs lexical fields and deletion tombstones.

### Consolidation

- A dedicated strict canonical prompt accepts typed claims; the legacy URI prompt remains separate. Output declares claim citations, which must refer to supplied claim IDs. Exact supporting statements still require model-quality evaluation; citation membership alone does not prove entailment.
- PostgreSQL triggers invalidate source nodes and all ancestors after claim, version, evidence, alias, hierarchy and relevant node changes. The same transaction coalesces one active overview task per tenant/scope and creates its Temporal outbox dispatch.
- Debouncing uses a **fixed window from the first dirty write**, default 30 seconds. Further writes increment `requested_generation` without moving the deadline. This deliberately avoids indefinite postponement and racing an already claimed dispatch. It differs from the earlier proposed trailing-window design.
- The worker captures `processing_generation`. Completion locks the task and, if new work arrived during processing, completes the old task and creates a successor task plus dispatch atomically. Retries of completion cannot duplicate that successor.
- Overview input walks canonical descendants, deduplicated by stable ID, with a default maximum of 100 source memories and 12,000 input tokens. Oversized scopes fail with a typed non-retryable budget error and require partitioning; they are not silently summarized from incomplete input.
- Model calls hold no database locks. Publication locks the scope and compares its revision/status atomically. Stale output is rejected. Successful artifacts are immutable for a scope revision and retain dependency IDs/revisions/version IDs, input fingerprint and cited claim IDs. Only current `canonical-overview-v3` artifacts qualify as current overviews; legacy artifacts remain historical.
- Late processing of an older accepted event cannot replace a newer single-value claim or the entity's newer current version. Older claims and versions remain in history.

### Turn lifecycle and tenant access

- Chat acceptance commits the user turn, source event, response-run record and, when enabled, its ingestion dispatch before any answer-model work. PostgreSQL session locks allocate turn order and allow one active response per session.
- Optional `client_message_id` is unique within the tenant. Identical completed requests replay the saved response. Changed input or an already running/failed/cancelled use of the same key returns a conflict with run information. Streaming versus buffered transport does not change request identity. Python and TypeScript clients accept the key.
- Responses are stored separately as derived conversation history, without an ingestion dispatch. Provider failures preserve the accepted user turn. Response persistence completes before answer delivery. SSE reports an `accepted` event with session, run and input-event IDs.
- `GET /api/v1/chat/runs/{run_id}` returns tenant-scoped response and memory-processing status. `POST /api/v1/chat/runs/{run_id}/cancel` fences late answer publication while leaving accepted memory processing intact. Abandoned runs expire after five minutes when read or when the next message is accepted. Upstream model calls are not forcibly interrupted by this cancellation fence.
- Automatic memory is behind `canonical_memory.automatic_user_memory_enabled` (default **false**) and also requires canonical mode. Its gate/extractor receive only the accepted user message. Exact supporting source quotes are checked; model-selected IDs/policy overrides are discarded; low-confidence claims are excluded. Negative assertions are conservatively deferred from positive typed claims. Ambiguous confirmations and references to prior assistant content are not automatically resolved in v1.
- Session close/compaction persistence merges newly accepted history under the session lock, rather than overwriting it with an older snapshot. The admin chat preserves SSE parser state across network chunks, blocks duplicate sends, reports saved messages and supports stopping a response. A session ID is labelled “Session active,” not “Memory ready.”
- Registry authentication fails closed on outage and honors revocation; restarting with a previously revoked legacy config key no longer restores it. PostgreSQL operations now use the existing bounded pool (12 connections, five-second checkout timeout), including nested transactions and scoped canonical reads.

## Configuration and deployment order

New settings, with implementation defaults:

```yaml
retrieval:
  hybrid_enabled: true
  hybrid_candidates_per_channel: 20
  hybrid_timeout_seconds: 3.0
  rrf_k: 60
consolidation:
  overview_debounce_seconds: 30
  overview_max_children: 100 # total canonical descendants, not just direct children
  overview_input_tokens: 12000
canonical_memory:
  automatic_user_memory_enabled: false
```

1. Apply Alembic `0006_overview_lineage` and `0007_durable_chat` before starting the updated API/workers. Both are forward-only. These add invalidation/dependency/generation contracts and response runs. Test migration lock time and least-privilege roles on the deployment's representative dataset first.
2. Create indexes using the existing initialization command. Check `SHOW FULLTEXT INDEXES` reports `canonical_bm25_v1` as `ONLINE` before enabling hybrid traffic. The index uses `standard-no-stop-words`; BM25 scoring is supplied by the tested Neo4j 5.24/Lucene stack, not PostgreSQL ranking functions.
3. Run the bounded `backfill-search` command per tenant, following `next_cursor` until null. Projection workers perform the work. Confirm dispatch completion and canonical/projection revision alignment. Keep traffic on the previous retrieval configuration during index preparation.
4. Canary `retrieval.hybrid_enabled`; set it to false to restore vector-only discovery. Keep source/deletion verification enabled. Do not revert migrations to roll back ranking.
5. Enable automatic user memory only after validating the source-policy gate/extractor with the intended live model and tenant corpus. This switch is independent of hybrid retrieval. Explicit ingest retains its existing compatibility behavior.

## Verification

Tests use disposable PostgreSQL 16 schemas and Neo4j 5.24 nodes, not the configured development database. Added coverage includes native scoped BM25 ranking/corrections/deletion, RRF deduplication, channel timeout/failure, complete path packing, compound predicates, source spans, hierarchy invalidation, stale publication, immutable retries, processing-generation handoff, out-of-order ingestion, idempotent chat, cancellation, concurrent acceptance, tenant access, pool bounds and migration upgrades.

Final combined verification: **298 Python tests passed** (all unit tests plus the selected retrieval, consolidation, chat, migration and real-database integration suites), in 68.72 seconds. **Two JavaScript transport tests passed**, exercising the actual admin chat parser with one-byte chunks, multiline data, split Unicode and LF/CRLF endings. Fresh-database and `0005`-to-head upgrades passed. TypeScript `tsc --noEmit` passed using TypeScript 5.3.3 in an external npm cache. Ruff and Python compilation cover the changed Python paths.

The Python run reports existing Starlette/AnyIO and Alembic configuration deprecation warnings. Whole-worktree whitespace checking also reports pre-existing whitespace in `engram/models/embeddings.py`, which this implementation did not edit. Live Temporal server execution/replay, live-model answer quality, browser automation, representative load, production-role validation, restore and failover were not performed in this work.

## Open work and release gates

The complete production plan is **not finished**. These remain substantive requirements:

- **RC-A/B:** arbitrary question-part/entity coverage, calibrated conversational sufficiency, independent valid-time versus known-at historical reconstruction, and structured ambiguity resolution. Current composite-predicate coverage is intentionally narrower. Existing historical behavior must not be advertised as full bitemporal support.
- **M1/M2:** true provider token streaming; deliberate regeneration of a failed/cancelled accepted turn without a new message; fully normalized/paginated message history; bounded snapshot retention; robust explicit-confirmation resolution and signed negative claims. Current answer deltas are emitted after generation/persistence, and conversational snapshots still grow.
- **M4:** 90-day raw-message retention, source-preserving evidence expiry, complete forgetting/erasure across derived summaries, graph/vector state, source events/artifacts, caches and backups, plus deletion-ledger restore replay. Tombstones and stale-summary exclusion do not constitute completed erasure.
- **M3/M5:** live-model source-support/contradiction evaluation, BM25-only/vector-only/hybrid held-out comparison, global-ANN small-tenant recall, native index restart/rebuild drills, source fan-out and bulk consolidation benchmarks, request-wide database/provider deadlines, production migration locks/roles, operational metrics and alerts.
- **M5/M6:** live Temporal retries/replay and workflow-version rollout, full browser/accessibility journeys, representative latency/capacity measurements, HA/PITR/restore/failover evidence, and release sign-off. No accuracy gain, SLO, throughput, RPO or RTO has been established by these unit/integration tests.

Private-user and shared-project authorization is **deferred by the user**, rather than counted as a missing feature for the current tenant-scoped release.
