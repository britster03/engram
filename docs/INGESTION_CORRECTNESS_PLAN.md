# Ingestion correctness implementation plan

The seven review findings are acceptance criteria for this work. No software
change can guarantee that a bug will never recur; regression tests and explicit
failure behavior make these particular failures detectable and recoverable.

1. Review request and upload idempotency: changed input must conflict, unrelated
   requests must remain independent, and concurrent retries must be atomic.
2. Review claim support verification: original quotations, attribution, meaning,
   negation and uncertainty must be checked before canonical claims are written.
   Unsupported or unrepresentable claims must remain out of active memory.
3. Reject oversized document members explicitly, including the filename, actual
   character count and accepted limit. Never accept a shortened source silently.
4. Route failed-event retries using their original workflow and aggregate;
   code archives must retain their job identity and dedicated task queue.
5. Persist terminal bookkeeping across database outages and reconcile started
   dispatches against their exact Temporal executions. Fence repairs against
   newer retries and preserve already committed canonical state.
6. Stop using names alone as global entity identity. Scope unqualified mentions
   conservatively and support explicit identities and distinguishing attributes.
7. Bind gate, extraction and typing artifacts to a persisted generation policy.
   Provide an explicit, idempotent reprocessing operation with immutable source
   and audit history and an atomic transition of canonical results.

For each step: add failure-case regression tests, run the relevant existing
tests, document API behavior and operational rollout, and record validation.
Final validation includes a real PostgreSQL database, migration checks and the
affected Temporal/API paths. Live provider evaluations remain separate from
scripted correctness tests and require configured provider credentials.

## Completed implementation

All seven findings now have code changes and regression coverage. Request/upload
idempotency and claim support verification were rechecked alongside the remaining
fixes. The document fix explicitly rejects oversized members; it does not add
document chunking. Entity identity now requires caller authorization for reuse
across sources, with bindings available in both Python and TypeScript clients.
Source reprocessing has a tenant-authenticated API, immutable policy artifacts,
concurrency control, audit history and atomic promotion/retirement. Failed
reprocessing preserves the previously active results.

## Validation recorded on 2026-10-23

- Combined unit/integration run: **655 passed, 6 skipped, 2 deselected**. This
  includes all 451 unit tests, real PostgreSQL transactions, API authentication
  and tenant isolation, concurrent requests, fresh migrations, upgrades from
  0005 and 0008, and real Temporal delivery/recovery.
- Real Temporal recovery survived four injected PostgreSQL connection failures
  across a worker restart. A separately terminated execution was reconciled
  without a bookkeeping activity. These are controlled fault-injection tests,
  not a production outage exercise.
- TypeScript type checking and both client retry/serialization tests passed.
  The Python client retry/identity-binding check also passed after its update.
- Ruff checks and `git diff --check` passed for the changed implementation.
- `test_full_flow_ingest_then_query` and `test_vector_search_is_tenant_scoped`
  fail identically in a clean checkout of baseline commit **7a416a0**. Both
  create legacy filesystem/Neo4j-only retrieval contexts without a canonical
  repository. They were left unchanged and excluded from the final combined run;
  the earlier unrestricted integration run reported both failures.
- Live provider claim-verification evaluation was skipped because no provider
  credential was configured. Scripted reviews test the application contract,
  not the deployed model's semantic accuracy.

## Rollout requirements

Follow `CONSOLIDATION_FLOW.md` and `TEMPORAL_PRODUCTION.md`: coordinate a stop,
apply migration **0009_ingestion_policy**, and restart API, dispatcher and all
workers together. Production services were not modified by these checks.
Evaluate the deployed reviewer on representative sources before rollout. Use
explicit reprocessing for historical extraction results; historical identity
merges require a source-by-source audit rather than automatic splitting.
