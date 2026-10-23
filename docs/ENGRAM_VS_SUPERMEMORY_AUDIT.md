# Engram vs. Supermemory: Memory Architecture and Product Audit

**Assessment date:** 10 September 2026  
**Engram revision:** `6184f0bc8210714ea8168f6c600bce4e96009c2c` plus the existing working-tree changes  
**Supermemory revision:** `958ae8b61960fe2ade495b095a01667cf44c5706`

## Executive summary

Supermemory is currently the stronger general-purpose memory product. Its documented product includes hybrid document-and-memory retrieval, static and dynamic user profiles, broad content ingestion, memory review and forgetting workflows, and a larger integration surface.

Engram has the stronger inspectable foundation for high-assurance memory. It stores canonical memory in PostgreSQL, records immutable versions and typed claims, preserves evidence and temporal validity, isolates tenants through row-level security, and treats Neo4j as a derived discovery projection rather than unquestioned truth. Retrieval candidates are hydrated from the canonical store and checked before being used to answer a question.

The practical conclusion is:

- **Best general-purpose memory product today:** Supermemory
- **Best auditable correctness and governance architecture:** Engram
- **Best opportunity for a differentiated enterprise memory system:** Engram, after its retrieval, profile, lifecycle, documentation, and developer-experience gaps are addressed

There is an important comparison limitation. The audited Supermemory repository revision contains documentation, SDKs, MCP code, user-interface applications, and playgrounds, but the current server-side memory engine implementation was not found in the repository. Supermemory's internal storage, consistency, tenant isolation, and failure behavior therefore could not be verified to the same depth as Engram's. Statements about those capabilities in this report are explicitly treated as documented product behavior or vendor-published results.

## Scope and methodology

The review covered the following areas:

- Canonical data model and source of truth
- Ingestion and memory formation
- Retrieval, ranking, and answer grounding
- Temporal history, conflicts, consolidation, and forgetting
- Personalization and user profiles
- Tenant isolation, authorization, and operational security
- Background processing, consistency, and failure recovery
- API and SDK completeness
- Deployment complexity, observability, and maintenance
- Automated tests, static analysis, documentation, and benchmarks

The Engram assessment used its application source, migrations, deployment configuration, current containerized stack, and automated test suite. Supermemory was assessed from the linked repository at the pinned revision and its first-party documentation.

## Overall comparison

| Dimension | Engram | Supermemory | Current lead |
|---|---|---|---|
| Canonical source of truth | PostgreSQL with immutable versions, claims, evidence, mutations, and event identities | Engine implementation unavailable for source-level verification | Engram |
| Temporal memory | Validity intervals, history, retirement, unmerge, and as-of retrieval | Documented evolving graph, update relationships, and forgotten state | Engram for auditability |
| Provenance and answer safety | Canonical hydration, tenant and temporal validation, evidence gate, and explicit conflict states | Documented confidence, reranking, and inference review | Engram |
| Retrieval breadth | Dense graph discovery with lexical evidence checks; no full hybrid pipeline | Documented memory and document hybrid search, filters, rewriting, and optional reranking | Supermemory |
| Personalization | Memory categories exist, but no complete first-class profile service | Static and dynamic profiles with configurable topical buckets | Supermemory |
| Ingestion formats | Conversations, text, JSONL, CSV, and code/text archives | Documents, URLs, PDF/OCR, Office files, images, audio/video, code, and connectors | Supermemory |
| Lifecycle experience | Version history, soft retirement, consolidation, and unmerge | Semantic forgetting, dry runs, review, approve/decline, and undo | Supermemory |
| Multi-tenancy | Tenant keys, tenant-scoped records, PostgreSQL RLS, and canonical tenant verification | Documented container-tag namespaces and scoped access | Engram for verifiability |
| Reliability and recovery | Event ledger, transactional dispatch, Temporal workflows, retries, and reconciliation | Documented background processing and single-binary self-hosting | Engram for observable recovery |
| Deployment simplicity | PostgreSQL, Neo4j, Redis, Temporal, workers, dispatcher, and API | Advertised one-binary, zero-configuration self-hosting | Supermemory, based on documentation |
| SDKs and integrations | Partial Python and TypeScript SDKs | Broader SDK and framework integration surface | Supermemory |
| Quality evidence | A LoCoMo runner exists, but no versioned result artifact was found | MemoryBench and vendor-published benchmark results | Supermemory |

## Engram architecture assessment

### Strengths

#### 1. A strong canonical memory model

Production configuration enables canonical PostgreSQL memory and disables filesystem-backed runtime memory. The canonical schema represents:

- Memory nodes and immutable versions
- Typed entity and scalar claims
- Evidence and source provenance
- Entity aliases and URI aliases
- Hierarchical memory relationships and overviews
- Ingestion artifacts
- Canonical mutations
- Projection dispatch records

Relevant implementation:

- [`engram/storage/memory_repository.py`](../engram/storage/memory_repository.py)
- [`engram/storage/canonical_schema.py`](../engram/storage/canonical_schema.py)
- [`engram/config.prod.yaml`](../engram/config.prod.yaml)

The memory repository commits the canonical memory state, conflict decisions, evidence, and graph-projection dispatch as part of one PostgreSQL transaction. Neo4j and the filesystem are not required to participate in that transaction. This gives Engram a recoverable source of truth even when derived projections are unavailable.

#### 2. Evidence-oriented retrieval

Neo4j is used to discover candidates, but a candidate is not automatically trusted. The retrieval layer hydrates it from PostgreSQL and rejects records that are absent, retired, stale, temporally invalid, or associated with the wrong tenant. The evidence gate can classify an answer as answerable, partial, insufficient, or conflicting.

Relevant implementation:

- [`engram/retrieval/evidence.py`](../engram/retrieval/evidence.py)
- [`engram/retrieval/orchestrator.py`](../engram/retrieval/orchestrator.py)

This is a meaningful architectural advantage for regulated or operationally sensitive use cases. It provides a clear boundary between approximate retrieval and canonical truth.

#### 3. Temporal and conflict-aware memory

Claims include asserted and validity times, and predicate policies control cardinality and temporal behavior. Engram can answer current and historical questions without overwriting prior state. It can also preserve conflicting evidence instead of silently selecting the most recent embedding match.

Relevant implementation:

- [`engram/domain/predicate_registry.py`](../engram/domain/predicate_registry.py)
- [`engram/storage/memory_repository.py`](../engram/storage/memory_repository.py)

#### 4. Reliable asynchronous processing

The event ledger, transactional dispatch records, dispatcher, Temporal workflows, projection retry behavior, and reconciliation facilities provide an inspectable path from ingestion to materialized memory. This is more operationally mature than an untracked background task or direct dual-write design.

#### 5. Verifiable tenant isolation

Canonical memory tables carry `tenant_id`, and the migrations install PostgreSQL row-level security policies. In the audited running database, the canonical tables were owned by the migration role rather than the runtime role, and the runtime role had neither superuser nor `BYPASSRLS` privileges. That means the inspected RLS boundary was active rather than cosmetic.

Relevant migration:

- [`engram/migrations/alembic/versions/0003_canonical_memory.py`](../engram/migrations/alembic/versions/0003_canonical_memory.py)

### Weaknesses and risks

#### 1. V1 and V2 architectural descriptions are mixed

The production implementation uses PostgreSQL as canonical memory, while several README and architecture sections still describe filesystem-authoritative `mem://` memory. The API also carries concepts from both representations. This makes it difficult for contributors and operators to determine which behavior is current.

Impact:

- New code can accidentally extend a legacy path.
- Operational runbooks may direct users to the wrong recovery procedure.
- Clients must understand both filesystem-era and canonical-memory fields.
- Security and consistency assumptions differ between the two systems.

Recommendation: declare the PostgreSQL design as V2 in one architecture decision record, archive V1 documentation, and place any required compatibility behavior behind an explicitly deprecated adapter.

#### 2. Retrieval prioritizes precision but lacks modern recall stages

Engram performs semantic discovery and then applies strong canonical and lexical checks. It does not currently provide a complete hybrid retrieval pipeline with document chunks, PostgreSQL full-text/BM25-style ranking, rank fusion, reranking, or query rewriting.

The lexical bridge is valuable for reducing broad vector false positives, but it can reject correct paraphrases and semantically equivalent wording. This is likely to reduce recall on conversational benchmarks.

Recommendation: preserve the canonical hydration and evidence gate, but improve the candidate set before those checks.

#### 3. Advertised adaptive re-entry is disabled

The retrieval orchestrator accepts `max_reentries`, but currently discards it. Final answer generation is called with `allow_need_more=False`, and recorded re-entry count remains zero.

Relevant implementation:

- [`engram/retrieval/orchestrator.py`](../engram/retrieval/orchestrator.py)

Recommendation: implement the bounded `NEED_MORE` loop with explicit latency and token budgets, or remove the unused parameter and update the documentation.

#### 4. Predicate coverage is narrow

The registry has good explicit policies for a small set of common relationships. Unknown predicates fall back to generic scalar, multiple-value, interval, and coexistence behavior. This protects ingestion from failure, but limits entity linking and relationship semantics for new domains.

Relevant implementation:

- [`engram/domain/predicate_registry.py`](../engram/domain/predicate_registry.py)

Recommendation: introduce a predicate schema registry per tenant/domain, validation tooling, and a controlled path for promoting frequently observed predicates from provisional to governed types.

#### 5. Profiles are not first-class

Engram can store profile- or preference-like memories, but no complete materialized user-profile API was found. An assistant therefore has to retrieve profile facts repeatedly rather than obtaining a compact, revisioned profile containing stable facts, recent dynamic observations, and topic-specific summaries.

Recommendation: materialize profiles from canonical claims, retain links to their evidence, and expose a fast `profile + relevant memories` context endpoint.

#### 6. Memory review and semantic forgetting are incomplete

History, retirement, consolidation, and unmerge exist, but there is no complete workflow for reviewing low-confidence derived memories, approving or declining them, and undoing that decision. There is also no semantic forget operation with a preview, safety limit, batch identity, and recovery path.

Recommendation: model inferred memories and their derivation evidence explicitly, then add review and forget-batch mutations to the canonical event ledger.

#### 7. Legacy graph mutation has correctness hazards

One legacy retirement path creates a Neo4j query containing `$tenant_id` but supplies only `uri`, then swallows the exception. This can leave the projection active after the source memory is retired.

Relevant implementation:

- [`engram/api/routes/memories.py`](../engram/api/routes/memories.py)

Recommendation: remove the legacy path during V2 cutover. If it must remain temporarily, fix the tenant parameter and add a regression test that compares canonical and projected states.

#### 8. Deployment identity and ownership can drift

The PostgreSQL initialization script creates app, dispatcher, migrator, and Temporal roles using the same password. It also makes the app role the database owner and grants it schema creation. The inspected running database currently has safer migrator ownership, but the checked-in bootstrap configuration does not reliably reproduce that state.

Relevant implementation:

- [`deploy/postgres/init/01-databases.sh`](../deploy/postgres/init/01-databases.sh)

Recommendation:

- Use separate secrets for app, dispatcher, migrator, and Temporal.
- Make the migrator role own the database, schema, and application tables.
- Revoke schema creation and DDL privileges from the app role.
- Use a dedicated migration URL instead of the runtime application URL.
- Add empty-install and N-1 upgrade tests that verify final ownership and grants.
- Consider `FORCE ROW LEVEL SECURITY` for canonical tenant tables, with a deliberately separate cross-tenant maintenance role where required.

#### 9. Neo4j least privilege is not fully realized

The production configuration uses the same Neo4j administrative identity for reader and writer operations. The configuration notes the limitations of Community Edition role-based access control.

Recommendation: either use an edition/deployment that provides real read/write separation or consider a simplified PostgreSQL + pgvector + full-text-search deployment profile where a separate graph database is not required.

#### 10. SDK and repository maturity gaps

The Python client has a static type error when adding `quotas` to the tenant creation body. Its documentation references asynchronous support that is not fully implemented. The TypeScript package contains placeholder repository metadata and has limited route/test coverage. The repository also lacks a committed top-level license, security policy, contribution guide, changelog, and CI workflows.

Relevant files:

- [`clients/python/engram_client/client.py`](../clients/python/engram_client/client.py)
- [`clients/typescript/package.json`](../clients/typescript/package.json)

## Supermemory architecture assessment

### Documented strengths

#### 1. Documents and memories are separate retrieval assets

Supermemory documents a model containing raw source documents, document chunks, extracted memories, and user profiles. Search can target memories, documents, or a hybrid of both. This helps the system answer both factual-profile questions and questions requiring detailed source passages.

Sources:

- [How it works](https://github.com/supermemoryai/supermemory/blob/958ae8b61960fe2ade495b095a01667cf44c5706/apps/docs/concepts/how-it-works.mdx)
- [Search](https://github.com/supermemoryai/supermemory/blob/958ae8b61960fe2ade495b095a01667cf44c5706/apps/docs/recall/search.mdx)

#### 2. Living graph relationships

Its documentation describes `UPDATES`, `EXTENDS`, and `DERIVES` relationships, latest-version tracking, expiration, and noise reduction. This is a useful product-level vocabulary for explaining how a memory evolves.

Source: [Graph memory](https://github.com/supermemoryai/supermemory/blob/958ae8b61960fe2ade495b095a01667cf44c5706/apps/docs/concepts/graph-memory.mdx)

#### 3. First-class user profiles

Static and dynamic profiles, including configurable topical buckets, give applications a compact always-available personalization layer. This is more efficient than repeatedly searching all memories for basic user context.

Source: [User profiles](https://github.com/supermemoryai/supermemory/blob/958ae8b61960fe2ade495b095a01667cf44c5706/apps/docs/concepts/user-profiles.mdx)

#### 4. Broad ingestion surface

The documented content pipeline supports text, URLs, PDF/OCR, Office documents, images, audio/video transcription, code-aware chunking, JSON, and CSV.

Source: [Content types](https://github.com/supermemoryai/supermemory/blob/958ae8b61960fe2ade495b095a01667cf44c5706/apps/docs/concepts/content-types.mdx)

#### 5. Product-friendly lifecycle operations

Direct memory operations, versioned patches, semantic bulk forgetting with a dry run and cap, and low-confidence derived-memory review make memory behavior visible and correctable to users.

Sources:

- [Memory operations](https://github.com/supermemoryai/supermemory/blob/958ae8b61960fe2ade495b095a01667cf44c5706/apps/docs/recall/memory-operations.mdx)
- [Memory review](https://github.com/supermemoryai/supermemory/blob/958ae8b61960fe2ade495b095a01667cf44c5706/apps/docs/recall/memory-review.mdx)

### Audit limitations

The linked revision does not expose the current memory server implementation needed to independently verify:

- Transaction boundaries
- Version and conflict consistency
- Deletion and forgetting guarantees
- Tenant-filter enforcement at the storage layer
- Queue durability and replay behavior
- Index-to-canonical-store consistency
- Authorization behavior behind container tags
- Benchmark implementation details inside the hosted product

Supermemory's benchmark numbers should therefore be treated as vendor-published evidence until reproduced using a fixed dataset, configuration, judge, and revision. Its public MemoryBench project is still a useful model for building reproducible evaluation.

Sources:

- [Supermemory README and benchmark claims](https://github.com/supermemoryai/supermemory/blob/958ae8b61960fe2ade495b095a01667cf44c5706/README.md)
- [MemoryBench overview](https://github.com/supermemoryai/supermemory/blob/958ae8b61960fe2ade495b095a01667cf44c5706/apps/docs/memorybench/overview.mdx)
- [Self-hosting overview](https://github.com/supermemoryai/supermemory/blob/958ae8b61960fe2ade495b095a01667cf44c5706/apps/docs/self-hosting/overview.mdx)

## Recommended target architecture

Engram should keep its canonical and evidence-oriented design while adopting the strongest product concepts from Supermemory:

```text
Conversations / files / URLs / connectors
                    |
                    v
       Durable documents and chunks
                    |
                    v
  Async extraction and contextual "dreaming"
                    |
                    v
PostgreSQL canonical memory under tenant RLS
  - nodes and immutable versions
  - typed claims and temporal validity
  - evidence and source provenance
  - derived/update/extension relationships
  - profile materializations
  - review and forget mutations
                    |
          transactional outbox
                    |
                    v
 Search projections: vector + lexical + graph
                    |
                    v
 Hybrid discovery -> rank fusion -> reranking
                    |
                    v
 Canonical hydration -> temporal/conflict/evidence gate
                    |
                    v
 Profile + memory context or grounded answer
```

The critical design rule is that search indexes and graph projections may discover candidates, but only canonical PostgreSQL state may authorize their use in an answer.

## Prioritized implementation roadmap

### P0: correctness, security, and credibility

#### P0.1 Complete the canonical-memory cutover

- Declare PostgreSQL canonical memory as the current architecture.
- Archive filesystem-authoritative V1 documentation.
- Remove or isolate legacy memory mutation and retrieval paths.
- Reduce the public API to one canonical memory representation.
- Add an architecture decision record describing source-of-truth and projection rules.

**Exit criteria:** documentation, API responses, runtime configuration, and recovery procedures describe the same architecture.

#### P0.2 Correct role separation and migrations

- Add a dedicated migration database URL.
- Give every database role a distinct secret.
- Make the migrator own database objects.
- Revoke application DDL access.
- Enforce and test RLS with runtime credentials.
- Validate grants and ownership after fresh installation and upgrades.

**Exit criteria:** an app credential cannot create/alter tables or read another tenant's canonical rows, even when a query omits an application-level tenant filter.

#### P0.3 Establish authoritative CI

CI should create disposable PostgreSQL, Neo4j, and Redis services and execute:

- Unit and integration tests
- Ruff
- Mypy
- Fresh Alembic installation
- Upgrade from the previous released schema
- Cross-tenant RLS tests
- Outbox retry and projection reconciliation tests
- SDK build and contract tests

Database-dependent tests should be marked consistently so that the default developer command either provisions its dependency or reports a clear skip rather than producing unrelated failures.

#### P0.4 Publish reproducible quality benchmarks

Create an Engram adapter for MemoryBench and run at least LoCoMo, LongMemEval, and ConvoMem. Store the following with every result:

- Engram revision and configuration
- Dataset revision
- Embedding, extraction, reranking, and answer models
- Judge model and prompt
- Accuracy and task-specific scores
- Recall@k
- Retrieved and final context tokens
- Ingestion and query latency percentiles
- Cost per conversation and query
- Results with retrieval stages individually disabled

**Exit criteria:** a versioned benchmark artifact can be reproduced from a clean checkout.

### P1: retrieval and memory-product quality

#### P1.1 Add hybrid retrieval

- Store raw source documents and immutable chunks separately from extracted memories.
- Combine dense vector retrieval with PostgreSQL full-text search.
- Use reciprocal-rank fusion or another transparent fusion stage.
- Add an optional reranker for the final candidate set.
- Add query rewriting for references, aliases, and temporal intent.
- Preserve canonical hydration and the evidence gate after ranking.

#### P1.2 Add materialized profiles

- Separate stable facts from dynamic observations.
- Support configurable topic buckets.
- Give each materialized profile a revision.
- Link every profile statement to canonical claims and evidence.
- Provide one low-latency endpoint returning profile context plus query-relevant memories.

#### P1.3 Add reviewable inference

- Mark directly observed and inferred memories separately.
- Record all evidence and parent-memory identities for derivations.
- Down-weight unreviewed low-confidence derivations.
- Add approve, decline, and undo mutations.
- Preserve the complete decision audit trail.

#### P1.4 Add safe semantic forgetting

- Support query-based match preview.
- Require dry-run before large operations.
- Apply configurable result limits.
- Assign a batch mutation ID.
- Support recovery or reversal when retention policy permits.
- Propagate forgotten state to all search projections.

#### P1.5 Implement real adaptive retrieval

- Allow a bounded `NEED_MORE` loop.
- Carry forward unresolved evidence requirements between iterations.
- Enforce iteration, latency, context-token, and model-cost budgets.
- Record re-entry reasons and outcomes in query diagnostics.

### P2: ecosystem, deployment, and maintainability

#### P2.1 Broaden ingestion and synchronization

- PDF and Office document parsing
- URLs with stable source identifiers
- OCR for images and scanned PDFs
- Audio/video transcription
- Connector synchronization with content hashes and delete propagation
- Parser-version tracking for reproducible reprocessing

#### P2.2 Generate and test SDKs

- Make OpenAPI the client contract source.
- Implement a real asynchronous Python client.
- Complete TypeScript route and type coverage.
- Add cross-language contract tests.
- Correct package repository/license metadata.
- Add MCP and major agent-framework integrations after the core APIs stabilize.

#### P2.3 Offer two deployment profiles

1. **Simple profile:** PostgreSQL, pgvector/full-text search, and an in-process or lightweight durable worker.
2. **Scale profile:** PostgreSQL canonical store, Neo4j projection, Redis, Temporal, dispatcher, and independent workers.

Both profiles must use the same canonical schema and API semantics.

#### P2.4 Modularize the canonical repository

Split the current large repository implementation into domain-oriented modules:

- Nodes and versions
- Claims and evidence
- Aliases and entity resolution
- Hierarchy and overviews
- Mutations and lifecycle
- Projection snapshots and dispatch
- Unit-of-work/transaction coordination

The transaction coordinator should remain explicit so modularization does not weaken atomicity.

#### P2.5 Improve supply-chain and project hygiene

- Use a reproducible dependency lock.
- Remove duplicate and ad hoc dependencies.
- Pin container images, preferably by digest for releases.
- Generate an SBOM and run dependency/container scans.
- Add LICENSE, SECURITY, CONTRIBUTING, and CHANGELOG files.
- Add supported-version and migration policies.

## Suggested success metrics

| Area | Metric |
|---|---|
| Recall | Recall@5 and Recall@15 on fixed conversational-memory datasets |
| Answer quality | Dataset score plus grounded-answer precision |
| Evidence | Percentage of answer claims linked to canonical evidence |
| Temporal correctness | Accuracy on current, historical, and superseded-fact questions |
| Tenant isolation | Zero cross-tenant results across API, direct SQL, cache, and projection tests |
| Projection health | Maximum and p95 canonical-to-projection lag |
| Reliability | Outbox success rate, retry count, and reconciliation backlog |
| Latency | p50/p95 ingest acknowledgement, profile retrieval, search, and full answer time |
| Efficiency | Retrieved tokens, final context tokens, and cost per answered query |
| User control | Review acceptance rate, false-memory decline rate, and forget completion time |
| Operability | Fresh-install time, upgrade success rate, and recovery-time objective |

## Validation performed during this audit

A disposable PostgreSQL 16 database was used to avoid modifying an existing user database.

Results:

- `263 passed, 26 deselected` for the non-E2E test selection with PostgreSQL configured
- Ruff completed successfully
- Mypy reported one error in the Python client request body around the `quotas` field
- The default test command without a database URL produced PostgreSQL setup failures rather than consistently skipping database-dependent tests
- The running application container reported successful liveness, readiness, and component health checks
- The inspected canonical PostgreSQL tables had RLS enabled and were owned by the migration role

The temporary audit database container was removed after validation. The later
dashboard graph repair is documented separately in
[`DASHBOARD_GRAPH_FIX.md`](DASHBOARD_GRAPH_FIX.md); it does not change the
comparative conclusions in this report.

## Final recommendation

Engram should not try to reproduce Supermemory feature-for-feature by weakening its current architecture. Its differentiation is the trustworthy canonical layer: typed claims, temporal state, evidence, tenant isolation, recoverable projections, and explicit conflict handling.

The best path is to retain that foundation and add the product capabilities that most directly improve real-world memory quality:

1. Complete and secure the PostgreSQL-canonical V2 cutover.
2. Measure quality through reproducible public benchmarks.
3. Add hybrid document-and-memory retrieval.
4. Add materialized profiles.
5. Add reviewable inference and safe semantic forgetting.
6. Simplify the default deployment and improve SDK/integration quality.

With those changes, Engram can be stronger than Supermemory for teams that need not only useful recall, but also an inspectable explanation of what was remembered, when it was true, which evidence supports it, how it changed, and which tenant is allowed to see it.
