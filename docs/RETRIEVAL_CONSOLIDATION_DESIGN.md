# Retrieval and consolidation engineering design

Reviewed: 2026-09-29. Status: **implementation underway; see [verified changes and remaining work](IMPLEMENTATION_STATUS.md)**. The findings below are the pre-implementation audit baseline. This deepens the [production delivery plan](ORDERED_RETRIEVAL_PLAN.md) at the user's request. It is an implementation specification, not a claim that the changes exist or have passed production evaluation.

BM25 revision: **BM25 candidate retrieval plus vector retrieval and reciprocal rank fusion are required implementation work in RC-G**, following the user's request. Their ranking quality and rollout settings must still pass evaluation. Optional reranking and alternative embedding representations remain experiments. The runtime search path is now implemented and tested on isolated Neo4j 5.24. Held-out ranking/answer-quality evaluation remains a release gate. Tenant/API-key scope applies in this release; private-user and shared-project ACLs are deferred.

The product must both **find enough trustworthy evidence to answer** and **maintain useful memory as evidence changes**. Retrieval and consolidation share source identity, authorization, time semantics and revisions. Improving search ranking without these contracts can surface incorrect memories faster; improving summaries without them can preserve an obsolete answer.

## 1. Architecture decisions

1. Preserve the required progression: **L0 parametric → L1 current session → sufficiency → L2 past sessions → PostgreSQL facts → BM25 + vector discovery → Neo4j relationships**. BM25 and vector retrieval are suboperations of the existing semantic stage, whose compatible public label remains `L3_VECTOR`. Consider each stage in order, skip ineligible stages with a reason, and stop on complete support. Do not query all stores for every question.
2. PostgreSQL owns accepted messages, canonical claims, evidence, history, mutation records and durable work. Graphs, embeddings, caches and summaries are derived. A canonical claim is a supported assertion, not a guarantee of objective truth.
3. Evaluate each accepted **user message** for useful memory asynchronously. Retain assistant turns for conversation, reference resolution and explicit confirmations; assistant text alone cannot become independent fact evidence. Do not run extraction again over the full conversation after each answer.
4. Separate fact formation, context compaction and derived-view consolidation. They have different triggers, correctness rules and freshness needs. Reuse the existing outbox and Temporal workflows; no second broker, autonomous schema editor or new vector database is required.
5. Current-session recall must work immediately after message acceptance. Canonical facts become available after their transaction commits. Neither waits for project summaries or graph/vector catch-up. A new session during pending ingestion may report the last statement and pending status, without claiming canonical readiness.

## 2. Additional repository findings

These findings supplement F01–F26 in the [product audit](PRODUCTION_PRODUCT_AUDIT.md). “Risk” means the implementation permits a failure; it does not claim a deployed incident.

| ID | Observation and evidence | Consequence / priority |
|---|---|---|
| RC01 | **Reproduced:** [canonical overview handler](../engram/consolidation/canonical.py) supplies `subject_id/predicate/object_entity_id/object_value`, while [overview.j2](../engram/prompts/overview.j2) reads `subject_uri/relation/object_uri`. The [prompt environment](../engram/prompts.py) uses `StrictUndefined`; rendering one canonical relationship raises `UndefinedError: 'dict object' has no attribute 'subject_uri'` | Nonempty canonical relationship input can fail before a model call. Fix the typed prompt contract first; retain strict validation |
| RC02 | **Inspected risk:** `consolidate_overview` checks the parent revision after generation. In [memory_repository.py](../engram/storage/memory_repository.py), `_bump_node_revision` changes a specific node and `add_hierarchy` bumps a parent for a newly inserted edge; this is not complete invalidation on existing child-claim changes. `save_overview` writes in a separate transaction without comparing expected input versions | A parent revision cannot certify all contributing source versions; an additional read/write race exists. Require dependency lineage and atomic publication |
| RC03 | **Inspected risk:** [enqueue_task](../engram/storage/postgres.py) deduplicates active `(tenant_id,node_id,task_type)` jobs with `ON CONFLICT DO NOTHING`. It records neither a new requested source generation on duplicate enqueue nor a trailing-run requirement. `overview_debounce_seconds` is declared but not applied by this enqueue path | Active-job deduplication is not sufficient coalescing. Changes during work can remain unprocessed; define dirty generations and race-safe follow-up |
| RC04 | **Inspected:** [EvidenceGate](../engram/retrieval/evidence.py) can declare surviving verified rows sufficient without modelling every requested answer component. `_query_covered` uses lexical overlap; `_conflicts` does not consult registered predicate cardinality | Add explicit coverage requirements; distinguish legitimate `MULTIPLE` values from incompatible `SINGLE` assertions |
| RC05 | **Inspected:** `_temporal_match` in [evidence.py](../engram/retrieval/evidence.py) excludes rows whose `asserted_at` is after an as-of point, even if their valid interval covers that point. [TemporalScope](../engram/retrieval/domain.py) has no independent valid-time and known-at bounds | “What was true then?” and “What had you learned by then?” cannot share one ambiguous filter. Add versioned semantics without silently changing legacy `AS_OF` |
| RC06 | **Inspected risk:** [graph tools](../engram/retrieval/tools.py) verify paths, then flatten evidence; [ContextBuilder](../engram/retrieval/context.py) can truncate an individual evidence block. Its default 200,000-token window is not proof of the chosen provider's usable limit | Preserve complete evidence bundles and reassess coverage after packing; derive budgets from actual model configuration |
| RC07 | **Inspected risk:** [Neo4j vector search](../engram/storage/neo4j_store.py) filters tenant/status after global ANN top-K selection; [session tools](../engram/retrieval/tools.py) skip oversized records whole | Authorized candidates can be crowded out, and useful original text can be omitted. Measure tenant-skew recall and use bounded source excerpts |
| RC08 | **Inspected:** canonical consolidation reads each child's current state separately and the prompt carries unbounded child lists; an output token limit does not bound input size | Batch bounded snapshot reads and impose input/fanout budgets. Large scopes require resumable partitions with source lineage |

The RC01 probe used synthetic dictionaries and local prompt rendering only. No database, model or production data was contacted. Other RC findings are code observations or concurrency risks, not new load-test results.

## 3. Retrieval contracts

Extend the existing [domain models](../engram/retrieval/domain.py), [planner](../engram/retrieval/planner.py), [cascade](../engram/retrieval/cascade.py) and [tools](../engram/retrieval/tools.py). Keep public stage names separate from the legacy repository route enums. Do not create a second competing orchestrator.

### Request and plan

| Contract | Required information and ownership |
|---|---|
| Request identity | Request/run/message IDs, accepted session revision and deadline; server-owned |
| Access scope | Tenant, principal, permitted project memberships and authorization-policy version; authenticated server context only |
| Message act | Question, statement, correction, confirmation, mixed input or forget request; distinguishes a conversational action from evidence retrieval |
| Answer requirements | A small bounded list of facts, time constraints or relationship steps needed to answer the whole question; each has a stable requirement ID |
| Search plan | Intent, validated entity/alias resolutions, predicate hints, source kinds, temporal mode, remaining ordered stages and maximum depth |
| Budget | Remaining wall time, model/tool calls, candidates, hops, input/output tokens and bounded repair attempts; enforced outside the model |

For “Where do I live and which company do I work for?”, current location and current employer are separate requirements. One supported location claim cannot make the whole question answerable. For ambiguous names, retain multiple candidates or request clarification instead of merging identities by spelling.

The model may propose hints and missing requirements through validated structured output. The server enforces scope, tool eligibility, stage order and budgets. It rejects fabricated evidence IDs and treats stored instructions as data. Add no unconditional classifier/model call at every stage; use deterministic checks where they suffice and measure the cost of optional semantic decisions.

### Evidence and outcome

An evidence envelope needs stable source/claim IDs, source kind, speaker, source span, canonical revision, valid interval, assertion/acceptance time, access scope, deletion state and provenance. Keep retrieval score separate from claim confidence. Neither a cosine score nor a model confidence estimate is proof of support.

Carry evidence as **bundles** where multiple records are jointly necessary: a graph path, a correction plus the superseded claim, or a confirmation plus its exact question. Each bundle identifies the answer requirements it supports and the source IDs required to verify it. Conversation evidence is explicitly distinct from canonical claims. A summary is a discovery/presentation artifact whose underlying evidence still needs checking.

Reuse the four answerability states with explicit meanings:

| State | Meaning and behavior |
|---|---|
| `ANSWERABLE` | All required components are supported by eligible evidence retained in the final context; stop deeper retrieval |
| `PARTIALLY_ANSWERABLE` | Some components have support; continue only if a later allowed stage could fill the gap, otherwise provide the supported portion and identify what is missing |
| `CONFLICTING_EVIDENCE` | Relevant incompatible assertions remain after cardinality and time checks; explain the conflict or ask a focused clarification |
| `INSUFFICIENT_EVIDENCE` | No adequate support for the requested answer; do not turn a plausible guess into a remembered fact |

Tool failures, deadline exhaustion and authorization exclusions must have internal reason codes. Public errors must not reveal inaccessible sources or their existence. “No matching evidence” and “search unavailable” are different outcomes.

## 4. Stage behavior and stopping rules

### L0 and L1

L0 is eligible for self-contained, stable public knowledge. Public names alone should not disable it; ambiguous private references and time-sensitive claims must not receive unsupported parametric answers. If current external information is required and no authorized external capability exists, disclose that limit rather than search private memory as a substitute.

L1 reads the latest accepted user message and a bounded conversation tail, using PostgreSQL after a cache miss or when cache freshness cannot be trusted. Search older original turns in the same session when needed. Keep speaker boundaries and dates; resolve “yes” only against a clear bounded referent. A recent user correction can support “You just told me…” immediately, but does not mean durable extraction has already finished.

Perform the sufficiency decision after eligible session work. Session statements can support contextual answers and quoted last-stated facts. Their dates, known contradictions and the question's current-versus-historical meaning determine whether a stronger canonical lookup is needed. Do not claim an old statement has been independently verified as current.

### L2: past conversation recall

Keep PostgreSQL original records authoritative and exclude the active session. Use summaries only to locate relevant originals. Return dated excerpts with role and stable event/message IDs; optionally load a bounded adjacent turn to resolve a reference. Replace whole-record skipping with bounded, traceable excerpts from large records. Never discard negation or speaker attribution while excerpting.

Keep the existing PostgreSQL full-text search for L2; its `ts_rank_cd` ranking is not BM25. The required BM25 integration below initially applies to canonical knowledge discovery after the facts stage, so past-session retrieval remains in PostgreSQL. Evaluate paraphrase misses, names, code identifiers and the actual supported languages. Changing the L2 index requires a separate evaluated extension. A retrieved assistant statement answers “What did the assistant say?” but cannot independently establish the user's employer or preference.

### PostgreSQL facts and history

Resolve scoped identity and predicate first; ambiguous resolution is not permission to select the highest similarity. Read typed current/history claims with evidence and cardinality policy. Batch authorized hydration of IDs discovered elsewhere. Check status, tombstones, source support, validity and scope before context construction.

Add explicit valid-time and known-at modes. For example, a fact learned on 10 February can describe where someone lived in January. It can answer a retrospective January question; it cannot appear in an answer limited to what the system had learned by 31 January. For canonical claims, define `known_at` from committed canonical history, with recorded-from/recorded-until versions alongside valid-from/valid-until intervals. Preserve those prior versions when correcting an assertion. For “What had I told you by then?”, use original message acceptance time instead. Keep user assertion time separately; an extractor cannot assign the system's knowledge clock. Version the API semantics and compatibility adapter for existing `AS_OF` users.

Current facts require open/current validity, supported evidence and any future effective-date rules. `MULTIPLE` predicates permit multiple valid values; they are not inherently conflicts. `SINGLE` predicates with incompatible overlapping validity require resolution or explicit conflict. Historical superseded claims remain queryable; deleted or inaccessible evidence does not.

### Semantic discovery after insufficient facts

Within this eligible stage, **implement BM25 lexical retrieval and vector retrieval under one shared budget, then combine their eligible ranked lists using RRF**. They may run concurrently inside this stage. This does not permit hybrid discovery before earlier stages are considered. A fresh canonical row may replace a stale projection candidate only after its current content is reevaluated for relevance; never retain stale text as support.

### BM25 and vector hybrid search — required implementation

**Purpose.** BM25 supplies lexical candidates for names, identifiers, uncommon terms and quoted wording; vectors supply semantically related candidates. Neither channel alone proves the answer. For example, a query containing a specific deployment error code should have a lexical route, while a differently worded preference question should retain semantic discovery. These are evaluation cases, not promised benchmark gains.

**Chosen first backend: the existing Neo4j full-text/Lucene capability.** The repository pins Neo4j 5.24 Community and already declares `l0_text_idx`, but the active retrieval code does not query it. Neo4j full-text indexes require an explicit `db.index.fulltext.queryNodes` call; creating an index alone does not wire retrieval. See the [Neo4j 5 manual](https://neo4j.com/docs/cypher-manual/5/indexes/semantic-indexes/full-text-indexes/).

The backend choice is supported by version-specific source inspection: Neo4j 5.24.0's [StatsCachingIndexSearcher](https://github.com/neo4j/neo4j/blob/5.24.0/community/fulltext-index/src/main/java/org/neo4j/kernel/api/impl/fulltext/StatsCachingIndexSearcher.java) inherits Lucene's searcher without replacing similarity; its [dependency manifest](https://github.com/neo4j/neo4j/blob/5.24.0/pom.xml) pins Lucene 9.11.1, whose [default similarity is BM25](https://github.com/apache/lucene/blob/releases/lucene/9.11.1/lucene/core/src/java/org/apache/lucene/search/IndexSearcher.java). This supports a native BM25 implementation without a new search service or PostgreSQL extension. It is source verification, not a live ranking test; the exact deployed version/index must still pass the compatibility suite.

BM25 uses term frequency saturation, document length and corpus term statistics. Lucene's [BM25 implementation](https://github.com/apache/lucene/blob/releases/lucene/9.11.1/lucene/core/src/java/org/apache/lucene/search/similarities/BM25Similarity.java) defaults to `k1=1.2` and `b=0.75`. Record those engine defaults; do not invent application knobs that Neo4j's supported interface does not expose. PostgreSQL's [`ts_rank_cd` uses cover-density ranking](https://www.postgresql.org/docs/16/textsearch-controls.html#TEXTSEARCH-RANKING), so it remains explicitly labelled FTS when used as a fallback.

#### Indexed content and identity

- Start with the existing canonical **memory ID** as the common fusion unit for both channels. Each candidate includes tenant, stable memory ID, projected canonical revision, representation version, channel/rank and index generation. Source text is loaded from PostgreSQL after authorization. Do not fuse a claim ID and a memory ID as if they were the same item.
- Create a versioned canonical full-text index over deterministic search text derived from the eligible memory version and its owned supported claims: names/aliases, predicate wording, typed values and bounded source context. The initial corpus covers current eligible canonical memories. Historical questions retain PostgreSQL history retrieval; archived lexical indexing is a separate extension.
- Keep raw user/assistant conversations out of this knowledge index. Summaries are optional discovery hints with dependency metadata, not an independent BM25 evidence corpus. Do not build lexical text by asking a model to rewrite facts or append guessed synonyms.
- The old abstract-only `l0_text_idx` is a migration baseline, not sufficient proof of claim-text coverage or access filtering. Use a new index name/version with explicit text and scope fields; preserve the old index through rollback. Atomic-claim or graph-neighborhood representations may be added later with explicit parent mapping and a separate evaluation.
- Version analyzers/tokenization and test supported languages, punctuation, acronyms and code identifiers. Keep exact identifiers available through typed/exact lookups when analysis splits them. Do not claim language-independent recall from the default analyzer.

#### Scope and query construction

The adapter accepts bounded plain search terms and server-owned scope; it never accepts model/user-authored Lucene, Cypher, index names or tenant clauses. Build the full-text query in one controlled helper, escape search syntax, cap terms/clauses, and use parameterized Cypher. A SQL/Cypher parameter does not by itself neutralize Lucene operators inside its string value.

Include opaque server-generated tenant and private/project access tokens in dedicated indexed fields. Construct mandatory scope clauses **inside the Lucene query before its top-K limit**, rather than retrieving global top-K and filtering afterward. Ensure scope tokens round-trip as exact tokens through the selected analyzer; validate membership against PostgreSQL again before fetching text. The implementation must prove that a text match cannot satisfy a scope clause and that input cannot escape its grouped query. If the selected index cannot meet those constraints, keep the feature disabled for affected scopes until a safe candidate-selection strategy is implemented; silently omitting scope is never a fallback.

Scope filtering does not make Lucene's corpus statistics tenant-local. Record that scoring uses the physical index's statistics; never compute IDF from only the returned top-K, expose private term statistics as user evidence, or promise tenant-independent scores. Test heavily imbalanced tenants and ACL revocation. Stale projected grants can still crowd out valid candidates; perform bounded refill after canonical filtering, and report incomplete discovery when the budget ends. Overfetching is not an isolation mechanism or a guarantee of complete recall.

#### Fusion, hydration and stopping

1. Enter the stage only after earlier ordered stages are insufficient or explicitly ineligible and the caller's maximum depth permits it.
2. Run BM25 and vector candidate generation concurrently with independent bounded result caps under the shared request deadline. BM25 must work when embedding generation is unavailable; the vector channel must work when the lexical index is unavailable.
3. Deduplicate each channel by the common canonical identity. Union stable IDs, batch-hydrate PostgreSQL records, and exclude inaccessible, deleted, temporally wrong or unsupported candidates. Reevaluate relevance on current canonical text if a projection revision changed. Preserve both original and eligible ranks in restricted diagnostics.
4. Rank each eligible list from one and fuse with `RRF(d) = w_bm25 / (k + rank_bm25(d)) + w_vector / (k + rank_vector(d))`. An absent item contributes zero; it does not receive rank zero. Initially use equal channel weights and `k=60`, then tune on development data and evaluate on held-out data. Use stable canonical ID tie-breaking. This adapts the [original RRF method](https://cormack.uwaterloo.ca/cormacksigir09-rrf.pdf).
5. A duplicated projection or several matching text fields do not give one memory multiple votes within a channel. When a channel is unavailable or returns nothing, preserve the surviving channel's eligible order and record the distinction between empty and failed. Never add raw BM25 and cosine scores directly.
6. Pack the selected evidence bundles, check complete-question coverage again, and answer only with retained supported evidence. If support remains insufficient, consider bounded graph retrieval next. Hybrid search does not grant an automatic `ANSWERABLE` outcome.

Proposed initial evaluation budgets are twenty candidates per channel, at most forty unique candidates for batch hydration, and twelve final memory candidates before bundle/token packing. They are new experimental settings, not current configuration or measured optimums. Cap each call by remaining request time and existing global limits; cancel unfinished optional work on deadline/Stop. Do not stack a full timeout independently for every channel and fallback. Keep `L3_VECTOR` compatible externally and expose additive metadata such as `retrievers_used`, `fusion_method`, index version and degradation reasons.

#### Projection lifecycle and failure handling

Reuse revisioned projection activities and the transactional outbox. Claim edits, corrections, evidence withdrawal, unmerge, access changes and deletion update/invalidate affected lexical text with stable identity and tombstone fencing. Where a claim affects its containing memory document, enqueue that document's regeneration explicitly; a child update must not leave its parent's indexed text silently stale. Updating the index is bounded background work, independent of interactive response and derived overview generation.

Backfill from eligible PostgreSQL canonical rows with checkpoints, replay later revisions, wait for the new index to be `ONLINE`, verify indexed fields/analyzer/counts and sample canonical hydration, then switch the configured version for a canary. Existing `ensure_indexes` logs errors; feature readiness must explicitly check this required capability rather than treating a swallowed creation error as success. Rollback changes the active index/ranking mode without deleting canonical data.

If Neo4j is down, both native BM25 and vector discovery may be unavailable together. Continue authorized session/exact/history capabilities and the explicitly labelled PostgreSQL FTS fallback where useful; do not call that fallback BM25. If PostgreSQL verification fails, no projected private text becomes answer evidence. Track lexical/vector latency, fusion contribution, filtered/refilled counts, revision lag, index readiness, supported-answer quality and per-channel degradation.

Compare representations on the same labelled questions and budgets:

| Representation | What to index | Key tradeoff |
|---|---|---|
| Existing projection | Current repository projection as the control | Establish real baseline errors before changing it |
| Atomic claim text | Subject, normalized predicate, typed value/entity and minimal supporting context | Precise hydration and updates; isolated fragments may miss broader context |
| Bounded graph-neighborhood text | Selected authorized relationships with all contributing claim IDs and revisions | Broader relational meaning; larger invalidation and access-control surface |
| Combined candidates | Separately versioned claim and neighborhood candidates, deduplicated after hydration | Potential recall gain with extra indexing/query cost; must win the evaluation |

Do not embed the entire knowledge graph into one undifferentiated document. Projection records need representation/model versions, content hash, source IDs, canonical revisions and deletion fencing. Test recall with a small authorized scope surrounded by a much larger unrelated tenant. Authorized candidate selection is preferred; bounded oversampling alone cannot guarantee recall. If the current index cannot satisfy scoped discovery, evaluate a solution within the existing stores before proposing new infrastructure.

Add reranking only after measuring the baseline, with a bounded candidate list and a fallback. Reranking can improve ordering; it cannot grant access, create evidence, resolve source contradictions or replace the coverage gate.

### Graph relationships last

Use controlled parameterized templates for relationship questions or remaining path requirements. Start from validated entities, apply authorization to every node and edge, and enforce direction, predicate, time, hop, row and execution limits. Validate each returned edge against PostgreSQL and preserve the path as one evidence bundle.

Do not answer a two-hop relationship from only one surviving edge. Do not invoke graph search merely because earlier stages found nothing when the graph cannot supply the missing kind of evidence. No-result or timeout is not proof that no relationship exists. LLM-generated Cypher and schema mutation remain outside the launch scope.

## 5. Context, freshness and latency

Compute the usable context from the configured provider/model limit minus system instructions, tool schemas, query, output reservation and a safety margin. The current 200,000-token builder default must not serve as a universal model limit. Preserve complete source identifiers and provenance; trim optional prose or select fewer bundles instead of truncating required evidence mid-record.

Run coverage again **after packing**. An answer cannot claim support that was retrieved but omitted from the actual prompt. Recheck selected source authorization and tombstones before final generation and before releasing buffered output. For streaming, validate before the first answer token and propagate revocation/cancellation to stop further output; already delivered text cannot be recalled. Cite the evidence snapshot used rather than promising perpetual freshness during a long generation.

Current [retrieval defaults](../engram/config.py) include a 6,000-token session budget, eight past-session results, twelve configured vector results, graph depth two with maximum four, and a three-second graph timeout. These are existing configuration values, not measured optimal settings. Introduce an end-to-end deadline and shared call/token accounting so individually bounded stages cannot collectively exceed the request budget. Profile before increasing result counts.

Record considered/skipped/attempted/sufficient stages, gap reasons, candidate counts before/after verification, packing losses, model calls, token usage and latency. Do not emit raw private text in default telemetry. Cancel downstream work on user Stop or expired deadlines. A bounded qualified answer is preferable to an unbounded search loop.

## 6. Three distinct memory operations

| Operation | Trigger and input | Durable result | Timing and trust |
|---|---|---|---|
| Selective fact formation | Accepted user message plus bounded explicit source context | Validated claims, evidence, temporal history and mutation/outbox records | Evaluate each eligible turn; no assistant-only promotion; response generation proceeds independently |
| Session compaction | Context threshold or lifecycle policy over an explicit message-sequence range | Derived digest with source range, input revision and generator version | Helps context size; preserves original records under retention policy; never overwrites newly appended turns |
| Derived consolidation | Relevant canonical changes, missing/stale views, controlled maintenance or explicit rebuild | Source-linked entity/collection/project views and their dependencies | Debounced and bounded; never required before a committed fact becomes retrievable |

The canonical worker currently supports `CONSOLIDATE_OVERVIEW` and `UNMERGE_CANONICAL`. Legacy task names such as `ATOMIZE` and `NORMALIZE` must not be presented as implemented canonical consolidation. See the corrected [current consolidation flow](CONSOLIDATION_FLOW.md). Identity unmerge is a controlled corrective mutation, separate from summary generation; automatic aggressive identity merges are not a prerequisite.

### Canonical actions and source discipline

| Action | Decision rule | Invariant |
|---|---|---|
| `ADD` | A supported assertion is new for the scoped identity, predicate and validity | Create claim plus source evidence atomically |
| `REINFORCE` | A genuinely distinct supporting source supports the same compatible assertion | Add evidence without duplicating a current fact or treating retries/quoted copies as independent corroboration |
| `SUPERSEDE` | An authorized correction/change resolves a single-valued fact with a known effective order | Preserve old history, close/adjust validity deliberately and point to the successor |
| `CONTRADICT` | Incompatible supported assertions cannot be resolved under source, cardinality and time policy | Preserve alternatives and conflict metadata; model confidence alone does not select a winner |
| `NOOP` | Same mutation/source already applied, or evaluated input carries no eligible new fact | No duplicate writes or confidence inflation; retain appropriate processing outcome |

Normalize predicates and typed values through versioned policy. Preserve explicit negation, uncertainty and future effective dates. Same spelling is not sufficient identity evidence. Sensitive or unsupported inferred traits must not become facts simply because they occur in a generated summary. User corrections can trigger these actions; periodic summary consolidation must not silently revise canonical truth.

Use stable source/mutation identity, not model wording, for retry deduplication. Relative time resolves against the original accepted message/timezone. Compare changes under the existing scoped canonical transaction protocol; older delayed work must not win simply because it commits last. Explicit re-extraction uses a new processing policy/version and controlled reconciliation, rather than replaying an old mutation as if it were new evidence.

## 7. Derived-view lineage and publication

### Required records

Extend existing PostgreSQL records through an additive migration; the table design is an implementation review item, not a demand to build a separate storage service.

| Logical record | Required contract |
|---|---|
| View version | Tenant, scope, view kind, immutable version ID, content, generator/model/prompt/policy versions, bounded-input manifest hash, generation time, coverage/truncation metadata and lifecycle state |
| Source dependency | Typed reference to each contributing claim, memory version, original message or child view; exact source version/revision, access scope and deletion state. Enforce tenant-consistent references; do not hide all dependencies only in opaque text |
| Scope generation | Monotonic requested/dirty generation, last published generation, scope membership revision and policy revision; mutations and publishers participate in the same locking protocol |
| Current view reference | Atomically selected published version for the scope and view kind; an input fingerprint and generator version identify one idempotent publication |
| Job progress | Captured input generation, requested generation, not-before deadline, attempt/error state and bounded partition cursor; reuse `consolidation_tasks` and existing dispatches |

A sorted input manifest contains the actual evidence dependencies and revisions. A checksum alone does not detect a newly added child that was absent from the old manifest; the scope membership/dirty generation must also change. Scope membership, source mutation, deletion and policy changes participate in invalidation. Unknown lineage means unverified/stale, never implicitly current.

### Read, generate, verify, publish

1. Under a short scope-generation lock, capture the dirty generation and a bounded, authorized dependency snapshot. Use a consistent read transaction or version-verified batch reads. Do not hold a database transaction or lock during a model call. PostgreSQL's [transaction isolation documentation](https://www.postgresql.org/docs/16/transaction-iso.html) describes the snapshot guarantees; isolation alone does not replace the publication check.
2. Assemble a typed, tested canonical prompt. Supply normalized human-readable entities/values plus source IDs; keep `StrictUndefined` so contract drift fails visibly. Record input size, selected coverage and omitted scope. Represent conflicts and unknowns explicitly.
3. Generate outside the transaction. Require structured statements with supporting IDs and a presentation body. Reject unsupported IDs, invalid types, empty output and size violations. Evidence references are necessary but not sufficient: evaluate whether the cited source actually supports each statement. Prefer deterministic rendering for simple factual sections; do not trust a second model as the sole validator.
4. Reenter a short transaction using the same scope/dependency locking protocol as source mutations. Recheck generation, membership, policy, tombstones and dependency revisions; atomically insert the immutable view version and change the current reference only if all checks still match. A reread followed by an unrelated write transaction is insufficient.
5. If inputs changed, discard the candidate publication, keep the scope dirty and schedule fresh work. Do not retry saving the same stale text under a newer revision. If the artifact already exists for the same input fingerprint/generator version, return it idempotently rather than overwrite it with a different completion.
6. Publish cache invalidation/projection work durably with the committed publication. Cache entries carry view ID, scope generation and policy version. Readers validate freshness/access before using summary content; when uncertain they use authorized canonical evidence or report a summary is updating.

This protocol is deliberately stronger than the current parent-revision check. Every mutating path must participate, including direct corrections, hierarchy changes, deletion, unmerge and administrative repairs. Test the protocol with concurrent transactions; hashing a snapshot does not by itself close a race.

For large scopes, build bounded partitions and then a parent view with versioned child dependencies and transitive access/deletion tracking. Do not create recursive summaries with no path back to original evidence. Mixed-access summaries must not be shown to a viewer who lacks any contributing source; prefer separate scope-coherent artifacts or compose from authorized source bundles at read time. Avoid producing an unbounded cache of summaries for arbitrary ACL combinations.

## 8. Temporal scheduling, retries and recovery

Temporal owns durable execution; PostgreSQL owns intended state and idempotent effects. Activities can execute again after failures, so persisted effects must tolerate retries as described in [Temporal's activity guidance](https://docs.temporal.io/activity-definition). Keep workflow inputs to stable IDs where possible; avoid duplicating raw conversation content into workflow history.

### Coalescing without lost changes

1. A relevant canonical transaction advances the affected scope generation and requests work through the existing task/outbox mechanism. Maintain bounded explicit dependency/scope membership so invalidation is complete; cascading expensive regeneration may be asynchronous, but stale source data must be excluded immediately at read time.
2. If a job is pending, merge the newer required generation. Use a trailing debounce with a maximum wait measured from the first dirty event, so a busy project is not postponed indefinitely. The existing 30-second setting is a candidate starting delay; maximum wait and freshness SLOs require workload measurement.
3. Schedule with the existing dispatch `available_at` facility, atomically aligned with the task's not-before time. Once dispatch is claimed, treat it as active: later changes update dirty generation rather than attempt to undo a started workflow. If a start races a deadline adjustment, the worker rechecks the persisted due time and uses a durable Temporal timer when necessary, never a sleeping process holding a database lock.
4. A running job captures generation G. Changes arriving during generation advance the requested generation under the same scope lock. Completion atomically records the processed generation and either clears dirty state or creates the next task plus dispatch if newer work remains. Releasing the active-task uniqueness constraint and creating follow-up work must be one transaction to avoid lost wakeups.
5. Reconciliation finds missing dispatches and dirty generations without active work. It is a repair mechanism, not the only way that a child correction eventually refreshes a parent. Include per-tenant fairness, bounded retries, backlog age and model token/concurrency limits.

Deletion/revocation immediately excludes sources from reads and blocks late publications, regardless of debounce. Prioritize cleanup; regenerate only from remaining authorized evidence. Stop scheduling unnecessary derived work during pressure while preserving accepted messages and canonical durability within the published backlog policy.

### Classify completion correctly

| Outcome | Planned treatment |
|---|---|
| Transient database/model/network failure | Bounded Temporal retry with stable mutation/job identity; reuse durable artifacts where safe |
| Source generation changed | Expected invalidation: replan from fresh sources, coalesce follow-up, do not repeatedly submit stale content |
| Invalid prompt/schema/unknown task type | Terminal classified failure with actionable redacted diagnostics; repeated model calls cannot repair a local field mismatch |
| Deleted scope or no eligible sources | Safe cancelled/no-content outcome; clear stale published content and record completion without resurrection |
| Crash after publish before activity completion | Retry detects committed fingerprint/version and finishes bookkeeping without another mutation |
| Exhausted retry budget | Failed task visible to operators and memory-status APIs, alert by age/impact; explicit controlled replay after cause is resolved |

Current Temporal activities leave ordinary failures `PROCESSING` for retry; terminal workflow handling marks tasks `FAILED`. The dispatch can be marked complete even for a failed task because handoff is finished. Task success, dispatch delivery, canonical commit and summary freshness must remain separate operational signals.

## 9. Worked product scenario

Suppose the user says on **10 February**, “I lived in Pune throughout January. I moved to Bengaluru on 1 February.”

1. Save the message before answering; same-session questions can use the statement immediately with its source/date. Temporal evaluates it and commits the two valid-time intervals with provenance. This does not wait for an overview.
2. In a new session after canonical commit, “Where do I live?” reaches authorized current evidence and answers Bengaluru. “Where did I live in January?” answers Pune with retrospective support. “What had you learned about my location by 31 January?” cannot use a message first received on 10 February.
3. If an older January statement finishes processing after the February move, event/effective ordering preserves the move. Repeating the same activity adds no second fact or reinforcement.
4. A project summary starts with generation 12. A correction changes a child claim and advances the scope to 13 while the model is running. Publication of generation 12 fails its atomic check even if the parent's ordinary node revision did not change. A coalesced run processes generation 13.
5. If the vector/graph projection still describes Pune, PostgreSQL hydration and validity checks prevent it being used as current evidence. If the user deletes the location memory during generation, tombstones exclude it immediately and fence the pending summary/projection writes.

This scenario joins ingestion, temporal retrieval, projection lag, consolidation and privacy in one contract. Test it end to end rather than proving each class only in isolation.

## 10. Implementation packages and dependencies

These are reviewable work packages within M0–M6 of the parent plan, not a second delivery schedule. Retrieval refinements can be developed against validated fixtures while durable user ingestion is built; production enablement depends on ownership and source integrity.

| Package | Main implementation paths | Deliverable and gate |
|---|---|---|
| RC-A — Correct existing contracts | `consolidation/canonical.py`, `prompts/overview.j2`, `retrieval/evidence.py`, existing prompt/retrieval tests | Reproduce/fix canonical prompt mismatch; typed literal/entity rendering; cardinality-aware conflict tests. Do not weaken strict prompt validation |
| RC-B — Evidence and temporal semantics | `retrieval/domain.py`, `planner.py`, `evidence.py`, `storage/memory_repository.py`, API/SDK temporal adapters | Bounded answer requirements, explicit valid/known-at semantics, scoped batch hydration and versioned backward compatibility |
| RC-C — Ordered evidence assembly | `retrieval/cascade.py`, `tools.py`, `context.py`, `storage/control_plane.py`, `session_search.py`, controlled Cypher templates | Original-record excerpts, complete path bundles, after-packing coverage, provider-derived budgets and shared deadlines; no unbounded escalation |
| RC-D — Safe source mutations and invalidation | `temporal/activities.py`, `storage/memory_repository.py`, `storage/postgres.py`, `storage/canonical_schema.py`, new Alembic migration | User-grounded processing, ordered corrections, distinct-source reinforcement, dirty generation and lineage invalidation in canonical transactions |
| RC-E — Derived-view publication | `consolidation/canonical.py`, repository overview methods, overview schema/prompt, cache integration | Bounded consistent input snapshot, exact source lineage, immutable versions and atomic publication; child mutation and delete race tests |
| RC-F — Durable coalescing and recovery | `storage/postgres.py`, `temporal/workflows.py`, `temporal/activities.py`, `consolidation/worker.py`, consolidation status/trigger API | Due-time scheduling, trailing-run guarantee, idempotent completion and explicit task/freshness states; actual worker restart validation |
| RC-G — Required BM25 and vector hybrid retrieval | `storage/neo4j_store.py`, `storage/memory_kg.py`, projection activities, `retrieval/tools.py`, `retrieval/orchestrator.py`, a small fusion helper, config/metadata and evaluation tests | G1: versioned scoped BM25 index and query adapter; G2: bounded concurrent channels, canonical hydration and deterministic RRF; G3: real-engine ranking/isolation/rebuild/fallback tests plus held-out quality evaluation. Optional reranker and alternative representations follow separately |

RC-A is immediate corrective work in the first implementation slice. RC-B/C establish retrieval contracts; RC-D/E/F make consolidation safe and belong in M2/M3, before enabling derived summaries as dependable answer context. **RC-G's BM25 adapter and RRF integration are required M3 delivery**, dependent on RC-B/C scope/evidence contracts and revisioned projection writes. Their rollout settings are evaluated; a failing quality/isolation gate requires correction before enablement. Optional neural reranking and representation comparisons remain experiments. M4 completes user-facing lifecycle controls and retention/deletion drills using the source lineage already established.

For the in-memory development adapter, expose the same candidate contract with an explicit capability flag. Either provide a separately verified BM25 scorer for its small fixture corpus or mark that capability unavailable and exercise the documented fallback. A token-overlap mock must not be labelled BM25 or substituted for the real Neo4j acceptance tests. Avoid loading the production corpus into each API process to run a Python BM25 library per request.

## 11. Tests, measurements and release gates

Add the following focused acceptance cases alongside A01–A33; do not replace the broader product/security gates.

| ID | Scenario | Required proof |
|---|---|---|
| RC-T01 | Overview containing entity relationships and literal typed values | Canonical prompt renders intended values/IDs; strict missing fields fail clearly; model invocation is not used to conceal a template error |
| RC-T02 | Two-part question with only one supported component | Gate and final response identify the missing component; no false complete answer |
| RC-T03 | Two legitimate `MULTIPLE` values versus two incompatible `SINGLE` values | Only the latter becomes an inferred conflict when validity overlaps |
| RC-T04 | Fact learned later about an earlier interval | Valid-time and known-at answers differ correctly; old API semantics remain documented/tested |
| RC-T05 | Graph path fits search results but not final token budget | Complete path retained or excluded; after-packing gate never reports unsupported coverage |
| RC-T06 | Relevant sentence in a very long original turn | Bounded excerpt preserves source, speaker, time and negation; whole record is not silently lost |
| RC-T07 | Small authorized scope among many unrelated ANN neighbors | Scoped evidence recall measured separately from isolation; no inaccessible IDs/text reach answer context |
| RC-T08 | Change existing child claim without adding a hierarchy edge | Parent view becomes dirty; old artifact cannot claim currentness |
| RC-T09 | Mutation between generation validation and publication | Concurrent PostgreSQL transactions prove atomic rejection; stale text cannot be relabelled with a new revision |
| RC-T10 | Updates while pending, running and finishing consolidation | Latest dirty generation eventually processed with bounded coalescing and no lost follow-up |
| RC-T11 | Worker crashes after publishing, before marking task complete | Retry reuses committed artifact; no duplicate publication, reinforcement or new model completion required |
| RC-T12 | Delete/revoke/unmerge while summary, projection or extraction is running | No inaccessible/deleted content reappears; derived scope/lineage invalidates correctly |
| RC-T13 | Summary invents a detail, uses a nonexistent ID or mis-cites a real ID | Reject/flag invalid support; no canonical claim promotion; quality evaluation detects semantic mis-citation |
| RC-T14 | Delayed correction and repeated extraction of the same source | Validity and accepted order preserved; distinct-source counts stable on retries |
| RC-T15 | Huge project, continuous edits and model outage | Bounded input/concurrency, no indefinite debounce starvation, fair tenant scheduling and visible freshness lag |
| RC-T16 | Private and shared claims feed an overview; viewer loses one grant | Mixed-access text withheld or rebuilt from eligible evidence; caches and source details obey revocation |
| RC-T17 | Concurrent session append and compaction | Exact covered range recorded; newer messages survive; summary is not promoted to original evidence |
| RC-T18 | Task fails after retry exhaustion but dispatch is complete | Status UI/metrics report failed consolidation rather than successful memory freshness |
| RC-T19 | Real BM25 rare/common terms, repeated terms, long/short texts, identifier and language fixtures | Version-pinned Lucene/Neo4j scoring and analyzer behavior recorded; active query adapter used, not a mocked score or an index declaration |
| RC-T20 | BM25-only, vector-only and overlapping candidates; duplicates and ties | Deterministic 1-based RRF, zero contribution for missing channel, one vote per identity/channel, stable ties and bounded union/hydration |
| RC-T21 | Lexical query includes quotes, operators, wildcards, field names or forged scope; small tenant under large unrelated corpus | Query builder cannot widen scope; authorized candidate selection precedes top-K; canonical verification blocks stale grants and no private text reaches the model |
| RC-T22 | Embedding failure, missing/POPULATING lexical index, Neo4j outage and deadline cancellation | Surviving channel used within shared budget; PostgreSQL FTS fallback labelled honestly; failed versus empty distinguished; no verification bypass |
| RC-T23 | Correct/delete/unmerge/revoke while lexical projection or index backfill is running | Older revisions cannot restore obsolete text; current authority wins; rebuild checkpoints and rollback remain safe |
| RC-T24 | Sufficient L0/L1/L2/facts answer, or depth limit excludes semantic stage | Zero BM25/vector calls; stage order preserved; graph considered only after remaining hybrid evidence gaps |
| RC-T25 | Fact changed in PostgreSQL but BM25 still returns its old memory projection | Fresh authorized hydration and relevance/validity checks decide support; no stale keyword snippet becomes a current fact |
| RC-T26 | Held-out comparison of current baseline, BM25-only, vector-only and BM25+vector RRF | Report required-evidence recall, nDCG/MRR where labelled, supported answers, abstentions, tenant skew, p95 latency and resource cost; thresholds recorded before rollout, no invented gain |

Use real isolated PostgreSQL for lineage, concurrency, temporal and authorization tests; real Neo4j for scoped/path/projection behavior; Temporal test facilities for timer/retry logic and staging restarts for operational recovery. Stub model responses for deterministic contract checks, then evaluate real-provider behavior separately. Code inspection or an aggregate unit-test count is not evidence that concurrency or semantic quality is correct.

Build a versioned labelled corpus stratified by stage, language, paraphrase, temporal intent, source role, identity ambiguity, contradiction, negative/no-answer cases and tenant skew. Record required answer components and supporting source IDs, including deliberately incomplete cases. Keep development tuning data separate from the held-out release set; preserve source timestamps and policy/index/model versions.

| Area | Measurements that determine acceptance |
|---|---|
| Retrieval quality | Required-evidence recall at candidate and packed-context stages; source-supported answer correctness; complete-question coverage; citation correctness; justified abstention/partial answers; historical/current confusion; unnecessary deeper calls |
| Retrieval efficiency | Stage p50/p95/p99, whole-request deadline compliance, candidates/hydration reads, model calls/tokens, provider cost and degraded-mode outcomes at stated concurrency |
| Fact formation | Supported-mutation precision, false identity merges, correction-order errors, assistant-only promotions, duplicate reinforcement and ambiguous cases deferred |
| Consolidation correctness | Dependency coverage, unsupported summary statements, stale publication attempts/rejections, duplicate side effects, deletion/revocation violations and source-grounded answer retention after compression |
| Consolidation operations | Canonical-to-view freshness, oldest dirty age, work amplification per source change, queue age/fairness, retries by class, input/output tokens and rebuild throughput |

Release requires zero observed isolation/deletion/source-promotion/duplicate-mutation violations in mandatory suites. Semantic score thresholds and acceptable latency/cost regressions must be recorded against a measured baseline before experiment selection; this document does not invent benchmark gains. A retrieval improvement must not trade away supported-answer correctness for recall. A smaller summary must retain required facts, conflicts, uncertainty and temporal distinctions on the labelled questions.

## 12. Migration and rollout

Ship additive lineage/generation fields and compatible readers first. Mark legacy overviews with unknown dependencies as unverified; rebuild bounded scopes from canonical sources instead of assuming old parent revisions prove freshness. Do not reingest summary prose to reconstruct lost evidence.

Introduce new temporal semantics with explicit API versioning/fields and SDK documentation. Shadow coverage decisions, extraction proposals and summary outputs on authorized evaluation data. Enable writes/publication only after the corresponding mutation and race tests pass. Canary extraction, summary publication and required BM25/RRF retrieval independently, with visible policy/model/index versions. Ship the lexical index and completed backfill before enabling its query path; retain the previous retrieval mode for rollback. Optional ranking experiments use separate flags.

Rollback disables the new producer/ranker while preserving accepted messages, canonical history, durable work and compatible readers. Derived views can be invalidated and rebuilt; erroneous canonical mutations require an audited correction, not merely deleting an index. Stop rollout for unauthorized disclosure, deleted-data resurrection, unsupported fact promotion, correction regression or stale publication.

This revision provides design and audit evidence only. RC-A–RC-G and RC-T01–RC-T26 remain planned; no runtime BM25 integration, live-model benchmark, production migration or deployment was performed. BM25 is now a specified implementation requirement, not an implemented feature.
