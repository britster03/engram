# Production product audit

**Implementation follow-up:** this document preserves the audit baseline. See [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) for fixes, tested behavior and open release gates. Current release scope is tenant/API-key authorization; private users and shared projects are deferred.

Date: 2026-09-29. Scope: repository inspection, targeted tests, and isolated browser checks. This is an implementation assessment, not a production certification or penetration test.

Read with [the delivery plan](ORDERED_RETRIEVAL_PLAN.md). The user's production-product requirements and established conversation decisions define the target; executable code defines present behavior. This audit supersedes conflicting present-tense claims in older design documents. No application behavior or deployment was changed during this review.

The subsequent [retrieval/consolidation review](RETRIEVAL_CONSOLIDATION_DESIGN.md#2-additional-repository-findings) adds RC01–RC08 to the findings below, including a reproduced canonical overview prompt failure and source-revision/publication risks. The delivery plan incorporates their implementation priorities.

## 1. Product assessment

Engram has a substantial PostgreSQL-canonical memory foundation, controlled retrieval, durable dispatch, and Temporal workflows. The recent retrieval implementation follows the agreed ordered cascade. The remaining work is not principally adding another search engine: the product must reliably accept messages, form trustworthy memories, recover conversations, enforce ownership, and explain state accurately.

The shipped browser surface is an **administrator application**, built with Jinja, Alpine, plain JavaScript, and Tailwind. It is not an authenticated personal-user product. Preserve useful operator functions, but do not reuse its shared admin identity as the future private-user identity model.

Three immediate cross-layer problems illustrate the gap:

1. Normal chat persists completed exchanges as `QUERY` events without ingestion dispatch; the browser nevertheless labels a created session **Memory ready**.
2. The backend buffers the final answer; the browser's SSE parser can additionally discard an answer when event lines cross network chunks.
3. Tenant filters and some RLS policies exist, but private users and explicitly shared projects within a tenant have no complete authorization model.

## 2. Evidence and confidence

The working tree already contains extensive local changes. Findings refer to the inspected working tree, not an assumed clean release. Existing changes were preserved.

| Evidence class | Meaning |
|---|---|
| Inspected | A specific implementation path or configuration was read |
| Reproduced | Behavior exercised with isolated fixtures or a focused test |
| Risk | A failure follows from a code path/concurrency boundary, but was not reproduced against a deployed stack |
| Unvalidated | A desired property has no proof from this review |

Validation performed on 2026-09-29:

- `pytest -q tests/unit/test_ordered_retrieval.py tests/unit/test_templates.py tests/unit/test_session_cache.py tests/unit/test_temporal_workflows.py tests/unit/test_temporal_preflight.py`: **30 passed**.
- Chromium rendered the real chat Jinja template and checked-in assets at **1440×900** and **390×844**. API responses were isolated fixtures; no real user data, database, or model was used. There were no page JavaScript errors in those initial states and no document-level horizontal overflow at these two widths.
- Both renders displayed **Memory ready** based solely on a fixture session ID; neither contained a multiline composer or a Stop button. The mobile composer was below the initial viewport and the navigation overflow required discovery.
- An isolated browser probe split `event: delta\n` and its `data:` line across separate stream chunks. The expected answer text was discarded; the assistant content remained empty after `done`.
- Additional isolated renders covered login, all five dashboard tabs and the graph screen at both viewport sizes, with API calls returning a deliberate 503. The memory tab still displayed “No memories found.” The graph screen did display an error. Dashboard initialization raised a null `bulkJob.project_uri` binding exception at both sizes (F26). These are render/failure checks, not successful full-stack journeys.
- An isolated authentication probe made tenant-registry resolution reject a fixture key while the configured legacy key matched. `_lookup_tenant` still returned an ACTIVE tenant, confirming F02 without contacting a real database or using real credentials.
- The preceding code review reproduced empty-memory terminal responses for statements such as “My name is Ravi,” “I live in Pune,” and a relocation correction. It also reproduced the current-fact classification of “What did I say about where I live?” and the refusal to try L0 for “Who is Einstein?” These were deterministic routing probes with stub providers, not live-model quality measurements.

The earlier [ordered retrieval validation record](ORDERED_RETRIEVAL_PLAN.md#historical-validation-record--2026-09-28) records 118 selected checks with isolated PostgreSQL and Neo4j. Those infrastructure checks were **not rerun** for this documentation review. Full-stack browser behavior, screen readers, all viewport sizes, production database grants, real provider streaming/cancellation, load, failover, backup restore, and live-model quality remain unvalidated here.

## 3. Component inventory

`EXISTS` means implemented, not production-certified. `PARTIALLY EXISTS` identifies a materially incomplete end-user capability. `DOES NOT EXIST` refers to the inspected product paths. `PROPOSED / PLANNED` explicitly identifies future work.

| Component / capability | Status | Repository evidence and boundary |
|---|---|---|
| Admin routing, layouts, navigation | EXISTS | [admin/routes.py](../engram/admin/routes.py), [base.html](../engram/admin/templates/base.html), dashboard partials, chat and graph pages |
| Browser state and API access | EXISTS | Alpine objects in [chat.html](../engram/admin/templates/chat.html) and [dashboard.js](../engram/admin/static/js/dashboard.js); inline `fetch`, localStorage, duplicated helpers |
| Design primitives | PARTIALLY EXISTS | Self-hosted Inter, Lucide, Tailwind and [dashboard.css](../engram/admin/static/css/dashboard.css); page-specific styles and inconsistent theme handling |
| Daily conversation workspace | PARTIALLY EXISTS | New session, send, progress, history viewer exist; chat restoration, naming, switching, stop, retry and provenance UX are incomplete |
| Uploads and code exploration | EXISTS | [bulk_ingest.py](../engram/api/routes/bulk_ingest.py), admin ingest forms and graph/code-map views support JSONL/CSV/ZIP jobs with progress/retry and size limits; not equivalent to chat attachments |
| Attachments in the chat composer | DOES NOT EXIST | Current chat request schema and composer have no attachment-reference contract |
| Personal-user / shared-project access | DOES NOT EXIST | [tenancy.py](../engram/tenancy.py) defines tenants and keys; admin wrappers bind the default tenant; no complete principal/project grants |
| Browser authentication | PARTIALLY EXISTS | Signed admin cookie plus bearer key; shared admin identity, localStorage secret storage, cookie/logout hardening needed |
| Public API validation and SDKs | EXISTS | [schemas.py](../engram/api/schemas.py), Python/TypeScript clients; message acceptance/idempotency and generation lifecycle contracts are missing |
| True interactive token streaming | PARTIALLY EXISTS | Providers include streaming capabilities, but [chat.py](../engram/api/routes/chat.py) calls buffered retrieval and later chunks its answer |
| End-to-end cancellation / resume | DOES NOT EXIST | Browser owns an AbortController; no complete server generation identity, cancellation and reconnect contract |
| Session persistence | EXISTS | [manager.py](../engram/session/manager.py), `session_states`, `events`; pair append and source event commit together |
| Durable user-message acceptance before generation | DOES NOT EXIST | Latest user text is persisted only after retrieval/answer generation |
| Redis cache and fallback | EXISTS | [redis_cache.py](../engram/storage/redis_cache.py) uses tenant keys and process-local fallback; manager recovers PostgreSQL snapshots on misses |
| PostgreSQL canonical model | EXISTS | [canonical_schema.py](../engram/storage/canonical_schema.py), [memory_repository.py](../engram/storage/memory_repository.py): nodes, versions, claims, evidence, mutations, aliases, hierarchy |
| PostgreSQL operational isolation | PARTIALLY EXISTS | Tenant predicates and canonical-table RLS exist; runtime/migrator ownership, control-plane RLS, principal ACLs and actual-role validation remain |
| Ordered retrieval | EXISTS | [cascade.py](../engram/retrieval/cascade.py): L0 → L1 → L2 → facts → vector → graph, with early termination |
| Intent and sufficiency | PARTIALLY EXISTS | [planner.py](../engram/retrieval/planner.py) heuristics plus optional enrichment; answer model supplies ANSWER/NEED_MORE; statement/historical/public-entity edge cases |
| Controlled tools | EXISTS | [tools.py](../engram/retrieval/tools.py) exposes server-dispatched semantic wrappers; model refines arguments, not arbitrary SQL/Cypher; not an autonomous native tool-calling loop |
| Past-session retrieval | PARTIALLY EXISTS | PostgreSQL FTS plus optional summary hints; original records returned; no demonstrated paraphrase/multilingual recall target |
| Vector discovery | EXISTS | [neo4j_store.py](../engram/storage/neo4j_store.py) vector candidates plus canonical hydration; current global ANN selection precedes tenant filtering |
| Hybrid fusion / optional reranking | PROPOSED / PLANNED | No complete measured BM25/vector fusion/reranker path in the ordered cascade; PostgreSQL FTS is not automatically BM25 |
| Bounded graph discovery | EXISTS | [t_canonical_paths.cypher](../templates/cypher/t_canonical_paths.cypher), template limits and per-edge PostgreSQL verification |
| User-only automatic chat ingestion | DOES NOT EXIST | Chat marks pairs `ingestible=False`; extractor otherwise prioritizes assistant output |
| Explicit canonical ingestion | EXISTS | `/ingest`, session `/message`, [Temporal activities](../engram/temporal/activities.py), `commit_conversational_event` |
| Typed predicates and claim history | EXISTS | [predicate_registry.py](../engram/predicate_registry.py), canonical versions/claims; delayed correction ordering still requires hardening |
| Temporal / outbox / projection | EXISTS | [dispatcher.py](../engram/temporal/dispatcher.py), [workflows.py](../engram/temporal/workflows.py), fenced dispatch leases, persisted artifacts, revisioned projection |
| Consolidation | PARTIALLY EXISTS | Canonical dispatch supports overview consolidation and canonical unmerge; broader proposed episode/topic/profile lineage is not a finished feature |
| Session compaction | PARTIALLY EXISTS | Explicit compaction exists; normal chat's helper is uncalled; concurrent snapshot lifecycle needs protection |
| Memory review / correction / forgetting UX | PARTIALLY EXISTS | Admin memory listing, detail/history and unmerge exist; no complete user-facing correction/erasure experience |
| Retention / erasure across all stores | DOES NOT EXIST | No complete accepted 90-day raw-retention and durable-evidence deletion workflow in inspected paths |
| Metrics, tracing, audit and probes | EXISTS | [metrics.py](../engram/metrics.py), [tracing.py](../engram/tracing.py), request IDs, audit log, liveness/readiness; telemetry consistency needs repair |
| Docker / Helm / Kubernetes | EXISTS | Compose, migration jobs, Temporal workers, ingress, HPA/PDB, network policies; manifests alone do not prove HA |
| Test layers | EXISTS | Unit, repository/integration, real-Cypher and browser E2E files; automated release gating and complete user journeys remain |
| Repository CI workflow | DOES NOT EXIST | No `.github` directory in this working tree; external CI may exist but was not available to inspect |

## 4. Prioritized findings

### Critical — trust and isolation release blockers

**F01 — Tenant identity is insufficient for the agreed private-user product.** Tenant API keys authorize a tenant, not a person within it. The admin API intentionally binds a default tenant. Reusing this for end users would expose the wrong scope. Introduce server-owned principals and explicit project membership; require the same policy for sessions, source records, facts, tools, caches, projections, and provenance. Preserve admin routes as privileged operations. Evidence: `tenancy.py`, `admin/routes.py:_bind_default_tenant`, API schemas. Inspected; not a demonstrated deployed breach.

**F02 — Legacy authentication fallback defeats registry rejection for the configured key.** `api/auth.py:_lookup_tenant` accepts `cfg.api.api_key` after registry resolution returns no tenant or errors, constructing an ACTIVE default tenant. Registry suspension/revocation is therefore not authoritative for that matching key. Make the registry authoritative once bootstrapped; distinguish service failure from invalid credentials and test both. Evidence: `api/auth.py:_lookup_tenant`, `tenancy.py:resolve_key/ensure_default`. Reproduced with isolated fixture credentials and a rejecting registry stub.

**F03 — Deployment roles do not establish the claimed RLS boundary.** `deploy/postgres/init/01-databases.sh` makes `engram_app` the database owner, grants it schema creation, and shares one password across application, dispatcher, migrator and Temporal roles. Alembic uses the same `ENGRAM_DATABASE_URL`; canonical migration enables RLS but does not force it, and the new session table has application filtering rather than its own RLS policy. Table owners normally bypass RLS ([PostgreSQL documentation](https://www.postgresql.org/docs/16/ddl-rowsecurity.html)). Verify actual table owners, split credentials/roles, prevent runtime DDL, apply scoped policies to sensitive tables, and test using the exact runtime role. Configuration risk; live grants were not inspected.

**F04 — Automatic ingestion must not promote assistant assertions.** `prompts/extract.j2` defaults to assistant text and accepts user-or-assistant asserted facts. Switching chat to `ingestible=True` would violate the agreed user-grounded memory policy. Require validated user source spans, explicit confirmation handling and scoped identity before enabling automatic memory. Current chat avoids that promotion by excluding the entire pair, leaving a functional gap. Inspected.

### High — loss, incorrect memory, broken interaction

**F05 — Accepted input has no independent durable lifecycle.** `chat.py:chat_completions` runs retrieval before `append_turn_pair`. A request/process failure before append loses the submitted turn from server history. Add durable message acceptance, stable client idempotency and separate response runs. Pair-ID deduplication does not deduplicate repeated HTTP requests that allocate new turn indices. Inspected; crash window is a risk.

**F06 — Browser drops fragmented SSE events.** `chat.html:send` resets `eventName` and `data` on each reader chunk. A chunk ending after an `event:` line loses that event association. Reproduced with a two-chunk response. A persistent event parser must support chunk splits, UTF-8 splits, CRLF, multiple data lines, clean completion and premature EOF according to the [event-stream specification](https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream).

**F07 — Concurrent browser sends and cancellation are incomplete.** `send()` has no loading/creation guard; submission stays enabled; one `streamAbort` and one `loading` value represent potentially multiple sends. There is no Stop control. The server launches a daemon thread and `join(timeout=5)` does not cancel model/tool work. Add per-run state, a server cancellation contract, provider deadlines, terminal status reconciliation and bounded concurrency. Inspected; multi-request race not load-tested.

**F08 — Restoring a session does not restore chat content.** `restoreOrCreateSession()` restores only its ID; `init()` adds a greeting rather than fetching history. The admin history endpoint already exists. Closed sessions can appear in list rollups as ARCHIVED regardless of durable lifecycle, then fail on a new message. Read canonical session status and paginated history; keep closed sessions readable and explicitly start a new session for further messages. Inspected.

**F09 — Conversation acts are not modeled.** New personal statements and corrections go through the question-only retrieval path and can receive an insufficient-evidence answer. Historical wording and public names also trigger wrong eligibility decisions. Reproduced in isolated routing checks. Recognize message acts without requiring an expensive classifier on every turn; preserve retrieval ordering for the question portion of mixed messages.

**F10 — New-session recall is incomplete until user ingestion is connected.** L2 correctly avoids treating old text as authoritative current facts, but chat never promotes eligible user assertions. Normal session close also excludes all these turns from canonical summary generation. Connect durable user-message ingestion and distinguish “last stated” from verified current support. Evidence: `cascade.py:needs_current`, `manager.py:commit_session`. Inspected.

**F11 — Correction order and concurrent snapshot changes are not proven safe.** Canonical single-valued claim supersession does not explicitly compare assertion order before replacement at that decision point. Independent workflows can finish out of order. Append locks do not protect full-snapshot compaction/close writes, and post-commit cache publishing has no revision comparison. Require temporal-order rules, revision checks and tests with deliberately reversed completion. Evidence: `memory_repository.py:commit_conversational_event/supersede_claim`, `manager.py:append_turn_pair/_persist`. Risk, not a claimed production incident.

**F12 — “Delete session” is actually close/commit, not erasure.** `DELETE /sessions/{id}` commits and deletes the cache entry; PostgreSQL history and memory remain. No complete forgetting pipeline was found. Preserve existing API compatibility explicitly and introduce separate close, delete-conversation, and forget-source/memory semantics. Tombstones must prevent in-flight ingestion and projection from restoring deleted information. Inspected.

**F13 — Admin browser credentials and logout need hardening.** Login stores the admin key in localStorage regardless of the “Stay signed in” checkbox; the signed cookie is HttpOnly but is not set Secure by this route. Chat logout tries to remove the HttpOnly cookie using JavaScript instead of calling the existing server logout endpoint. The cookie signing key can be process-random if configuration omits it. Use a stable secret, TLS-appropriate Secure cookies, server logout, protected mutation requests, and no persistent browser bearer secret. Evidence: `templates/login.html`, `templates/chat.html:logout`, `admin/auth.py`, `admin/routes.py:admin_login_api`. Inspected.

**F14 — Operational metrics and alerts disagree.** The canonical commit histogram is `engram_canonical_commit_seconds`; its alert queries `engram_canonical_commit_latency_seconds_bucket`. The idle-traffic rule is named NotReady even though lack of traffic does not establish unavailability. Old depth/reentry alarms also need review against the new cascade. Validate rule expressions against exported metrics and inject faults to prove paging. Evidence: `metrics.py`, `deploy/prometheus/alerts.yml`. Inspected.

**F15 — Runtime connections bypass the declared pool limit.** `PostgresStore` creates a bounded pool but `get_conn()` and `transaction()` open thread-local direct connections. The pool's max size therefore does not bound request/workflow connection growth. Use a shared bounded connection/unit-of-work mechanism while preserving nested transaction and tenant-context semantics. Test pool reuse, session setting cleanup, timeouts and saturation. Evidence: `storage/postgres.py`. Inspected.

### Medium — retrieval quality, product clarity and scale

**F16 — Memory listing does not fulfill its search/display promise.** The input says “URI or content” but sends `prefix`; canonical list items contain no abstract/body/timestamp. The UI substitutes the current time for absent creation time and can show empty content. Define honest list/search contracts, preserve server timestamps, expose errors and paginate. Evidence: `_memories.html`, `dashboard.js:searchMemories`, `routes/memories.py:MemoryListItem/list_memories`. Inspected.

**F17 — Search recall has bounded but real gaps.** L2 uses `simple` PostgreSQL FTS and an OR token query. Oversized records are skipped whole. Neo4j global top-K ANN selection happens before tenant filtering, which can starve small tenants of candidates even without exposing another tenant's records. Use a labelled recall evaluation, bounded excerpting, authorized candidate generation and hybrid retrieval only where measured improvements justify it. Evidence: `retrieval/tools.py`, `control_plane.py:search_session_events`, `neo4j_store.py:vector_search`. Inspected; recall loss not quantified.

**F18 — Graph verification and context packaging need a shared contract.** Tools verify complete paths, then flatten edges; later token packing must preserve the required relationship evidence bundle. Hydration currently occurs per path; overview consolidation also loads children individually. Batch where profiling identifies N+1 cost, and test insufficient budgets without partially supported relationship answers. Evidence: `tools.py:graph_search`, `retrieval/context.py`, `consolidation/canonical.py:consolidate_overview`. Risk; no latency benchmark.

**F19 — User-facing status does not match backend facts.** “Memory ready” follows session existence; “Answer time” uses the maximum individual latency rather than end-to-end duration; old level labels describe obsolete semantics. Show durable acceptance, generation state and memory processing independently. Display actual source kind, not raw trace details in the default personal UI. Evidence: `chat.html:retrievalTime/retrievalScope`, status badge. Inspected/rendered.

**F20 — Chat rendering and accessibility are incomplete.** Chat sanitizes HTML but does not parse Markdown into rich content; there is no multiline input, explicit composer label or complete message live-region model. The base viewport restricts zoom. Mobile navigation and composer require intentional layout, not compressed desktop controls. Adopt WCAG 2.2 AA as the validation target and preserve safe rendering ([W3C specification](https://www.w3.org/TR/WCAG22/)). Initial render inspected; no comprehensive accessibility certification.

**F21 — Cache fallback can remain stale across replicas.** Redis failure permanently sets `_client=None` in that process. Subsequent local caching has neither automatic recovery nor revision validation against updates from other replicas. Durable PostgreSQL is still available, but recovery alone does not guarantee fresh reads. Prefer PostgreSQL reads during distributed-cache failure, bounded reconnect attempts, and revision-aware cache publication. Evidence: `redis_cache.py:_disable_redis`, session manager. Inspected risk.

**F22 — Session growth and retention are not bounded by compaction.** Snapshot JSON retains all turns; each append rewrites it. Compaction adds summaries and an index, rather than deleting raw source records from that structure. Size limits at retrieval do not bound storage growth. Move history to paginated messages, keep bounded snapshots, and implement the accepted 90-day raw policy independently of selected durable evidence. Evidence: `manager.py`, `session_search.py`. Inspected.

**F23 — Error and telemetry contracts may disclose content or confuse users.** Provider parsing errors embed raw response snippets; routes can return exception text. The browser inserts failures as assistant messages and submits its message array on subsequent requests. Use stable public error codes/request IDs, redact sensitive internals, and keep UI failures outside conversational evidence. Evidence: `openai_compat.py`, `chat.py`, `chat.html:send`. Inspected risk.

**F24 — Production artifacts are not operational proof.** Helm provides API/worker replicas and PDBs; Compose is a single-host deployment. Actual external PostgreSQL/Temporal availability, restores, rolling workflow compatibility, saturation and migration restart safety still need drills. `temporal_preflight.py` checks secrets/disk/Docker, not the full service contract. No repository CI was found. Evidence: deployment files, preflight script, test layout. Unvalidated production readiness.

**F25 — Existing plans contained stale and conflicting claims.** At audit start, the older best-system plan prioritized advanced recall before product trust, described auto-compaction as active, and listed a missing `$tenant_id` parameter now present in `memories.py`. The documentation index described filesystem authority. This documentation revision corrects the index, priority and compaction descriptions and marks the older defect lists/build order as historical. Addressed in planning documents; this does not fix the runtime findings above.

**F26 — Hidden upload UI evaluates a nullable binding during dashboard initialization.** `_ingest.html` guards visibility with `x-show="bulkJob?.project_uri"`, but its `:href` evaluates `bulkJob.project_uri` while `bulkJob` is null. Hiding an element does not guard its bindings. Reproduced when rendering the dashboard at desktop/mobile sizes. Make empty/loading bindings safe and test every dashboard tab's initial state, not only successful uploads.

## 5. What to preserve

- PostgreSQL canonical transactions, versioned claims, evidence IDs, typed predicates and stable identity.
- The existing outbox and Temporal dispatch/worker architecture; no second message broker is justified by this audit.
- Ordered retrieval with early exit, canonical hydration, controlled graph templates and explicit insufficient/conflicting outcomes.
- Existing admin ingest, graph, operator views, SDKs, history APIs and self-hosted UI assets.
- Redis as an optional acceleration layer, with PostgreSQL as the recovery authority.
- Separate projection health from canonical availability: current canonical-mode readiness already treats Neo4j and Redis as degradable capabilities.

## 6. Product journeys used to prioritize delivery

| Journey | Present gap | Required outcome |
|---|---|---|
| Share a fact and ask about it immediately | Statement response + missing auto-ingest | Acknowledge naturally; current-session answer works immediately; durable learning has independent status |
| Return tomorrow | ID-only chat restoration; current fact not promoted | Restore readable history and recall authorized, supported memory |
| Correct a preference/location | Processing order can differ from message order | Current fact reflects the correction; old fact retains history and provenance |
| Confirm an assistant's question | Extractor trust boundary unclear | Confirmation cites user text and the bounded question context |
| Ask a general public question | Overly conservative named-entity routing | Safe direct answer without unrelated private-memory calls |
| Ask a relationship question | Projection/packing/budget limitations | Every relationship step is authorized and supported, or limitation is explained |
| Lose network while receiving an answer | Fragment handling, no resume/run identity | Accepted input survives; partial/final response can be reconciled without duplicate sends |
| Stop a response | No user control or propagation | Generation stops within a bounded deadline; accepted input remains saved |
| Inspect or change a memory | Raw admin-oriented concepts | Plain-language fact, source, date, current/history status and correction action |
| Forget a conversation and its derived memories | Close is mistaken for delete | Explicit deletion scope, immediate retrieval exclusion and observable purge completion |
| Collaborate on a project | Tenant-only scope | Private by default; only explicitly shared project sources become accessible |
| Lose a supporting service | Recovery unproven across all paths | Clear degradation while healthy canonical capabilities continue safely |

The delivery order and measurable acceptance tests for these findings are specified in [ORDERED_RETRIEVAL_PLAN.md](ORDERED_RETRIEVAL_PLAN.md). Proposed work remains proposed until its implementation and validation records are added.

## 7. Retrieval and consolidation follow-up

At the user's request, the review examined evidence coverage, predicate cardinality, temporal filtering, context packing, overview prompts, source revisions, task deduplication and Temporal completion behavior more deeply. Results and code references are recorded as RC01–RC08 in [the focused design](RETRIEVAL_CONSOLIDATION_DESIGN.md#2-additional-repository-findings).

One additional defect was **reproduced** using a synthetic relationship dictionary and the real `prompts.render("overview", ...)`: `jinja2.exceptions.UndefinedError: 'dict object' has no attribute 'subject_uri'`. The canonical handler supplies different field names from the template. This happens before any model call; no database or real user information was needed. Keep it open as an implementation blocker; this documentation revision does not fix runtime code.

The remaining findings are inspected limitations or risks, not reproduced deployed failures: parent revisions do not fully cover child-source changes; publication checks and writes are separate; active-job deduplication lacks dirty-generation tracking; evidence coverage/cardinality/time semantics and bundle packing need stronger contracts. The new plan defines RC-A–RC-G work packages and initially added RC-T01–RC-T18 checks. The subsequent user-requested BM25 plan revision makes BM25/vector RRF required RC-G work and extends the matrix through RC-T26. Repository inspection still finds PostgreSQL FTS and vector retrieval, plus an unused full-text index declaration; BM25 runtime integration remains planned. Previously reported suites were not rerun for these documentation follow-ups, and no new concurrency/load/live-model validation is claimed.
