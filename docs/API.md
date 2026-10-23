# REST API

Every endpoint except `/api/v1/health` and `/metrics` requires:

```
Authorization: Bearer ${ENGRAM_API_KEY}
```

All bodies are JSON. Errors follow the FastAPI convention:

```json
{ "detail": "..." }
```

429 responses include a `Retry-After` header (integer seconds).
503 responses include a `Retry-After` header when the consolidation queue
is saturated (§7.5 backpressure).

## Health / Config

### `GET /api/v1/health`

No auth. Returns per-component readiness.

```json
{
  "status": "healthy",
  "components": {
    "control_plane": true,
    "neo4j": true,
    "redis": true,
    "filesystem": true
  }
}
```

### `GET /api/v1/config`

Returns non-sensitive config subset so clients can introspect the
cascade / model settings.

```json
{
  "retrieval": { "max_depth": "L4", "max_reentries": 2, ... },
  "core_model": { "provider": "openai_responses", "model_path": "muse-spark-1.3-contributor" },
  "frontier_llm": { "provider": "openai_responses", "model_path": "muse-spark-1.3-contributor" }
}
```

## Ingest

### `POST /api/v1/ingest`

```json
{
  "idempotency_key": "import-request-123",             // optional; reuse for retries
  "session_id": "sess-abc-123",                       // optional
  "turn_pair": {
    "user": { "content": "I just started a new job at Meta.",
              "timestamp": "2026-04-12T10:28:00Z", "turn_idx": 14 },
    "assistant": { "content": "Congratulations! What team are you on?",
                   "timestamp": "2026-04-12T10:28:05Z", "turn_idx": 15 }
  },
  "source": "client",                                  // default "client"
  "session_summary": "...",                            // optional, aids gating
  "session_context": "..."                             // optional, aids extraction
}
```

Response — 202 Accepted:

```json
{
  "event_id": "evt-0194...",
  "pair_id": "d3a1f...-sha256",
  "status": "RECEIVED"
}
```

Explicit ingestion uses an optional, tenant-scoped `idempotency_key` in the JSON
body (1–128 characters, without whitespace). Reusing the key with the same
normalized request returns the original event ID and its current status without
creating another event or dispatch. Reusing it with different content, session,
context, source or processing options returns `409 Conflict`.

Without a key, every submission receives a fresh identity, including identical
text or requests with the same session and turn numbers. Session/turn metadata
does not provide request idempotency. `pair_id` is an opaque request identity;
clients must not derive it from session/turn coordinates. Internal session replay
continues to use stable turn-pair IDs.

#### Turn groups (§5.2)

For tool-using agents, submit a `turn_group` instead of a `turn_pair`:

```json
{
  "session_id": "sess-abc-123",
  "turn_group": {
    "user": { "content": "...", "turn_idx": 14 },
    "assistant": { "content": "(final response)", "turn_idx": 17 },
    "intermediate": [
      { "content": "(tool_call)", "tool_calls": [{...}] },
      { "content": "(tool_result)", "tool_results": [{...}] }
    ]
  }
}
```

The pair fed to the gate/extract pipeline is `user` + `assistant`.
Intermediate turns are preserved in the event payload for provenance.

### `POST /api/v1/ingest/batch`

Accepts up to 100 IngestRequest objects in a single call. Each item can supply its
own `idempotency_key`, shared with the single-request endpoint. Response is 202
with a list of per-item results. Acceptance is atomic: a key/payload conflict
returns 409 and rolls back all new events and dispatches from that batch.

### `POST /api/v1/ingest/bulk`

Upload JSONL, CSV or ZIP using multipart form data. An optional form field
`idempotency_key` makes the entire upload retryable: identical bytes, filename,
format and processing options return the original job and event IDs. Changed
input with the same key returns 409. Without a key, another upload creates a new
job. Upload keys have a separate namespace from individual ingest keys.

Rows are identified by their upload and row position, independently of session
metadata or any per-row ingest key. Default sessions are unique to the upload;
explicitly supplied session IDs are preserved. Receipt, events, dispatches and
job bookkeeping commit in one transaction. Keyed uploads have a completed
`BULK_UPLOAD` receipt event; only their ingestion items are dispatched.

ZIP text/Markdown members are limited to 32,000 Unicode characters each. A
larger member is rejected with its filename, actual size and limit in
`rejected_rows`; it creates no ingestion event. Split the document before
uploading. Accepted documents retain their complete text. Other valid members
in the archive can still be accepted, so inspect the rejection counts.

### Explicit entity identity

Names and aliases alone do not authorize joining entities from different source
events, even within one session. Unqualified entities are scoped to the original
source event. Distinguishing attributes on extracted structured entities separate
same-name mentions within that source. Uncertain attribution is deferred by the
claim reviewer. No automatic first-name/full-name expansion is performed.

For deliberate identity reuse, provide `entity_bindings` on an ingest request
(also supported in JSONL rows). Each binding needs a name and exactly one of an
existing tenant-local `entity_id` or an application-defined `identity_key`:

```json
{"name":"John Smith","identity_key":"employee:42",
 "aliases":["John"],"attributes":{"employee_id":"42"}}
```

Bindings are caller assertions, not model-selected IDs. Use different keys for
different people. Aliases must be unambiguous within the request. Conflicting
distinguishing attributes fail ingestion rather than changing an existing
identity. Existing merged data requires a source-by-source audit; deploying this
policy does not automatically split historical entities.

### `POST /api/v1/events/{event_id}/reprocess`

Requires canonical strict evidence mode and Temporal. Supply:

```json
{"idempotency_key":"policy-upgrade-2026-10","reason":"Re-evaluate with the updated extraction policy"}
```

Returns 202 with a **new event ID** to poll using the event endpoint. Source text,
original event IDs, evidence and versions remain available. Gate, extraction,
typing and verification run under a newly pinned policy. Successful canonical
commit atomically replaces the previous generation's contribution and records
its supersession. Independently supported shared claims stay active. A failed
replacement leaves the previous generation active; an explicit retry can resume
that failed request if a newer request has not replaced it. A successful gate
skip or zero accepted claims retires that source's previous contribution.

The key is scoped to the original source and tenant. Repeating a key with the
same reason and policy returns the same event; changed input returns 409. Only
one replacement can be pending per source. Overlapping requests, unfinished
terminal bookkeeping and unsupported event types return 409; another tenant's
source returns 404. Code archives use their dedicated retry workflow and do not
support conversational extraction reprocessing.

Ordinary retries reuse their pinned policy and artifacts. Changing generator
settings midway causes `ProcessingPolicyChanged`, requiring explicit
reprocessing. Increment `canonical_memory.extraction_policy_version` when a
provider changes an underlying model behind an unchanged model name.

### Ingestion claim verification

In canonical `evidence_mode: enforce`, final proposed claims receive a separate
source-support review before canonical commit. Only supported direct positive
statements become active claims. Rejected and deferred candidates retain their
original source and an audit decision. Event status `COMPLETE` describes the
completed processing operation; it does not promise that any claims were accepted.

`GET /api/v1/events/{event_id}/verification` uses the same tenant authentication
as the event endpoint and returns the committed review, or the latest pending
review if no review has committed. The response includes `binding` (source,
candidate and policy hashes), `candidates`, `reviews`, `counts`, `artifact_key`
and `committed`. Each review includes the claim index, `SUPPORTED`/
`CONTRADICTED`/`UNCLEAR` verdict, assertion kind, polarity, server-enforced
`ACCEPTED`/`REJECTED`/`DEFERRED` disposition and a reason. A missing event or
unavailable review returns 404. A pending report is not proof of canonical commit.
The response also includes original `sources`, the supplied `preceding_context`,
and `source_matches`, which indicates whether the source still matches the
reviewed payload hash.

For example, a completed source may have:

```json
{
  "counts": {"accepted": 1, "rejected": 1, "deferred": 0},
  "committed": true
}
```

The full response also contains the candidate statements and their reasons.
Verification improves fidelity to source assertions; it does not establish their
external truth. Existing committed claims are not automatically reprocessed.

## Query

Query and chat share the required ordered cascade:
`L0` parametric → `L1` current session → `L2` past sessions → `L3_FACTS` →
`L3_VECTOR` → `L3_GRAPH`. The server stops when the complete question is supported.
The tentative sufficiency gate uses the existing ANSWER/NEED_MORE protocol;
no separately trained classifier is required.

`max_depth` now limits this public cascade: `L0` allows only parametric answers,
`L1` (or `SESSION`) allows the current session, `L2` includes past sessions, and
`L3` allows the full knowledge base. `L4` is retained as an alias for the full
cascade. It no longer selects the legacy internal history capability.

`retrieval_metadata` includes `retrieval_route`, `routes_attempted`, `answer_mode`,
`stop_reason`, `tool_calls`, `degraded_sources`, and `source_records`.
`source_records` identifies conversational event/session references; these are
distinct from the top-level `sources` array of verified canonical memory.
`max_reentries` remains accepted for compatibility; the cascade advances through
bounded stages instead of restarting retrieval.

Deploy migration `0005_ordered_retrieval` before the updated API. See
[the implementation plan](ORDERED_RETRIEVAL_PLAN.md).

### `POST /api/v1/query`

```json
{
  "session_id": "sess-abc-123",    // optional
  "query": "What project is Alice working on?",
  "session_context": "...",        // optional explicit override
  "max_depth": "L4",               // optional, default from config
  "max_reentries": 2               // optional, default from config
}
```

Response — 200 OK:

```json
{
  "answer": "Alice is working on Project Atlas, a distributed ML pipeline.",
  "session_id": "sess-abc-123",
  "retrieval_metadata": {
    "cascade_depth_reached": "L2",
    "levels_visited": ["L0", "L1", "L2"],
    "predicted_depth": "L2",
    "nodes_retrieved": 3,
    "total_context_tokens": 1842,
    "token_usage": {
      "input_tokens": 2100,
      "output_tokens": 96,
      "total_tokens": 2196,
      "estimated": false,
      "query_tokens": 8,
      "session_context_tokens": 120,
      "retrieval_context_tokens": 1842,
      "model_calls": [
        {
          "stage": "frontier_answer_0",
          "provider": "openai_responses",
          "model": "muse-spark-1.3-contributor",
          "input_tokens": 2100,
          "output_tokens": 96,
          "total_tokens": 2196,
          "estimated": false
        }
      ]
    },
    "reentries": 0,
    "l0_decision": "CONTINUE",
    "l0_reason": "regex:\\bmy\\s+(wife|husband|partner…",
    "latency_ms": {
      "l0_gate": 18,
      "l1_plan": 1120,
      "l1_execute": 43,
      "l2_plan": 980,
      "l2_execute": 65,
      "msc_assembly": 15,
      "frontier_answer_0": 2430,
      "total": 4671
    }
  }
}

## Chat Completions

### `POST /api/v1/chat/completions`

Buffered mode is the default (`stream=false`).

The latest implementation saves the user message before generation. Optional
`client_message_id` supplies tenant-scoped idempotency; completed retries replay
the saved result, while changed input or a non-completed existing run returns
409. Responses also include `run_id` and `input_event_id`. The final user message
must contain 1-8,000 characters.

Use `GET /api/v1/chat/runs/{run_id}` to read response/memory status, and
`POST /api/v1/chat/runs/{run_id}/cancel` to fence late answer publication.
Both use the same tenant API key and return 404 for another tenant's run.
Run status includes `client_message_id`, `created_at`, and the absolute `deadline`.
`GET /api/v1/sessions/{session_id}/chat-run` returns `{"run": <run status>}` for
the latest run, or `{"run": null}` for an existing session without a run. Optional
`?client_message_id=...` discovers a particular submission when its acknowledgement
was lost. Missing or foreign sessions return 404.

A concurrent submission receives HTTP 409 with a readable `detail`,
`code: "session_busy"`, and `run` identifiers/status/deadline. Poll the existing
run rather than submitting another message. Identical retries of running messages
return the same recovery information; terminal unsuccessful retries use
`code: "message_finalized"`. Completed retries still replay the saved response.
The five-minute deadline is absolute; retrieval progress does not extend it.
Authenticated admin wrappers expose the same status and discovery paths under
`/admin/api`. The admin chat restores durable history and polls after refresh or
disconnect, preserving an unsent draft and the Stop action. Operational notices
are excluded from model conversation context.

Cancellation preserves the accepted user message and any ingestion dispatch.
Streaming emits `accepted` with session/run/input-event IDs before answer
delivery. Answer deltas are currently sent after generation and persistence;
they are not upstream model token streaming.

Automatic user-only memory evaluation is opt-in through
`canonical_memory.automatic_user_memory_enabled=true` in canonical mode.
Assistant responses remain derived history. See the
[implementation record](IMPLEMENTATION_STATUS.md) for migration prerequisites,
source policy, feature flags and limitations.

```json
{
  "messages": [
    { "role": "system",  "content": "You are a helpful assistant." },
    { "role": "user",    "content": "What project is Alice working on?" }
  ],
  "session_id": "sess-abc-123",    // optional; omit to auto-create
  "client_message_id": "message-unique-to-this-tenant", // optional retry key
  "stream": false,                 // optional, default false
  "session_context": "...",        // optional explicit override
  "max_depth": "L4",               // optional, default from config
  "max_reentries": 2               // optional, default from config
}
```

Request rules (enforced by schema validation):

- `messages` must contain at least one object with `role: "user"`.
- The last message must have `role: "user"`.
- `max_depth`, when provided, must match the pattern `^L[0-4]$|^SESSION$`.

Response - 200 OK:

```json
{
  "answer": "Alice is working on Project Atlas, a distributed ML pipeline.",
  "session_id": "sess-abc-123",
  "retrieval_metadata": {
    "cascade_depth_reached": "L2",
    "levels_visited": ["L0", "L1", "L2"],
    "predicted_depth": "L2",
    "nodes_retrieved": 3,
    "total_context_tokens": 1842,
    "token_usage": {
      "input_tokens": 2100,
      "output_tokens": 96,
      "total_tokens": 2196,
      "estimated": false,
      "query_tokens": 8,
      "session_context_tokens": 120,
      "retrieval_context_tokens": 1842,
      "model_calls": [
        {
          "stage": "frontier_answer_0",
          "provider": "openai_responses",
          "model": "muse-spark-1.3-contributor",
          "input_tokens": 2100,
          "output_tokens": 96,
          "total_tokens": 2196,
          "estimated": false
        }
      ]
    },
    "reentries": 0,
    "l0_decision": "CONTINUE",
    "l0_reason": "regex:\\bmy\\s+(wife|husband|partner…",
    "latency_ms": {
      "l0_gate": 18,
      "l1_plan": 1120,
      "l1_execute": 43,
      "l2_plan": 980,
      "l2_execute": 65,
      "msc_assembly": 15,
      "frontier_answer_0": 2430,
      "total": 4671
    }
  },
  "finish_reason": "stop"
}
```

#### Streaming (`stream=true`)

Response content-type: `text/event-stream`.

SSE events are emitted in this order:

```text
event: metadata
data: {"retrieval_metadata": {...}}

event: delta
data: {"text": "Alice"}

event: delta
data: {"text": " is"}

event: delta
data: {"text": " working"}

event: done
data: {}
```

- `event: metadata` - single event carrying the retrieval metadata object.
- `event: delta` - one event per token (or natural sub-word chunk). The `text` field contains the raw generated fragment.
- `event: done` - signals completion. No body fields.
- `event: error` - emitted if generation fails. `data: {"error": "..."}`.

#### Buffered curl example

```bash
curl -sS https://api.engram.local/api/v1/chat/completions \
  -H "Authorization: Bearer ${ENGRAM_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {"role": "user", "content": "What is Alice working on?"}
    ]
  }'
```

#### Streaming curl example

Use `-N` so curl does not buffer the SSE stream.

```bash
curl -NsS https://api.engram.local/api/v1/chat/completions \
  -H "Authorization: Bearer ${ENGRAM_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {"role": "user", "content": "What is Alice working on?"}
    ],
    "stream": true
  }'
```

#### Python SDK example

```python
from engram_client import EngramClient

client = EngramClient(api_key="...", base_url="https://api.engram.local")

response = client.chat_completions(
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "What is Alice working on?"}
    ],
    session_id="sess-abc-123",   # optional
    max_depth="L2",
)

print(response.answer)
print(response.session_id)
```

#### Python SDK streaming example

```python
for chunk in client.chat_completions_stream(
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "What is Alice working on?"}
    ],
    session_id="sess-abc-123",   # optional
    max_depth="L2",
):
    print(chunk, end="", flush=True)
```

#### TypeScript SDK example

```typescript
import { EngramClient } from '@engram/ts-client';

const client = new EngramClient({
  apiKey: process.env.ENGRAM_API_KEY!,
  baseUrl: 'https://api.engram.local',
});

const result = await client.chatCompletions({
  messages: [
    { role: 'system', content: 'You are a helpful assistant.' },
    { role: 'user', content: 'What is Alice working on?' },
  ],
  sessionId: 'sess-abc-123',   // optional
  maxDepth: 'L2',
});

console.log(result.answer);
console.log(result.session_id);
```

#### TypeScript SDK streaming example

```typescript
for await (const chunk of client.chatCompletionsStream({
  messages: [
    { role: 'system', content: 'You are a helpful assistant.' },
    { role: 'user', content: 'What is Alice working on?' },
  ],
  sessionId: 'sess-abc-123',   // optional
  maxDepth: 'L2',
})) {
    process.stdout.write(chunk);
}
```

### Notes

- When `session_id` is omitted a new session is created automatically and returned in the response.
- After the assistant answer is complete, the turn pair is appended to the session and a background ingest event fires automatically.
- Auth and rate limits match `/api/v1/query` exactly.
- The SSE stream is true token-by-token via the frontier LLM provider, not artificially chunked.

#### Per-query token usage

Every query response includes `retrieval_metadata.token_usage`. `input_tokens`
and `output_tokens` are the summed tokens reported by the Core planner and
Frontier answer providers; `total_tokens` is their sum. `model_calls` shows the
stage-by-stage breakdown. `query_tokens`, `session_context_tokens`, and
`retrieval_context_tokens` are separate tokenizer counts that explain the
request size. When a provider does not return usage metadata, the affected call
is tokenizer-estimated and its `estimated` flag is `true`.

## Sessions

### `POST /api/v1/sessions` — create

Response — 201: `{ "session_id": "sess-...", "status": "ACTIVE" }`

### `GET /api/v1/sessions/{id}` — state

```json
{
  "session_id": "sess-abc-123",
  "status": "ACTIVE",
  "turn_count": 12,
  "created_at": "...",
  "compacted_turns": 0,
  "key_facts": ["user moved to NYC"]
}
```

### `DELETE /api/v1/sessions/{id}` — end

Triggers `COMMITTING → COMMITTED`. Drains remaining turns into the ingest
pipeline, writes a SESSION_SUMMARY node, deletes the Redis key.

### `POST /api/v1/sessions/{id}/message`

Combined ingest + session update. Body: `{ "user": "...", "assistant": "..." }`.

Response — 202: `{ "event_id": "...", "pair_id": "...", "needs_compaction": false }`.

If `needs_compaction=true`, a background compaction task is already
scheduled.

### `POST /api/v1/sessions/{id}/compact` — force compaction

Response: `{ "session_id": "...", "status": "WINDOWED", "compacted_turns": 8 }`.

## Memories

### `GET /api/v1/memories?prefix=mem://user/entities/&limit=50&cursor=...`

Cursor-paginated list of ACTIVE nodes under a URI prefix.

### `GET /api/v1/memories/{source_uri:path}`

Returns the full memory file parsed:

```json
{
  "source_uri": "mem://user/entities/alice/alice.md",
  "frontmatter": { ... },
  "body": "...",
  "edges": [
    { "relation": "works_at", "object_uri": "mem://user/entities/meta/meta.md" }
  ]
}
```

### `POST /api/v1/memories/{source_uri:path}/retire`

Soft-delete: marks the node HISTORICAL on disk and in the KG. No hard-delete
endpoint is exposed.

### `POST /api/v1/memories/{source_uri:path}/unmerge`

Splits a merged ENTITY node back into per-source contributing splits. Each
split becomes a new ACTIVE ENTITY node; the original is HISTORICAL with
`superseded_by` pointing at the list of splits.

### `GET /api/v1/memories/{source_uri:path}/history`

Follows SUPERSEDES chains — returns HISTORICAL nodes on purpose.

## Events

### `POST /api/v1/events/{event_id}/retry`

Manually retry a FAILED ingest event. Resets status → RECEIVED; the
reconciliation worker (or the triggered BackgroundTask) picks it up.

## Consolidation

### `GET /api/v1/consolidation/status`

```json
{
  "queue_depth": 42,
  "by_task_type": {
    "CONSOLIDATE_OVERVIEW": 18,
    "REGENERATE_MANIFEST": 12,
    "PROPAGATE_OVERVIEW": 12
  },
  "by_status": {
    "PENDING": 42,
    "PROCESSING": 4,
    "COMPLETE": 9021,
    "FAILED": 3
  }
}
```

### `POST /api/v1/consolidation/trigger`

```json
{
  "node_id": "mem://user/entities/alice/",
  "task_type": "CONSOLIDATE_OVERVIEW",
  "priority": 5,
  "subtree": false
}
```

Allowed task types: `CONSOLIDATE_OVERVIEW`, `REGENERATE_MANIFEST`,
`PROPAGATE_OVERVIEW`, `ATOMIZE`, `NORMALIZE`, `TEMPORALIZE`, `INTEGRATE`.

## Observability

### `GET /metrics`

Prometheus scrape target. No auth. Produces standard text-format exposition
with these series (subset of §13.2):

```
engram_query_latency_seconds{phase="..."}
engram_ingest_pipeline_stage_seconds{stage="..."}
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
