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
    "sqlite": true,
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
  "core_model": { "provider": "ollama_cloud", "model_path": "kimi-k2.7-code:cloud" },
  "frontier_llm": { "provider": "ollama_cloud", "model_path": "kimi-k2.7-code:cloud" }
}
```

## Ingest

### `POST /api/v1/ingest`

```json
{
  "session_id": "sess-abc-123",                       // optional
  "turn_pair": {
    "user": { "content": "I just started a new job at Meta.",
              "timestamp": "2026-04-12T10:28:00Z", "turn_idx": 14,
              "external_id": "D1:14", "speaker": "Alice",
              "source_conversation_id": "sample-1",
              "source_session_id": "session_1",
              "source_task": "locomo",
              "image_caption": null, "image_urls": null, "image_query": null },
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

Ingest is **idempotent** by `pair_id = sha256(session_id || user_idx || assistant_idx)`.
Duplicate submissions return the existing event_id without re-processing.

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
Source-native turn IDs and optional multimodal metadata are also persisted into
memory frontmatter and KG provenance; they are returned in opt-in retrieval traces.

### `POST /api/v1/ingest/batch`

Accepts up to 100 IngestRequest objects in a single call. Each is processed
independently; response is 202 with a list of per-item results.

## Query

### `POST /api/v1/query`

```json
{
  "session_id": "sess-abc-123",    // optional
  "query": "What project is Alice working on?",
  "session_context": "...",        // optional explicit override
  "max_depth": "L4",               // optional, default from config
  "max_reentries": 2,              // optional, default from config
  "include_trace": true            // optional, default false
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
  "trace_id": "trace-...",
  "retrieval_trace": {
    "vector_queries": ["What project is Alice working on?"],
    "commands": [],
    "hits": [
      {
        "source_uri": "mem://user/episodes/evt-....md",
        "score": 0.87,
        "retrieval_level": "L1",
        "source_turn_ids": ["D1:14", "D1:15"]
      }
    ],
    "selected_sources": [],
    "token_allocation": {},
    "reentry_requests": []
  }
}
```

Trace payloads are bounded and omit prompts, API keys, and memory bodies.

## Chat Completions

### `POST /api/v1/chat/completions`

Buffered mode is the default (`stream=false`).

```json
{
  "messages": [
    { "role": "system",  "content": "You are a helpful assistant." },
    { "role": "user",    "content": "What project is Alice working on?" }
  ],
  "session_id": "sess-abc-123",    // optional; omit to auto-create
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

### `POST /api/v1/events/status`

Accepts 1–500 unique event IDs and reports exact tenant-scoped memory readiness.
This is the supported drain signal for benchmarks; consolidation/overview
readiness remains separate.

```json
{
  "event_ids": ["evt-a", "evt-b"]
}
```

The response includes per-event durable stage, artifact counts/readiness,
legacy outbox state, `missing_ids`, `failures`,
terminal/ready counts, and aggregate `memory_ready`. A stored event is ready
when its required filesystem/KG work is indexed; a gate skip is terminal with
no required artifacts.

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
`PROPAGATE_OVERVIEW`, `REFRESH_DIRECTORY`, `ATOMIZE`, `NORMALIZE`,
`TEMPORALIZE`, `INTEGRATE`, and `UNMERGE`. Normal ingestion uses the coalesced
`REFRESH_DIRECTORY` task; granular tasks remain for explicit maintenance.

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
