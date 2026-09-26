# Engram dashboard graph fix

## What was wrong

The semantic dashboard assumed every source was a `DOCUMENT`. Conversation JSONL
ingest creates canonical `EPISODE` memories instead, so the source picker filtered
out every usable anchor and displayed “No documents match this view yet.”

The canonical conversation commit also created the episode and extracted entities
without a hierarchy edge between them. Neo4j therefore received isolated nodes (or
only entity-to-entity claims), which made the graph appear incomplete.

The configured OpenCode provider requires an `x-opencode-session` header. Canonical
Temporal activities were not binding the event session while calling the model, so
concurrent uploads could receive empty responses and remain queued.

## Changes

- `engram/storage/memory_repository.py`
  - Conversation commits now create idempotent `EPISODE → ENTITY` hierarchy edges.
  - Each edge is sent through the existing PostgreSQL outbox and Neo4j projection.
  - Hierarchy dispatch IDs are included in the commit result used by graph-status UI.
- `engram/api/routes/kg.py` and `engram/admin/routes.py`
  - The graph type filter accepts `EPISODE`, `COLLECTION`, `PROFILE`, and `PREFERENCE`.
- `engram/admin/templates/kg.html`, `kg.js`, and `graph-lite.js`
  - Conversations are shown as source anchors, with labels, colors, and filters.
  - The empty state now distinguishes “documents or conversations.”
- `engram/temporal/activities.py`
  - Canonical gate and extraction calls bind the event/session model context so
    OpenCode receives a stable session header.
- `engram/models/providers/openai_compat.py`
  - Empty or malformed model responses retry with a larger correction budget,
    accounting for providers that consume part of the budget for hidden reasoning.
- `engram/storage/neo4j_store.py`
  - Graph labels use canonical names instead of UUID-only source URIs.
  - Empty optional relationship rows are excluded before API validation.
- `engram/backfill_episode_links.py`
  - One-shot repair utility for episodes committed before this fix.

## Repair existing canonical data

Run the utility inside the application container after deploying the new image:

```bash
docker exec engram-app python -m engram.backfill_episode_links --dry-run
docker exec engram-app python -m engram.backfill_episode_links
```

To limit a repair to one upload session:

```bash
docker exec engram-app python -m engram.backfill_episode_links \
  --session-id atul-village-story-01
```

The command is idempotent and writes projection work to the normal dispatcher;
it does not delete or rewrite memories.

## Attached JSONL check

`atul_village_story_100_lines.jsonl` contains 100 conversation records with
`session_id`, `user`, and `assistant` fields. It contains story data, not
operational instructions, so the dashboard repair was applied to the
application rather than treating those rows as instructions.

At verification time, the running database contained a different 200-record
upload (`atul_singh_story_200_records.jsonl`) and no rows for session
`atul-village-story-01`; the session-scoped repair therefore reported zero
episodes. Re-ingest the attached file after deployment if it is not present,
then run the repair command above only if older episode rows need linking.

## Verification

1. Open `/admin/kg` and confirm the source picker lists conversation episodes.
2. Select a conversation and confirm its entity connections are visible.
3. Use the type filter “Conversations”; it should return HTTP 200 and episode nodes.
4. Check the ingest item status; model failures should become visible as terminal
   failures rather than silently leaving graph nodes disconnected.
