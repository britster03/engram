# engram-client (TypeScript)

`ingest` generates one idempotency key per method call and retains it across
automatic HTTP retries. Pass `idempotencyKey: 'your-request-id'` to retry the same
submission across calls. Changed input with that key returns 409; separate calls
without an explicit key create separate submissions.

Chat methods accept optional `clientMessageId` for tenant-scoped idempotency.
Use the same ID and content for a retry. Buffered responses include `run_id`
and `input_event_id`; a non-completed existing run returns 409. The server
exposes `GET /api/v1/chat/runs/{run_id}` and
`POST /api/v1/chat/runs/{run_id}/cancel` under the same tenant API key.

Official TypeScript client for the Engram memory management API. Uses
`fetch` natively — works in Node 18+, Deno, Bun, browsers, and
Cloudflare Workers.

```bash
npm install engram-client
```

Names alone do not join entities across source events. Supply
`entityBindings: [{ name: 'John Smith', identityKey: 'employee:42' }]` to
`ingest()` for deliberate identity reuse. An existing tenant-local `entityId`
can be supplied instead of `identityKey`.

```ts
import { EngramClient } from 'engram-client';

const client = new EngramClient({
  baseUrl: 'https://engram.example.com',
  apiKey: process.env.ENGRAM_API_KEY!,
});

await client.ingest({
  sessionId: 'sess-1',
  user: 'I just accepted a job at Meta.',
  assistant: 'Congratulations!',
});

const r = await client.query('Where does the user work?');
console.log(r.answer);
console.log(r.retrieval_metadata.token_usage.total_tokens);

// Streaming
for await (const chunk of client.queryStream('...')) {
  process.stdout.write(chunk);
}
```
