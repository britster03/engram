# engram-client (TypeScript)

Official TypeScript client for the Engram memory management API. Uses
`fetch` natively — works in Node 18+, Deno, Bun, browsers, and
Cloudflare Workers.

```bash
npm install engram-client
```

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

// Streaming
for await (const chunk of client.queryStream('...')) {
  process.stdout.write(chunk);
}
```
