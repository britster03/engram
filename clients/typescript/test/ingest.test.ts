import { describe, expect, it } from 'vitest';
import { EngramClient } from '../src/index';

describe('ingest request identities', () => {
  it('retains a generated key across retries and generates a new key per call', async () => {
    const bodies: Record<string, unknown>[] = [];
    const fetchImpl: typeof fetch = async (_input, init) => {
      bodies.push(JSON.parse(String(init?.body)));
      if (bodies.length === 1) {
        return new Response(JSON.stringify({ detail: 'retry' }), {
          status: 503,
          headers: { 'Retry-After': '0.001' },
        });
      }
      return new Response(JSON.stringify({ event_id: 'evt-1', pair_id: 'pair-1', status: 'RECEIVED' }), { status: 202 });
    };
    const client = new EngramClient({ baseUrl: 'https://example.test', apiKey: 'test', fetch: fetchImpl });
    await client.ingest({ user: 'Alice', assistant: 'Recorded.' });
    expect(bodies[0].idempotency_key).toBeTruthy();
    expect(bodies[1].idempotency_key).toBe(bodies[0].idempotency_key);
    await client.ingest({ user: 'Alice', assistant: 'Recorded.' });
    expect(bodies[2].idempotency_key).not.toBe(bodies[0].idempotency_key);
  });

  it('uses a caller-supplied key', async () => {
    const bodies: Record<string, unknown>[] = [];
    const fetchImpl: typeof fetch = async (_input, init) => {
      bodies.push(JSON.parse(String(init?.body)));
      return new Response(
        JSON.stringify({ event_id: 'evt-1', pair_id: 'pair-1', status: 'RECEIVED' }), { status: 202 },
      );
    };
    const client = new EngramClient({ baseUrl: 'https://example.test', apiKey: 'test', fetch: fetchImpl });
    await client.ingest({ user: 'Alice', assistant: 'Recorded.', idempotencyKey: 'caller-key',
      entityBindings: [{ name: 'Alice', identityKey: 'employee:42' }],
    });
    expect(bodies[0].idempotency_key).toBe('caller-key');
    expect(bodies[0].entity_bindings).toEqual([{ name: 'Alice', identity_key: 'employee:42' }]);
  });
});
