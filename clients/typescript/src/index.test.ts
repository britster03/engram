import { describe, expect, it, vi } from 'vitest';

import { EngramClient, EngramError } from './index.js';

function jsonResponse(body: unknown, status = 200, headers: Record<string, string> = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json', ...headers },
  });
}

describe('EngramClient', () => {
  it('serializes force-store ingestion and authentication', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(jsonResponse({
      event_id: 'evt-1', pair_id: 'pair-1', status: 'RECEIVED',
    }, 202));
    const client = new EngramClient({
      baseUrl: 'https://engram.test/', apiKey: 'tenant-key', fetch: fetchMock,
    });

    await client.ingest({
      user: 'hello', assistant: 'world', forceStore: true,
      userTurnIdx: 4, assistantTurnIdx: 5,
    });

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe('https://engram.test/api/v1/ingest');
    expect(new Headers(init?.headers).get('Authorization')).toBe('Bearer tenant-key');
    expect(init?.signal).toBeInstanceOf(AbortSignal);
    expect(JSON.parse(String(init?.body))).toMatchObject({
      force_store: true,
      turn_pair: { user: { turn_idx: 4 }, assistant: { turn_idx: 5 } },
    });
  });

  it('sends benchmark-safe query controls', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(jsonResponse({
      answer: 'Paris', session_id: null, retrieval_metadata: {}, trace_id: 'trace-1',
    }));
    const client = new EngramClient({
      baseUrl: 'https://engram.test', apiKey: 'key', fetch: fetchMock,
    });

    const result = await client.query('Where?', {
      includeTrace: true, retrievalMode: 'forced', minDepth: 'L2',
      maxDepth: 'L2', maxReentries: 1,
    });

    expect(result.trace_id).toBe('trace-1');
    const body = JSON.parse(String(fetchMock.mock.calls[0][1]?.body));
    expect(body).toMatchObject({
      include_trace: true, retrieval_mode: 'forced', min_depth: 'L2',
      max_depth: 'L2', max_reentries: 1,
    });
  });

  it('encodes memory path segments and preserves API error request IDs', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(jsonResponse(
      { detail: 'missing' }, 404, { 'x-request-id': 'req-9' },
    ));
    const client = new EngramClient({
      baseUrl: 'https://engram.test', apiKey: 'key', fetch: fetchMock, retries: 0,
    });

    await expect(client.getMemory('mem://user/a name?.md')).rejects.toMatchObject({
      status: 404, requestId: 'req-9',
    } satisfies Partial<EngramError>);
    expect(fetchMock.mock.calls[0][0]).toBe(
      'https://engram.test/api/v1/memories/user/a%20name%3F.md',
    );
  });

  it('posts exact event readiness IDs', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(jsonResponse({
      requested_count: 1, found_count: 1, terminal_count: 1, ready_count: 1,
      failed_count: 0, memory_ready: true, missing_ids: [], failures: [], events: [],
    }));
    const client = new EngramClient({
      baseUrl: 'https://engram.test', apiKey: 'key', fetch: fetchMock,
    });

    expect((await client.eventStatus(['evt-1'])).memory_ready).toBe(true);
    expect(JSON.parse(String(fetchMock.mock.calls[0][1]?.body))).toEqual({
      event_ids: ['evt-1'],
    });
  });
});
