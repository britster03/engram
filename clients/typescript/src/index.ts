/**
 * Official TypeScript client for the Engram memory management API.
 *
 * Uses the native `fetch` API (available in Node 18+, all modern browsers,
 * Deno, Bun, Cloudflare Workers). Retries 429 / 5xx with exponential
 * backoff.
 */

export interface RetrievalMetadata {
  retrieval_mode: string;
  min_depth: string | null;
  max_depth: string | null;
  cascade_depth_reached: string;
  levels_visited: string[];
  predicted_depth: string | null;
  nodes_retrieved: number;
  total_context_tokens: number;
  reentries: number;
  latency_ms: Record<string, number>;
  l0_decision: string | null;
  l0_reason: string | null;
}

export interface QueryResponse {
  answer: string;
  session_id: string | null;
  retrieval_metadata: RetrievalMetadata;
  trace_id?: string | null;
  retrieval_trace?: Record<string, unknown> | null;
}

export interface IngestResponse {
  event_id: string;
  pair_id: string;
  status: string;
}

export interface EventReadiness {
  event_id: string;
  pair_id: string;
  status: string;
  terminal: boolean;
  memory_ready: boolean;
  completed_stage?: string | null;
  artifact_count: number;
  filesystem_ready_count: number;
  kg_ready_count: number;
  artifact_error_count: number;
  error?: string | null;
}

export interface EventStatusResponse {
  requested_count: number;
  found_count: number;
  terminal_count: number;
  ready_count: number;
  failed_count: number;
  memory_ready: boolean;
  missing_ids: string[];
  failures: EventReadiness[];
  events: EventReadiness[];
}

export interface SessionState {
  session_id: string;
  status: string;
  turn_count: number;
  created_at: string;
  compacted_turns: number;
  key_facts: string[];
}

export interface TenantPayload {
  tenant_id: string;
  display_name: string;
  status: string;
  created_at: string;
  quotas: Record<string, number>;
  api_key_count: number;
}

export interface ClientOptions {
  baseUrl: string;
  apiKey: string;
  timeoutMs?: number;
  retries?: number;
  fetch?: typeof fetch;
  userAgent?: string;
}

export class EngramError extends Error {
  constructor(
    public status: number,
    public detail: unknown,
    public requestId?: string,
  ) {
    super(`${status}: ${typeof detail === 'string' ? detail : JSON.stringify(detail)}`);
  }
}

export class EngramClient {
  private readonly baseUrl: string;
  private readonly apiKey: string;
  private readonly timeoutMs: number;
  private readonly retries: number;
  private readonly fetchImpl: typeof fetch;
  private readonly userAgent: string;

  constructor(opts: ClientOptions) {
    if (!/^https?:\/\//.test(opts.baseUrl)) {
      throw new Error('baseUrl must include scheme (http:// or https://)');
    }
    this.baseUrl = opts.baseUrl.replace(/\/+$/, '');
    this.apiKey = opts.apiKey;
    this.timeoutMs = opts.timeoutMs ?? 60_000;
    this.retries = opts.retries ?? 3;
    this.fetchImpl = opts.fetch ?? globalThis.fetch;
    this.userAgent = opts.userAgent ?? 'engram-typescript/0.1.0';
  }

  // ---- Core ---------------------------------------------------------

  async ingest(params: {
    user: string;
    assistant: string;
    sessionId?: string | null;
    userTurnIdx?: number;
    assistantTurnIdx?: number;
    source?: string;
    forceStore?: boolean;
  }): Promise<IngestResponse> {
    const body = {
      session_id: params.sessionId ?? null,
      source: params.source ?? 'client',
      force_store: params.forceStore ?? false,
      turn_pair: {
        user: { content: params.user, turn_idx: params.userTurnIdx ?? 0 },
        assistant: {
          content: params.assistant,
          turn_idx: params.assistantTurnIdx ?? (params.userTurnIdx ?? 0) + 1,
        },
      },
    };
    return this.post<IngestResponse>('/api/v1/ingest', body);
  }

  async query(query: string, opts: {
    sessionId?: string | null;
    sessionContext?: string | null;
    maxDepth?: string;
    minDepth?: 'L1' | 'L2' | 'L3' | 'L4';
    maxReentries?: number;
    includeTrace?: boolean;
    forceRetrieval?: boolean;
    retrievalMode?: 'adaptive' | 'forced' | 'no_memory' | 'vector_only';
  } = {}): Promise<QueryResponse> {
    const body = {
      session_id: opts.sessionId ?? null,
      query,
      session_context: opts.sessionContext ?? null,
      max_depth: opts.maxDepth ?? null,
      min_depth: opts.minDepth ?? null,
      max_reentries: opts.maxReentries ?? null,
      include_trace: opts.includeTrace ?? false,
      force_retrieval: opts.forceRetrieval ?? false,
      retrieval_mode: opts.retrievalMode ?? 'adaptive',
    };
    return this.post<QueryResponse>('/api/v1/query', body);
  }

  /**
   * Stream a query response via Server-Sent Events.
   * Consume with `for await (const chunk of client.queryStream(q)) { ... }`.
   */
  async *queryStream(query: string, opts: {
    sessionId?: string | null;
    sessionContext?: string | null;
  } = {}): AsyncIterable<string> {
    const body = {
      session_id: opts.sessionId ?? null,
      query,
      session_context: opts.sessionContext ?? null,
      stream: true,
    };
    const r = await this.request('/api/v1/query', {
      method: 'POST',
      headers: this.authHeaders('application/json'),
      body: JSON.stringify(body),
    });
    if (!r.ok) throw await this.errFromResponse(r);
    if (!r.body) return;
    const reader = r.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    let currentEvent = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let idx: number;
      while ((idx = buf.indexOf('\n\n')) !== -1) {
        const block = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        for (const line of block.split('\n')) {
          if (line.startsWith('event:')) currentEvent = line.slice(6).trim();
          else if (line.startsWith('data:')) {
            const data = line.slice(5).trim();
            if (currentEvent === 'delta') {
              try {
                const payload = JSON.parse(data) as { text?: string };
                if (payload.text) yield payload.text;
              } catch { /* ignore */ }
            } else if (currentEvent === 'error') {
              throw new EngramError(500, data);
            } else if (currentEvent === 'done') {
              return;
            }
          }
        }
      }
    }
  }

  // ---- Chat completions ---------------------------------------------

  async chatCompletions(params: {
    messages: Array<{ role: string; content: string }>;
    sessionId?: string | null;
    sessionContext?: string | null;
    maxDepth?: string;
    maxReentries?: number;
  }): Promise<{
    answer: string;
    session_id: string;
    retrieval_metadata: RetrievalMetadata;
    finish_reason: string;
  }> {
    const body = {
      session_id: params.sessionId ?? null,
      messages: params.messages,
      session_context: params.sessionContext ?? null,
      max_depth: params.maxDepth ?? null,
      max_reentries: params.maxReentries ?? null,
    };
    return this.post<{
      answer: string;
      session_id: string;
      retrieval_metadata: RetrievalMetadata;
      finish_reason: string;
    }>('/api/v1/chat/completions', body);
  }

  /**
   * Stream a chat completions response via Server-Sent Events.
   * Consume with `for await (const chunk of client.chatCompletionsStream({messages})) { ... }`.
   */
  async *chatCompletionsStream(params: {
    messages: Array<{ role: string; content: string }>;
    sessionId?: string | null;
    sessionContext?: string | null;
    maxDepth?: string;
    maxReentries?: number;
  }): AsyncIterable<string> {
    const body = {
      session_id: params.sessionId ?? null,
      messages: params.messages,
      session_context: params.sessionContext ?? null,
      max_depth: params.maxDepth ?? null,
      max_reentries: params.maxReentries ?? null,
      stream: true,
    };
    const r = await this.request('/api/v1/chat/completions', {
      method: 'POST',
      headers: this.authHeaders('application/json'),
      body: JSON.stringify(body),
    });
    if (!r.ok) throw await this.errFromResponse(r);
    if (!r.body) return;
    const reader = r.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    let currentEvent = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let idx: number;
      while ((idx = buf.indexOf('\n\n')) !== -1) {
        const block = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        for (const line of block.split('\n')) {
          if (line.startsWith('event:')) currentEvent = line.slice(6).trim();
          else if (line.startsWith('data:')) {
            const data = line.slice(5).trim();
            if (currentEvent === 'delta') {
              try {
                const payload = JSON.parse(data) as { text?: string };
                if (payload.text) yield payload.text;
              } catch { /* ignore */ }
            } else if (currentEvent === 'error') {
              throw new EngramError(500, data);
            } else if (currentEvent === 'done') {
              return;
            }
          }
        }
      }
    }
  }

  // ---- Session ------------------------------------------------------

  async createSession(): Promise<string> {
    const r = await this.post<{ session_id: string }>('/api/v1/sessions', {});
    return r.session_id;
  }

  async getSession(id: string): Promise<SessionState> {
    return this.get<SessionState>(`/api/v1/sessions/${encodeURIComponent(id)}`);
  }

  async endSession(id: string): Promise<void> {
    await this.delete(`/api/v1/sessions/${encodeURIComponent(id)}`);
  }

  async sendMessage(sessionId: string, params: {
    user: string; assistant: string;
  }): Promise<IngestResponse> {
    return this.post<IngestResponse>(
      `/api/v1/sessions/${encodeURIComponent(sessionId)}/message`, params,
    );
  }

  // ---- Memories -----------------------------------------------------

  async getMemory(sourceUri: string): Promise<unknown> {
    return this.get(`/api/v1/memories/${memoryPath(sourceUri)}`);
  }

  async listMemories(opts: {
    prefix?: string; limit?: number; cursor?: string;
  } = {}): Promise<unknown> {
    const params = new URLSearchParams({
      prefix: opts.prefix ?? 'mem://',
      limit: String(opts.limit ?? 50),
      ...(opts.cursor ? { cursor: opts.cursor } : {}),
    });
    return this.get(`/api/v1/memories?${params}`);
  }

  async retire(sourceUri: string): Promise<unknown> {
    return this.post(`/api/v1/memories/${memoryPath(sourceUri)}/retire`, {});
  }

  async unmerge(sourceUri: string): Promise<unknown> {
    return this.post(`/api/v1/memories/${memoryPath(sourceUri)}/unmerge`, {});
  }

  async eventStatus(eventIds: string[]): Promise<EventStatusResponse> {
    return this.post<EventStatusResponse>('/api/v1/events/status', { event_ids: eventIds });
  }

  // ---- Admin --------------------------------------------------------

  async createTenant(
    tenantId: string,
    opts: { displayName?: string; quotas?: Record<string, number> } = {},
  ): Promise<TenantPayload & { api_key: string }> {
    const body: Record<string, unknown> = { tenant_id: tenantId };
    if (opts.displayName) body.display_name = opts.displayName;
    if (opts.quotas) body.quotas = opts.quotas;
    return this.post('/api/v1/admin/tenants', body);
  }

  async listTenants(): Promise<TenantPayload[]> {
    return this.get<TenantPayload[]>('/api/v1/admin/tenants');
  }

  async mintTenantKey(tenantId: string): Promise<string> {
    const r = await this.post<{ api_key: string }>(
      `/api/v1/admin/tenants/${encodeURIComponent(tenantId)}/keys`, {},
    );
    return r.api_key;
  }

  // ---- HTTP plumbing ------------------------------------------------

  private url(path: string): string {
    return `${this.baseUrl}${path}`;
  }

  private authHeaders(contentType?: string): HeadersInit {
    const h: Record<string, string> = {
      Authorization: `Bearer ${this.apiKey}`,
      'User-Agent': this.userAgent,
    };
    if (contentType) h['Content-Type'] = contentType;
    return h;
  }

  private request(path: string, init: RequestInit): Promise<Response> {
    return this.fetchImpl(this.url(path), {
      ...init,
      signal: init.signal ?? AbortSignal.timeout(this.timeoutMs),
    });
  }

  private async errFromResponse(r: Response): Promise<EngramError> {
    let detail: unknown = await r.text();
    try { detail = JSON.parse(detail as string); } catch { /* keep text */ }
    return new EngramError(r.status, detail, r.headers.get('x-request-id') ?? undefined);
  }

  private async retryable<T>(fn: () => Promise<Response>): Promise<Response> {
    let attempt = 0;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const r = await fn();
      if (r.status < 500 && r.status !== 429) return r;
      if (attempt >= this.retries) return r;
      const retryAfter = Number(r.headers.get('Retry-After') ?? 0) || Math.min(8, 0.5 * 2 ** attempt);
      await new Promise((res) => setTimeout(res, retryAfter * 1000));
      attempt += 1;
    }
  }

  private async get<T>(path: string): Promise<T> {
    const r = await this.retryable(() =>
      this.request(path, { method: 'GET', headers: this.authHeaders() }),
    );
    if (!r.ok) throw await this.errFromResponse(r);
    return r.json() as Promise<T>;
  }

  private async post<T>(path: string, body: unknown): Promise<T> {
    const r = await this.retryable(() =>
      this.request(path, {
        method: 'POST',
        headers: this.authHeaders('application/json'),
        body: JSON.stringify(body),
      }),
    );
    if (!r.ok) throw await this.errFromResponse(r);
    if (r.status === 204 || r.headers.get('content-length') === '0') return {} as T;
    return r.json() as Promise<T>;
  }

  private async delete(path: string): Promise<void> {
    const r = await this.retryable(() =>
      this.request(path, { method: 'DELETE', headers: this.authHeaders() }),
    );
    if (!r.ok) throw await this.errFromResponse(r);
  }
}

function memoryPath(uri: string): string {
  const stripped = uri.startsWith('mem://') ? uri.slice('mem://'.length) : uri;
  return stripped.split('/').map(encodeURIComponent).join('/');
}
