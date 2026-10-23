const test = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { webcrypto } = require('node:crypto');
const vm = require('node:vm');

const template = readFileSync('engram/admin/templates/chat.html', 'utf8');
const script = template.split('<script>')[1].split('</script>')[0];
const json = (body, status = 200) => new Response(JSON.stringify(body), {
  status, headers: { 'content-type': 'application/json' },
});
const storage = () => {
  const data = new Map();
  return { getItem: key => data.get(key) || null, setItem: (key, value) => data.set(key, String(value)), removeItem: key => data.delete(key) };
};
const running = { run_id: 'run-1', session_id: 'sess-1', client_message_id: 'existing-message', status: 'RUNNING', created_at: new Date().toISOString() };
const completed = { ...running, status: 'COMPLETED', response: { answer: 'Atul is your colleague.', retrieval_metadata: { levels_visited: ['L1'] } } };
const history = { turns: [{ id: 'turn:user:0', role: 'user', content: 'Who is Atul?', client_message_id: running.client_message_id }] };
const completedHistory = { turns: [...history.turns, { id: 'turn:assistant:1', role: 'assistant', content: completed.response.answer, run_id: running.run_id }] };

function setup(fetch, options = {}) {
  const timers = new Map();
  let timerId = 0;
  const context = {
    crypto: webcrypto, TextDecoder, AbortController,
    localStorage: options.localStorage || storage(), sessionStorage: options.sessionStorage || storage(),
    fetch, setTimeout: (fn, delay) => { timers.set(++timerId, { fn, delay }); return timerId; },
    clearTimeout: id => timers.delete(id), ...options,
  };
  vm.createContext(context);
  vm.runInContext(script, context);
  const component = context.chat();
  component._authHeaders = () => ({});
  component.$nextTick = callback => callback();
  component.$refs = { history: { scrollTop: 0, scrollHeight: 0 } };
  component.sessionId = 'sess-1';
  return { component, context, timers, tick: async () => {
    const [id, timer] = timers.entries().next().value;
    timers.delete(id);
    await timer.fn();
  } };
}

function sse(events, chunkSize = 1) {
  const bytes = new TextEncoder().encode(events);
  let offset = 0;
  return new Response(new ReadableStream({ pull(controller) {
    if (offset >= bytes.length) { controller.close(); return; }
    controller.enqueue(bytes.slice(offset, offset += chunkSize));
  } }), { headers: { 'content-type': 'text/event-stream' } });
}

for (const ending of ['\n', '\r\n']) {
  test(`chat reconstructs one-byte SSE chunks with ${JSON.stringify(ending)} endings`, async () => {
    const events = [
      'event: accepted\ndata: {"run_id":"run-1","session_id":"sess-1"}\n\n',
      'event: delta\ndata: {"text":\ndata: "Bengaluru ☀"}\n\n',
      'event: done\ndata: {}\n\n',
    ].join('').replaceAll('\n', ending);
    const { component } = setup(async () => sse(events));
    component.input = 'Where do I live?';
    await component.send();
    assert.equal(component.messages[0].saved, true);
    assert.equal(component.messages[1].content, 'Bengaluru ☀');
    assert.equal(component.messages[1].loading, false);
    assert.equal(component.loading, false);
    assert.equal(component.sessionNotice, 'Message saved');
  });
}

for (const [name, cryptoApi] of [
  ['getRandomValues without randomUUID', { getRandomValues: webcrypto.getRandomValues.bind(webcrypto) }],
  ['crypto without either random API', {}],
  ['no crypto global', undefined],
]) {
  test(`chat sends and preserves the retry ID with ${name}`, async () => {
    const requests = [];
    const { component } = setup(async (_url, options) => {
      requests.push(JSON.parse(options.body));
      return requests.length === 1 ? json({ detail: 'session not found' }, 404) : json({ answer: 'Saved response' });
    }, { crypto: cryptoApi });
    component.sessionId = 'sess-old';
    component.createSession = async () => { component.sessionId = 'sess-new'; return true; };
    component.input = 'First message';
    await component.send();
    assert.equal(requests.length, 2);
    assert.match(requests[0].client_message_id, /^[A-Za-z0-9-]{1,128}$/);
    assert.equal(requests[0].client_message_id, requests[1].client_message_id);
    assert.equal(component.messages[1].content, 'Saved response');
    assert.equal(component.loading, false);
    component.input = 'Second message';
    await component.send();
    assert.equal(requests.length, 3);
    assert.notEqual(requests[2].client_message_id, requests[0].client_message_id);
    assert.equal(component.messages[3].content, 'Saved response');
  });
}

for (const [status, body, expected] of [
  [409, { detail: 'client_message_id was already used for different input' }, 'different input'],
  [422, { detail: [{ msg: 'Invalid input' }] }, 'Invalid input'],
  [403, 'Permission denied', 'Permission denied'],
]) {
  test(`HTTP ${status} is read once and shown as a notice`, async () => {
    const response = typeof body === 'string' ? new Response(body, { status }) : json(body, status);
    const { component } = setup(async () => response);
    component.input = 'Unsent question';
    await component.send();
    assert.match(component.sessionError, new RegExp(expected));
    assert.equal(response.bodyUsed, true);
    assert.equal(component.messages.length, 0);
    assert.equal(component.input, 'Unsent question');
    assert.equal(component.loading, false);
  });
}

test('busy response preserves draft, polls existing run and renders completion once', async () => {
  let finished = false, posts = 0;
  const state = setup(async (url, options) => {
    if (options.method === 'POST') { posts++; return json({ detail: 'a response is already running for this session', code: 'session_busy', run: running }, 409); }
    if (url.endsWith('/history')) return json(finished ? completedHistory : history);
    return json(finished ? completed : running);
  });
  const { component, timers, tick } = state;
  component.input = 'My next question';
  await component.send();
  assert.equal(component.input, 'My next question');
  assert.equal(component.activeRunId, 'run-1');
  assert.equal(component.loading, true);
  assert.equal(component.messages.some(msg => msg.content === 'My next question'), false);
  assert.equal([...timers.values()][0].delay, 2000);
  await component.send();
  assert.equal(posts, 1);
  finished = true;
  await tick();
  assert.equal(component.messages.filter(msg => msg.content === completed.response.answer).length, 1);
  assert.equal(component.loading, false);
  assert.equal(component.input, 'My next question');
  assert.equal(timers.size, 0);
  await component._startRecovery(completed);
  assert.equal(component.messages.filter(msg => msg.content === completed.response.answer).length, 1);
});

test('poll failure keeps Stop available and backs off without resubmitting', async () => {
  const { component, timers } = setup(async () => { throw new TypeError('offline'); });
  await component._startRecovery(running);
  assert.equal(component.activeRunId, 'run-1');
  assert.equal(component.loading, true);
  assert.equal([...timers.values()][0].delay, 10000);
  component.destroy();
  assert.equal(timers.size, 0);
});

test('disconnect after acceptance recovers the stored answer instead of posting again', async () => {
  let posts = 0;
  const { component } = setup(async (url, options) => {
    if (options.method === 'POST') {
      posts++;
      return sse('event: accepted\ndata: {"run_id":"run-1"}\n\nevent: delta\ndata: {"text":"Partial"}\n\n');
    }
    return json(url.endsWith('/history') ? completedHistory : completed);
  });
  component.input = 'Who is Atul?';
  await component.send();
  assert.equal(posts, 1);
  assert.equal(component.messages.filter(msg => msg.content === completed.response.answer).length, 1);
  assert.equal(component.messages.some(msg => msg.content === 'Partial'), false);
  assert.equal(component.loading, false);
});

test('disconnect before acknowledgement discovers the submission by its client ID', async () => {
  let clientId, posts = 0, discoveryUrl;
  const { component } = setup(async (url, options) => {
    if (options.method === 'POST') {
      posts++;
      clientId = JSON.parse(options.body).client_message_id;
      throw new TypeError('lost acknowledgement');
    }
    if (url.includes('/chat-run')) { discoveryUrl = url; return json({ run: { ...completed, client_message_id: clientId } }); }
    return json(completedHistory);
  });
  component.input = 'Who is Atul?';
  await component.send();
  assert.match(discoveryUrl, new RegExp('client_message_id=' + clientId));
  assert.equal(posts, 1);
  assert.equal(component.loading, false);
  assert.equal(component.messages.at(-1).content, completed.response.answer);
});

test('refresh discovers a running reply in the session shared by two tabs', async () => {
  const shared = storage();
  shared.setItem('engram_chat_session_id', 'sess-1');
  const fetch = async url => json(url.endsWith('/history') ? history : url.includes('/chat-run') ? { run: running } : running);
  const first = setup(fetch, { localStorage: shared });
  const second = setup(fetch, { localStorage: shared });
  await first.component.init();
  await second.component.init();
  for (const { component } of [first, second]) {
    assert.equal(component.activeRunId, 'run-1');
    assert.equal(component.loading, true);
    assert.equal(component.messages.filter(msg => msg.role === 'user').length, 1);
    component.destroy();
  }
});

test('cancellation racing completion renders the completed answer', async () => {
  const { component } = setup(async url => json(url.endsWith('/history') ? completedHistory : completed));
  component.loading = true;
  component.activeRunId = 'run-1';
  component.input = 'Keep this draft';
  await component.stopResponse();
  assert.equal(component.messages.at(-1).content, completed.response.answer);
  assert.equal(component.loading, false);
  assert.equal(component.input, 'Keep this draft');
  assert.doesNotMatch(component.sessionNotice, /stopped/);
});

for (const [status, error_code] of [['CANCELLED', 'cancelled'], ['FAILED', 'deadline_exceeded']]) {
  test(`${status} releases Send and preserves the accepted question`, async () => {
    const { component } = setup(async url => json(url.endsWith('/history') ? history : { ...running, status, error_code }));
    await component._startRecovery(running);
    assert.equal(component.loading, false);
    assert.equal(component.activeRunId, null);
    assert.equal(component.messages.length, 1);
    assert.equal(component.messages[0].role, 'user');
    assert.match(component.sessionNotice, /message is saved/);
  });
}

test('notices and greeting are excluded from subsequent model context', async () => {
  const requests = [];
  const { component } = setup(async (_url, options) => {
    requests.push(JSON.parse(options.body));
    return requests.length === 1 ? json({ detail: 'bad request' }, 422) : json({ answer: 'Done' });
  });
  component._greet();
  component.input = 'Rejected question';
  await component.send();
  component.input = 'Actual question';
  await component.send();
  assert.deepEqual(requests[1].messages, [{ role: 'user', content: 'Actual question' }]);
});

test('an unconfirmed submission is not confused with an older completed run', async () => {
  let posts = 0;
  const { component, context, timers } = setup(async (url, options) => {
    if (options.method === 'POST') { posts++; throw new TypeError('lost request'); }
    return json(url.includes('client_message_id=') ? { run: null } : { run: completed });
  });
  component.input = 'Unconfirmed question';
  await component.send();
  assert.equal(component.loading, true);
  assert.equal(posts, 1);
  assert.equal(component.activeRunId, null);
  assert.equal(component.messages.some(msg => msg.content === completed.response.answer), false);
  assert.equal(context.sessionStorage.getItem('engram_chat_submitted:sess-1'), 'Unconfirmed question');
  assert.equal(timers.size, 1);
  component.destroy();
});

test('polling is sequential even when a status read takes longer than the polling interval', async () => {
  let release, polls = 0;
  const { component, timers } = setup(async url => {
    if (url.endsWith('/history')) return json(history);
    polls++;
    await new Promise(resolve => { release = resolve; });
    return json(running);
  });
  const pending = component._startRecovery(running);
  assert.equal(polls, 1);
  assert.equal([...timers.values()].filter(timer => timer.delay === 2000).length, 0);
  release();
  await pending;
  assert.equal(polls, 1);
  assert.equal(timers.size, 1);
  component.destroy();
});

test('page teardown ignores an in-flight recovery and preserves pending identifiers', async () => {
  let release;
  const { component, context, timers } = setup(async () => {
    await new Promise(resolve => { release = resolve; });
    return json(running);
  });
  const pending = component._startRecovery(running);
  component.destroy();
  release();
  await pending;
  assert.equal(timers.size, 0);
  assert.equal(component.messages.length, 0);
  assert.equal(JSON.parse(context.localStorage.getItem('engram_chat_pending:sess-1')).run_id, 'run-1');
});

test('a stalled status read times out and schedules recovery backoff', async () => {
  const state = setup(async (_url, options) => new Promise((_, reject) => {
    options.signal.addEventListener('abort', () => reject(new DOMException('Timed out', 'AbortError')));
  }));
  const pending = state.component._startRecovery(running);
  const timeout = [...state.timers.values()].find(timer => timer.delay === 15000);
  timeout.fn();
  await pending;
  assert.equal(state.component.activeRunId, 'run-1');
  assert.equal(state.component.loading, true);
  assert.equal([...state.timers.values()][0].delay, 10000);
  state.component.destroy();
});

test('typing while a submission is rejected preserves both unsent questions', async () => {
  let release;
  const { component, context } = setup(async (url, options) => {
    if (options.method === 'POST') {
      await new Promise(resolve => { release = resolve; });
      return json({ detail: 'session busy', code: 'session_busy', run: running }, 409);
    }
    return json(url.endsWith('/history') ? completedHistory : completed);
  });
  component.input = 'Rejected question';
  const request = component.send();
  component.input = 'Another draft';
  release();
  await request;
  assert.equal(component.input, 'Another draft');
  assert.equal(component.unsentDraft, 'Rejected question');
  assert.equal(context.sessionStorage.getItem('engram_chat_unsent:sess-1'), 'Rejected question');
  component.restoreUnsentDraft();
  assert.equal(component.input, 'Rejected question');
  assert.equal(component.unsentDraft, 'Another draft');
});
