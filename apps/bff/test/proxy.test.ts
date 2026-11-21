import assert from 'node:assert/strict';
import { once } from 'node:events';
import http from 'node:http';
import type { TestContext } from 'node:test';
import test from 'node:test';
import pg from 'pg';
import { loadConfig } from '../src/config.ts';
import { startApp } from './helpers/server.ts';

const cookie = { Cookie: 'other=secret; rowan_session=rk_sess_proxy' };
const config = loadConfig({
  DATABASE_URL: 'postgres://unused:unused@127.0.0.1:1/unused',
  ROWAN_CORE_URL: 'http://127.0.0.1:1',
  ROWAN_WEB_ORIGIN: 'http://localhost:3000',
});

type Recorded = { method: string | undefined; url: string | undefined; headers: http.IncomingHttpHeaders; body: Buffer };

async function fakeCore(t: TestContext, status = 200, body: string | Buffer = '', headers: Record<string, string> = {}) {
  const requests: Recorded[] = [];
  const server = http.createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) {
      const value: unknown = chunk;
      assert.ok(Buffer.isBuffer(value));
      chunks.push(value);
    }
    requests.push({ method: req.method, url: req.url, headers: req.headers, body: Buffer.concat(chunks) });
    res.writeHead(status, headers);
    res.end(body);
  });
  t.after(() => new Promise<void>((resolve, reject) => {
    if (!server.listening) { resolve(); return; }
    server.close((error) => error ? reject(error) : resolve());
    server.closeAllConnections();
  }));
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  return { requests, server, url: `http://127.0.0.1:${address.port}` };
}

async function clientFor(t: TestContext, coreUrl: string, fetchImpl?: typeof fetch) {
  // No connection is opened. Any accidental DB lookup fails against the unused port.
  const pool = new pg.Pool({ connectionString: config.databaseUrl });
  t.after(() => pool.end());
  return startApp(t, { pool, config: { ...config, coreUrl }, ...(fetchImpl ? { fetch: fetchImpl } : {}) });
}

test('proxy preserves path, query, JSON bytes, status, and allowed headers only', async (t) => {
  const body = '{ "error": {"code":"conflict"} }\n';
  const core = await fakeCore(t, 409, body, {
    'Content-Type': 'application/json', 'Idempotent-Replay': 'true', 'Retry-After': '12',
    'WWW-Authenticate': 'Bearer', 'X-Internal': '1', 'Set-Cookie': 'secret=1',
    'Access-Control-Allow-Origin': '*',
  });
  const client = await clientFor(t, core.url);
  const input = '{ "company_id": "123" }\n';
  const response = await client.request('POST', '/api/v1/enrichment/run?x=1&name=a%2Fb', {
    body: input,
    headers: { ...cookie, 'Idempotency-Key': 'k1', Accept: 'application/json',
      Authorization: 'Bearer client-token', 'X-Secret': 'private', 'X-Forwarded-For': '1.2.3.4' },
  });
  assert.equal(response.status, 409);
  assert.equal(await response.text(), body);
  assert.equal(response.headers.get('Content-Type'), 'application/json');
  assert.equal(response.headers.get('Idempotent-Replay'), 'true');
  assert.equal(response.headers.get('Retry-After'), '12');
  assert.equal(response.headers.get('WWW-Authenticate'), 'Bearer');
  for (const name of ['X-Internal', 'Set-Cookie', 'Access-Control-Allow-Origin']) assert.equal(response.headers.get(name), null);
  assert.equal(core.requests.length, 1);
  const request = core.requests[0]!;
  assert.equal(request.method, 'POST');
  assert.equal(request.url, '/v1/enrichment/run?x=1&name=a%2Fb');
  assert.deepEqual(request.body, Buffer.from(input));
  assert.equal(request.headers.authorization, 'Bearer rk_sess_proxy');
  assert.equal(request.headers['idempotency-key'], 'k1');
  assert.equal(request.headers['content-type'], 'application/json');
  assert.equal(request.headers.accept, 'application/json');
  for (const name of ['cookie', 'origin', 'x-secret', 'x-forwarded-for']) assert.equal(request.headers[name], undefined);
});

test('proxy passes CSV and binary bytes unchanged', async (t) => {
  const bytes = Buffer.from('name,value\r\n"Rowan",42\r\n');
  const core = await fakeCore(t, 200, bytes, {
    'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename="export.csv"',
  });
  const client = await clientFor(t, core.url);
  const response = await client.request('PUT', '/api/v1/exports', {
    headers: { ...cookie, 'Content-Type': 'text/csv' }, body: bytes,
  });
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('Content-Type'), 'text/csv');
  assert.equal(response.headers.get('Content-Disposition'), 'attachment; filename="export.csv"');
  assert.deepEqual(Buffer.from(await response.arrayBuffer()), bytes);
  assert.deepEqual(core.requests[0]!.body, bytes);
  assert.equal(core.requests[0]!.headers['content-type'], 'text/csv');
});

test('proxy rejects absent session, oversized body, and foreign or missing Origin before core calls', async (t) => {
  const core = await fakeCore(t);
  const client = await clientFor(t, core.url);
  const unauthorized = await client.request('GET', '/api/v1/credits', { headers: { Authorization: 'Bearer ignored' } });
  assert.equal(unauthorized.status, 401);
  assert.deepEqual(await unauthorized.json(), { error: { code: 'unauthorized', message: 'authentication required' } });
  const oversized = await client.request('POST', '/api/v1/enrichment/run', {
    headers: cookie, body: Buffer.alloc(1_048_577),
  });
  assert.equal(oversized.status, 413);
  assert.deepEqual(await oversized.json(), { error: { code: 'payload_too_large', message: 'payload too large' } });
  for (const origin of ['https://evil.example', null]) {
    const forbidden = await client.request('POST', '/api/v1/enrichment/run', { headers: cookie, origin });
    assert.equal(forbidden.status, 403);
    assert.deepEqual(await forbidden.json(), { error: { code: 'origin_forbidden', message: 'origin forbidden' } });
  }
  assert.equal(core.requests.length, 0);
});

test('proxy accepts exact body limit and GET, HEAD, PATCH, DELETE, OPTIONS methods', async (t) => {
  const core = await fakeCore(t, 200, 'ok');
  const client = await clientFor(t, core.url);
  const boundary = Buffer.alloc(1_048_576, 0xff);
  const response = await client.request('POST', '/api/v1/data', { headers: cookie, body: boundary });
  assert.equal(response.status, 200);
  await response.text();
  assert.deepEqual(core.requests[0]!.body, boundary);
  for (const method of ['GET', 'HEAD', 'PATCH', 'DELETE', 'OPTIONS']) {
    const response = await client.request(method, '/api/v1/data', { headers: cookie });
    assert.equal(response.status, 200);
    await response.text();
    const recorded = core.requests.at(-1)!;
    assert.equal(recorded.method, method);
    assert.equal(recorded.body.length, 0);
  }
});

test('closed core port returns fixed 502 and logs internal error', async (t) => {
  const core = await fakeCore(t);
  await new Promise<void>((resolve, reject) => core.server.close((error) => error ? reject(error) : resolve()));
  const logged = t.mock.method(console, 'error', () => {});
  const client = await clientFor(t, core.url);
  const response = await client.request('GET', '/api/v1/credits', { headers: cookie });
  assert.equal(response.status, 502);
  const body = await response.text();
  assert.doesNotMatch(body, /ECONNREFUSED/);
  assert.deepEqual(JSON.parse(body), { error: { code: 'core_unavailable', message: 'core service unavailable' } });
  assert.equal(logged.mock.callCount(), 1);
});

test('proxy uses injected fetch and 30 second timeout', async (t) => {
  const logged = t.mock.method(console, 'error', () => {});
  const timeout = t.mock.method(AbortSignal, 'timeout', (milliseconds: number) => {
    assert.equal(milliseconds, 30000);
    return AbortSignal.abort(new DOMException('private timeout details', 'TimeoutError'));
  });
  const fetchImpl: typeof fetch = async (_input, init) => {
    assert.ok(init?.signal);
    init.signal.throwIfAborted();
    throw new Error('unreachable');
  };
  const client = await clientFor(t, config.coreUrl, fetchImpl);
  const response = await client.request('GET', '/api/v1/credits', { headers: cookie });
  assert.equal(response.status, 502);
  assert.deepEqual(await response.json(), { error: { code: 'core_unavailable', message: 'core service unavailable' } });
  assert.equal(timeout.mock.callCount(), 1);
  assert.equal(logged.mock.callCount(), 1);
});

test('proxy preserves redirect status without following Location or copying it', async (t) => {
  const core = await fakeCore(t, 307, 'redirect', { Location: '/elsewhere' });
  const client = await clientFor(t, core.url);
  const response = await client.request('GET', '/api/v1/data', { headers: cookie });
  assert.equal(response.status, 307);
  assert.equal(await response.text(), 'redirect');
  assert.equal(response.headers.get('Location'), null);
  assert.equal(core.requests.length, 1);
});
