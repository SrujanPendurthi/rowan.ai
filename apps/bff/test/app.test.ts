import assert from 'node:assert/strict';
import { createServer, IncomingMessage, request } from 'node:http';
import { Socket } from 'node:net';
import test from 'node:test';
import { readdir } from 'node:fs/promises';
import { z } from 'zod';
import { once } from 'node:events';
import { createApp } from '../src/app.ts';
import { loadConfig } from '../src/config.ts';
import { BadRequestError, getCookie, parseJSON, PayloadTooLargeError, readBody } from '../src/http.ts';
import { newTestDB } from './helpers/db.ts';
import { startApp } from './helpers/server.ts';

const env = {
  DATABASE_URL: 'postgres://rowan:rowan@localhost:54329/rowan',
  ROWAN_CORE_URL: 'http://localhost:8080/',
  ROWAN_WEB_ORIGIN: 'http://localhost:3000/',
};

test('health, missing routes, and origin guard', async (t) => {
  const pool = await newTestDB(t);
  const client = await startApp(t, { pool, config: loadConfig(env) });
  const health = await client.request('GET', '/healthz');
  assert.equal(health.status, 200);
  assert.deepEqual(await health.json(), { ok: true });
  const missing = await client.request('GET', '/nope');
  assert.equal(missing.status, 404);
  assert.equal(missing.headers.get('cache-control'), 'no-store');
  assert.deepEqual(await missing.json(), { error: { code: 'not_found', message: 'not found' } });
  for (const origin of [null, 'https://evil.example']) {
    const denied = await client.request('POST', '/api/anything', { origin });
    assert.equal(denied.status, 403);
    assert.deepEqual(await denied.json(), { error: { code: 'origin_forbidden', message: 'origin forbidden' } });
  }
  assert.equal((await client.request('POST', '/api/anything')).status, 404);
  assert.equal((await client.request('POST', '/api', { origin: null })).status, 403);
  assert.equal((await client.request('HEAD', '/api/anything', { origin: null })).status, 404);
  assert.equal((await client.request('POST', '/apiculture', { origin: null })).status, 404);

  const files = (await readdir(new URL('../../../migrations/', import.meta.url))).filter((f) => f.endsWith('.sql')).sort();
  const ledger = await pool.query<{ version: string }>('SELECT version FROM schema_migrations ORDER BY version');
  assert.deepEqual(ledger.rows.map((row) => row.version), files);
  assert.equal((await pool.query('SELECT * FROM users')).rowCount, 0);

  const logged = t.mock.method(console, 'error', () => {});
  const broken = await startApp(t, {
    pool,
    config: {
      ...loadConfig(env),
      get webOrigin(): string { throw new Error('private database details'); },
    },
  });
  const failure = await broken.request('POST', '/api/anything', { origin: 'http://localhost:3000' });
  assert.equal(failure.status, 500);
  assert.deepEqual(await failure.json(), { error: { code: 'internal', message: 'internal error' } });
  assert.equal(logged.mock.callCount(), 1);
});

test('config validates env and applies defaults', () => {
  assert.throws(() => loadConfig({}), /DATABASE_URL/);
  assert.throws(() => loadConfig({ ...env, ROWAN_CORE_URL: 'ftp://x' }), /ROWAN_CORE_URL/);
  assert.throws(() => loadConfig({ ...env, ROWAN_WEB_ORIGIN: 'not a URL' }), /ROWAN_WEB_ORIGIN/);
  for (const PORT of ['0', '65536', 'bad', '']) {
    assert.throws(() => loadConfig({ ...env, PORT }), /PORT/);
  }
  assert.throws(() => loadConfig({ ...env, ROWAN_COOKIE_SECURE: 'no' }), /ROWAN_COOKIE_SECURE/);
  const config = loadConfig(env);
  assert.equal(config.port, 8081);
  assert.equal(config.cookieSecure, true);
  assert.equal(config.coreUrl, 'http://localhost:8080');
  assert.equal(config.webOrigin, 'http://localhost:3000');
  assert.equal(loadConfig({ ...env, ROWAN_COOKIE_SECURE: 'false' }).cookieSecure, false);
});

test('HTTP helpers validate JSON, cookies, and body size boundary', async () => {
  const schema = z.object({ name: z.string() });
  assert.deepEqual(parseJSON(Buffer.from('{"name":"Rowan"}'), schema), { name: 'Rowan' });
  assert.throws(() => parseJSON(Buffer.from('{'), schema), BadRequestError);
  assert.throws(() => parseJSON(Buffer.from('{"name":1}'), schema), /name/);
  const req = new IncomingMessage(new Socket());
  req.headers.cookie = 'other=x; rowan_session=rk_sess_abc';
  assert.equal(getCookie(req, 'rowan_session'), 'rk_sess_abc');
  assert.equal(getCookie(req, 'missing'), undefined);
  for (const size of [0, 1_048_576, 1_048_577]) {
    const bodyReq = new IncomingMessage(new Socket());
    const result = readBody(bodyReq);
    bodyReq.push(Buffer.alloc(size));
    bodyReq.push(null);
    if (size > 1_048_576) await assert.rejects(result, PayloadTooLargeError);
    else assert.equal((await result).length, size);
  }
});

test('GET // returns not_found for an invalid request URL', async (t) => {
  const pool = await newTestDB(t);
  const server = createServer(createApp({ pool, config: loadConfig(env) }));
  t.after(() => new Promise<void>((resolve) => server.close(() => resolve())));
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const response = await new Promise<{ status: number | undefined; body: string }>((resolve, reject) => {
    const req = request({ host: '127.0.0.1', port: address.port, path: '//', method: 'GET' }, (res) => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', (chunk: string) => { body += chunk; });
      res.on('end', () => resolve({ status: res.statusCode, body }));
    });
    req.on('error', reject);
    req.end();
  });
  assert.equal(response.status, 404);
  assert.deepEqual(JSON.parse(response.body), { error: { code: 'not_found', message: 'not found' } });
});

test('web origin rejects paths and noncanonical origins while core URL permits paths', () => {
  for (const ROWAN_WEB_ORIGIN of ['http://localhost:3000/app', 'HTTP://Localhost:3000']) {
    assert.throws(() => loadConfig({ ...env, ROWAN_WEB_ORIGIN }), /ROWAN_WEB_ORIGIN/);
  }
  assert.equal(loadConfig({ ...env, ROWAN_CORE_URL: 'http://localhost:8080/app/' }).coreUrl, 'http://localhost:8080/app');
});
