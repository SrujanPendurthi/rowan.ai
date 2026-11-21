import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import test from 'node:test';
import { z } from 'zod';
import { loadConfig } from '../src/config.ts';
import { newTestDB } from './helpers/db.ts';
import { startApp } from './helpers/server.ts';

const config = loadConfig({
  DATABASE_URL: 'postgres://rowan:rowan@localhost:54329/rowan',
  ROWAN_CORE_URL: 'http://localhost:8080', ROWAN_WEB_ORIGIN: 'http://localhost:3000',
});
const keySchema = z.strictObject({
  id: z.uuid(), label: z.string(), monthly_spend_cap: z.number(), rate_per_minute: z.number(),
  created_at: z.iso.datetime(), revoked_at: z.iso.datetime().nullable(),
});
const createdSchema = z.strictObject({ key: keySchema, token: z.string() });
const listSchema = z.strictObject({ keys: z.array(keySchema) });
const accountSchema = z.object({
  user: z.object({ id: z.uuid() }), workspace: z.object({ id: z.uuid() }),
});
const errorSchema = z.object({ error: z.object({ code: z.string() }) });

test('agent keys are scoped, hashed, listed without secrets, and revoked idempotently', async (t) => {
  const pool = await newTestDB(t);
  let now = new Date('2026-11-21T12:00:00Z');
  const deps = { pool, config, now: () => now };
  const owner = await startApp(t, deps);
  const other = await startApp(t, deps);
  const anonymous = await startApp(t, deps);
  const accounts = [];
  for (const [client, email] of [[owner, 'owner@example.com'], [other, 'other@example.com']] as const) {
    const response = await client.request('POST', '/api/auth/signup', {
      body: { email, password: 'correct horse battery staple', workspace_name: 'Test' },
    });
    assert.equal(response.status, 201);
    accounts.push(accountSchema.parse(await response.json()));
  }
  const account = accounts[0]!;
  const otherAccount = accounts[1]!;
  const response = await owner.request('POST', '/api/keys', { body: { label: ' agent-1 ', monthly_spend_cap: 500 } });
  assert.equal(response.status, 201);
  const created = createdSchema.parse(await response.json());
  assert.match(created.token, /^rk_live_[A-Za-z0-9_-]{43}$/);
  assert.deepEqual(created.key, {
    id: created.key.id, label: 'agent-1', monthly_spend_cap: 500, rate_per_minute: 120,
    created_at: now.toISOString(), revoked_at: null,
  });
  const stored = (await pool.query<{
    key_hash: Buffer; kind: string; user_id: string; workspace_id: string; expires_at: Date | null;
  }>('SELECT key_hash, kind, user_id, workspace_id, expires_at FROM api_keys WHERE id = $1', [created.key.id])).rows[0]!;
  assert.deepEqual(stored.key_hash, createHash('sha256').update(created.token).digest());
  assert.equal(stored.kind, 'agent');
  assert.equal(stored.user_id, account.user.id);
  assert.equal(stored.workspace_id, account.workspace.id);
  assert.equal(stored.expires_at, null);

  // CLI-created keys have no user and may exceed JavaScript's safe integer range.
  const cliIds = ['00000000-0000-4000-8000-000000000002', '00000000-0000-4000-8000-000000000001'];
  const later = new Date(now.getTime() + 1000);
  for (const id of cliIds) {
    await pool.query(`INSERT INTO api_keys
      (id, workspace_id, key_hash, label, monthly_spend_cap, created_at)
      VALUES ($1, $2, $3, $4, $5, $6)`,
    [id, account.workspace.id, createHash('sha256').update(id).digest(), 'cli', '9223372036854775807', later]);
  }
  const listed = await owner.request('GET', '/api/keys');
  assert.equal(listed.status, 200);
  const text = await listed.text();
  assert.doesNotMatch(text, /key_hash|token/);
  const keys = listSchema.parse(JSON.parse(text)).keys;
  assert.deepEqual(keys.map((key) => key.id), [cliIds[1], cliIds[0], created.key.id]);
  assert.equal(keys[0]!.monthly_spend_cap, Number('9223372036854775807'));
  assert.deepEqual(keys[2], created.key);
  const otherList = await other.request('GET', '/api/keys');
  assert.equal(otherList.status, 200);
  assert.deepEqual(listSchema.parse(await otherList.json()).keys, []);

  const sessionId = (await pool.query<{ id: string }>(
    "SELECT id FROM api_keys WHERE user_id = $1 AND kind = 'session'", [otherAccount.user.id],
  )).rows[0]!.id;
  for (const id of [created.key.id, sessionId, 'not-a-uuid', randomUUID()]) {
    const denied = await other.request('DELETE', `/api/keys/${id}`);
    assert.equal(denied.status, 404);
    assert.equal(errorSchema.parse(await denied.json()).error.code, 'not_found');
  }
  const unchanged = await pool.query<{ revoked_at: Date | null }>(
    'SELECT revoked_at FROM api_keys WHERE id = $1 OR id = $2', [created.key.id, sessionId],
  );
  assert.equal(unchanged.rowCount, 2);
  assert.ok(unchanged.rows.every((row) => row.revoked_at === null));
  now = later;
  assert.equal((await owner.request('DELETE', `/api/keys/${created.key.id}`)).status, 204);
  now = new Date(later.getTime() + 1000);
  assert.equal((await owner.request('DELETE', `/api/keys/${created.key.id}`)).status, 204);
  const revoked = (await pool.query<{ revoked_at: Date }>(
    'SELECT revoked_at FROM api_keys WHERE id = $1', [created.key.id],
  )).rows[0]!;
  assert.equal(revoked.revoked_at.toISOString(), later.toISOString());
  const after = listSchema.parse(await (await owner.request('GET', '/api/keys')).json());
  assert.equal(after.keys.find((key) => key.id === created.key.id)?.revoked_at, later.toISOString());

  for (const [method, path] of [['GET', '/api/keys'], ['POST', '/api/keys'], ['DELETE', `/api/keys/${created.key.id}`]]) {
    const denied = await anonymous.request(method!, path!, { body: method === 'POST' ? { label: 'x', monthly_spend_cap: 0 } : undefined });
    assert.equal(denied.status, 401);
    assert.deepEqual(await denied.json(), { error: { code: 'unauthorized', message: 'authentication required' } });
  }
});

test('agent key bodies enforce trimmed labels and integer cap and rate bounds', async (t) => {
  const pool = await newTestDB(t);
  const client = await startApp(t, { pool, config });
  assert.equal((await client.request('POST', '/api/auth/signup', {
    body: { email: 'bounds@example.com', password: 'correct horse battery staple', workspace_name: 'Test' },
  })).status, 201);
  const valid = { label: 'agent', monthly_spend_cap: 500 };
  const invalid = [
    { monthly_spend_cap: 1000001 }, { monthly_spend_cap: -1 }, { monthly_spend_cap: 0.5 },
    { monthly_spend_cap: '500' }, { rate_per_minute: 0 }, { rate_per_minute: 601 },
    { rate_per_minute: 1.5 }, { rate_per_minute: '120' }, { label: '   ' },
    { label: 'x'.repeat(101) }, { label: null },
  ];
  for (const patch of invalid) {
    const response = await client.request('POST', '/api/keys', { body: { ...valid, ...patch } });
    assert.equal(response.status, 400, JSON.stringify(patch));
    assert.equal(errorSchema.parse(await response.json()).error.code, 'invalid_request');
  }
  for (const body of [null, {}, { label: 'x' }]) {
    const response = await client.request('POST', '/api/keys', { body });
    assert.equal(response.status, 400);
    assert.equal(errorSchema.parse(await response.json()).error.code, 'invalid_request');
  }
  assert.equal((await pool.query("SELECT id FROM api_keys WHERE kind = 'agent'")).rowCount, 0);
  for (const [cap, rate, label] of [[0, 1, ' x '], [1000000, 600, ` ${'x'.repeat(100)} `]] as const) {
    const response = await client.request('POST', '/api/keys', {
      body: { label, monthly_spend_cap: cap, rate_per_minute: rate },
    });
    assert.equal(response.status, 201);
    const created = createdSchema.parse(await response.json());
    assert.equal(created.key.label, label.trim());
    assert.equal(created.key.monthly_spend_cap, cap);
    assert.equal(created.key.rate_per_minute, rate);
  }
});
