import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import { z } from 'zod';
import { loadConfig } from '../src/config.ts';
import { LoginLimiter } from '../src/auth/limiter.ts';
import { newTestDB } from './helpers/db.ts';
import { startApp } from './helpers/server.ts';

const config = loadConfig({
  DATABASE_URL: 'postgres://rowan:rowan@localhost:54329/rowan',
  ROWAN_CORE_URL: 'http://localhost:8080', ROWAN_WEB_ORIGIN: 'http://localhost:3000',
});
const password = 'correct horse battery staple';
const accountSchema = z.object({
  user: z.object({ id: z.uuid(), email: z.email() }),
  workspace: z.object({ id: z.uuid(), name: z.string(), tier: z.literal('solo') }),
});

test('signup, normalized login, transactional duplicates, logout, and absolute expiry', async (t) => {
  const pool = await newTestDB(t);
  let now = new Date('2026-11-21T12:00:00Z');
  const client = await startApp(t, { pool, config, now: () => now });
  const signup = await client.request('POST', '/api/auth/signup', {
    body: { email: ' Founder@Example.com ', password, workspace_name: 'Acme' },
  });
  assert.equal(signup.status, 201);
  const account = accountSchema.parse(await signup.json());
  const cookie = signup.headers.get('set-cookie') ?? '';
  for (const attribute of ['HttpOnly', 'SameSite=Strict', 'Path=/', 'Max-Age=1209600', 'Secure']) assert.ok(cookie.includes(attribute));
  const oldCookie = cookie.split(';')[0]!;
  assert.match(oldCookie, /^rowan_session=rk_sess_[A-Za-z0-9_-]{43}$/);
  const key = (await pool.query<{
    kind: string; label: string; rate_per_minute: number; monthly_spend_cap: string;
    expires_at: Date; created_at: Date; key_hash: Buffer; user_id: string;
  }>('SELECT * FROM api_keys')).rows[0]!;
  assert.equal(key.kind, 'session');
  assert.equal(key.label, 'session');
  assert.equal(key.rate_per_minute, 300);
  assert.equal(key.monthly_spend_cap, '9223372036854775807');
  assert.equal(key.user_id, account.user.id);
  assert.equal(key.created_at.toISOString(), now.toISOString());
  assert.equal(key.expires_at.getTime(), now.getTime() + 14 * 86400000);
  assert.deepEqual(key.key_hash, createHash('sha256').update(oldCookie.slice('rowan_session='.length)).digest());
  const me = await client.request('GET', '/api/me');
  assert.equal(me.status, 200);
  assert.equal(me.headers.get('cache-control'), 'no-store');
  assert.deepEqual(accountSchema.parse(await me.json()), account);
  assert.equal(account.user.email, 'founder@example.com');
  assert.equal(account.workspace.tier, 'solo');
  const duplicate = await client.request('POST', '/api/auth/signup', {
    body: { email: 'founder@example.com', password, workspace_name: 'Duplicate' },
  });
  assert.equal(duplicate.status, 409);
  assert.equal(z.object({ error: z.object({ code: z.literal('email_taken') }) }).safeParse(await duplicate.json()).success, true);
  for (const table of ['workspaces', 'users', 'api_keys']) {
    // Table names are fixed test constants, never request input.
    assert.equal((await pool.query(`SELECT * FROM ${table}`)).rowCount, 1);
  }
  const short = await client.request('POST', '/api/auth/signup', {
    body: { email: 'short@example.com', password: '12345678901', workspace_name: 'Short' },
  });
  assert.equal(short.status, 400);
  assert.match(await short.text(), /invalid_request/);
  const blankWorkspace = await client.request('POST', '/api/auth/signup', {
    body: { email: 'blank@example.com', password, workspace_name: '   ' },
  });
  assert.equal(blankWorkspace.status, 400);
  assert.equal(z.object({ error: z.object({ code: z.literal('invalid_request') }) }).safeParse(await blankWorkspace.json()).success, true);
  const login = await client.request('POST', '/api/auth/login', { body: { email: 'FOUNDER@example.com', password } });
  assert.equal(login.status, 200);
  assert.deepEqual(accountSchema.parse(await login.json()), account);
  const wrong = await client.request('POST', '/api/auth/login', { body: { email: account.user.email, password: 'wrong password' } });
  const unknown = await client.request('POST', '/api/auth/login', { body: { email: 'unknown@example.com', password: 'wrong password' } });
  assert.equal(wrong.status, 401);
  assert.equal(unknown.status, 401);
  assert.equal(await wrong.text(), await unknown.text());
  const logout = await client.request('POST', '/api/auth/logout', { headers: { Cookie: oldCookie } });
  assert.equal(logout.status, 204);
  assert.match(logout.headers.get('set-cookie') ?? '', /Max-Age=0/);
  const revoked = (await pool.query<{ revoked_at: Date }>('SELECT revoked_at FROM api_keys WHERE key_hash = $1', [key.key_hash])).rows[0]!;
  assert.equal(revoked.revoked_at.toISOString(), now.toISOString());
  assert.equal((await client.request('GET', '/api/me', { headers: { Cookie: oldCookie } })).status, 401);
  assert.equal((await client.request('GET', '/api/me')).status, 401);
  assert.equal((await client.request('POST', '/api/auth/logout')).status, 204);
  await client.request('POST', '/api/auth/login', { body: { email: account.user.email, password } });
  now = new Date(now.getTime() + 14 * 86400000 - 1);
  assert.equal((await client.request('GET', '/api/me')).status, 200);
  now = new Date(now.getTime() + 1);
  assert.equal((await client.request('GET', '/api/me')).status, 401);
});

test('five failed logins block only that email until the oldest failure expires', async (t) => {
  const pool = await newTestDB(t);
  let now = new Date('2026-11-21T12:00:00Z');
  const client = await startApp(t, { pool, config: { ...config, cookieSecure: false }, now: () => now });
  for (const email of ['blocked@example.com', 'other@example.com']) {
    const signup = await client.request('POST', '/api/auth/signup', { body: { email, password, workspace_name: 'Test' } });
    assert.equal(signup.status, 201);
    assert.doesNotMatch(signup.headers.get('set-cookie') ?? '', /; Secure/);
  }
  for (let i = 0; i < 5; i++) {
    const response = await client.request('POST', '/api/auth/login', { body: { email: 'blocked@example.com', password: 'wrong password' } });
    assert.equal(response.status, 401);
  }
  const blocked = await client.request('POST', '/api/auth/login', { body: { email: 'BLOCKED@example.com', password } });
  assert.equal(blocked.status, 429);
  assert.equal(blocked.headers.get('retry-after'), '900');
  assert.match(await blocked.text(), /rate_limited/);
  assert.equal((await client.request('POST', '/api/auth/login', { body: { email: 'other@example.com', password } })).status, 200);
  now = new Date(now.getTime() + 15 * 60000);
  assert.equal((await client.request('POST', '/api/auth/login', { body: { email: 'blocked@example.com', password } })).status, 200);
});

test('limiter uses the oldest failure, expires at boundary, and resets', () => {
  const limiter = new LoginLimiter();
  const start = new Date('2026-11-21T12:00:00Z');
  for (let i = 0; i < 5; i++) limiter.fail('email', new Date(start.getTime() + i * 1000));
  assert.equal(limiter.check('email', new Date(start.getTime() + 4000)), 896);
  assert.equal(limiter.check('other', start), 0);
  assert.equal(limiter.check('email', new Date(start.getTime() + 900000)), 0);
  limiter.fail('email', new Date(start.getTime() + 900000));
  assert.equal(limiter.check('email', new Date(start.getTime() + 900000)), 1);
  limiter.reset('email');
  assert.equal(limiter.check('email', start), 0);
});

test('concurrent wrong-password logins admit at most five attempts', async (t) => {
  const pool = await newTestDB(t);
  // Keep one DB socket for deterministic teardown; password verification still overlaps.
  pool.options.max = 1;
  const client = await startApp(t, { pool, config });
  assert.equal((await client.request('POST', '/api/auth/signup', {
    body: { email: 'concurrent@example.com', password, workspace_name: 'Concurrent' },
  })).status, 201);
  const responses = await Promise.all(Array.from({ length: 10 }, () => client.request('POST', '/api/auth/login', {
    body: { email: 'concurrent@example.com', password: 'wrong password' },
  })));
  assert.equal(responses.filter((response) => response.status === 401).length, 5);
  assert.equal(responses.filter((response) => response.status === 429).length, 5);
});

test('seven logins retain only the newest five live sessions', async (t) => {
  const pool = await newTestDB(t);
  let now = new Date('2026-11-21T12:00:00Z');
  const client = await startApp(t, { pool, config, now: () => now });
  const email = 'sessions@example.com';
  const signup = await client.request('POST', '/api/auth/signup', { body: { email, password, workspace_name: 'Sessions' } });
  const account = accountSchema.parse(await signup.json());
  const cookies: string[] = [];
  for (let i = 0; i < 7; i++) {
    now = new Date(now.getTime() + 1000);
    const login = await client.request('POST', '/api/auth/login', { body: { email, password } });
    assert.equal(login.status, 200);
    cookies.push(login.headers.get('set-cookie')!.split(';')[0]!);
  }
  const live = await pool.query(`SELECT id FROM api_keys WHERE user_id = $1 AND kind = 'session'
    AND revoked_at IS NULL AND expires_at > $2`, [account.user.id, now]);
  assert.equal(live.rowCount, 5);
  for (const cookie of cookies.slice(0, 2)) {
    assert.equal((await client.request('GET', '/api/me', { headers: { Cookie: cookie } })).status, 401);
  }
  assert.equal((await client.request('GET', '/api/me', { headers: { Cookie: cookies[6]! } })).status, 200);
});

test('limiter prunes stale emails above the entry threshold and retains recent failures', () => {
  const limiter = new LoginLimiter(2);
  const start = new Date('2026-11-21T12:00:00Z');
  limiter.fail('stale', start);
  const now = new Date(start.getTime() + 900001);
  for (let i = 0; i < 5; i++) limiter.fail('recent', now);
  limiter.fail('new', now);
  const entries: unknown = Reflect.get(limiter, 'failures');
  assert.ok(entries instanceof Map);
  assert.equal(entries.size, 2);
  assert.equal(entries.has('stale'), false);
  assert.equal(limiter.check('recent', now), 900);
});
