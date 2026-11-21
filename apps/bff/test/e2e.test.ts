import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import net from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { z } from 'zod';
import { loadConfig } from '../src/config.ts';
import {
  BuyBoxSchema, CompanySchema, CreditsSchema, DiscoveryResultSchema, EnrichmentRunSchema,
  ErrorBodySchema, EvaluationsSchema, QuoteSchema,
  ReviewQueueSchema,
} from '../src/core/schemas.ts';
import type { EnrichmentRun } from '../src/core/schemas.ts';
import { newTestDB } from './helpers/db.ts';
import { startApp } from './helpers/server.ts';

const execute = promisify(execFile);
const root = fileURLToPath(new URL('../../../', import.meta.url));
const accountSchema = z.object({
  user: z.object({ id: z.uuid(), email: z.email() }),
  workspace: z.object({ id: z.uuid(), name: z.string(), tier: z.string() }),
});
const agentSchema = z.object({
  key: z.object({
    id: z.uuid(), label: z.string(), monthly_spend_cap: z.number(), rate_per_minute: z.number(),
    created_at: z.iso.datetime(), revoked_at: z.iso.datetime().nullable(),
  }),
  token: z.string().regex(/^rk_live_/),
});

async function freePort(): Promise<number> {
  const server = net.createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  return address.port;
}

test('browser session completes the core loop and revocation applies directly at core', { timeout: 120_000 }, async (t) => {
  const directory = await mkdtemp(join(tmpdir(), 'rowan-bff-e2e-'));
  let core: ReturnType<typeof spawn> | undefined;
  let logs = '';
  let spawnError: Error | undefined;
  // Register before the DB helper so the core stops before its database is dropped.
  t.after(async () => {
    try {
      if (core && core.exitCode === null && core.signalCode === null && !spawnError) {
        const exited = once(core, 'exit');
        core.kill('SIGTERM');
        const force = setTimeout(() => core?.kill('SIGKILL'), 5000);
        try { await exited; } finally { clearTimeout(force); }
      }
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
  const pool = await newTestDB(t);
  const databaseUrl = z.string().parse(pool.options.connectionString);
  const env = { ...process.env, DATABASE_URL: databaseUrl };
  for (const command of ['core', 'rowanctl']) {
    await execute('go', ['build', '-o', join(directory, command), `./services/core/cmd/${command}`],
      { cwd: root, env, timeout: 60_000, signal: t.signal });
  }
  const port = await freePort();
  const coreUrl = `http://127.0.0.1:${port}`;
  core = spawn(join(directory, 'core'), [], {
    cwd: root, env: { ...env, ROWAN_PROVIDER: 'fake', PORT: String(port), ROWAN_ARTIFACT_DIR: directory },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  core.on('error', (error: Error) => { spawnError = error; });
  core.stdout?.on('data', (chunk: Buffer) => { logs = (logs + chunk.toString()).slice(-8192); });
  core.stderr?.on('data', (chunk: Buffer) => { logs = (logs + chunk.toString()).slice(-8192); });
  const deadline = Date.now() + 30_000;
  while (true) {
    assert.ifError(spawnError);
    assert.equal(core.exitCode, null, logs);
    try {
      const response = await fetch(`${coreUrl}/v1/credits`, { signal: AbortSignal.timeout(1000) });
      const body: unknown = await response.json();
      if (response.status === 401) {
        ErrorBodySchema.parse(body);
        break;
      }
    } catch { /* The listener may not be ready yet. */ }
    assert.ok(Date.now() < deadline, `core did not become ready: ${logs}`);
    await delay(50, undefined, { signal: t.signal });
  }
  const config = loadConfig({ DATABASE_URL: databaseUrl, ROWAN_CORE_URL: coreUrl,
    ROWAN_WEB_ORIGIN: 'http://localhost:3000', ROWAN_COOKIE_SECURE: 'false' });
  const client = await startApp(t, { pool, config });
  let workspaceId = '';
  let sessionToken = '';
  let buyBoxId = '';
  let run: EnrichmentRun | undefined;
  let runBody: { buy_box_id: string; candidates: typeof candidates };
  let creditsAfter = 0;
  const candidates = [
    { ref: 'cached', legal_name: 'Zephyr Plumbing LLC', state: 'OH' },
    { ref: 'zero', legal_name: 'Orion Logistics Corp', state: 'OH' },
  ];
  async function success<T>(response: Response, schema: z.ZodType<T>, status = 200): Promise<T> {
    const body: unknown = await response.json();
    assert.equal(response.status, status, JSON.stringify(body));
    return schema.parse(body);
  }
  async function directCredits(token: string) {
    return fetch(`${coreUrl}/v1/credits`, { headers: { Authorization: `Bearer ${token}` }, signal: t.signal });
  }

  await t.test('signup and top up a workspace with zero initial credits', async () => {
    const response = await client.request('POST', '/api/auth/signup', {
      body: { email: 'founder@example.com', password: 'correct horse battery staple', workspace_name: 'E2E' },
    });
    const account = await success(response, accountSchema, 201);
    workspaceId = account.workspace.id;
    const cookie = response.headers.getSetCookie().find((value) => value.startsWith('rowan_session='));
    assert.ok(cookie);
    sessionToken = cookie.split(';')[0]!.slice('rowan_session='.length);
    assert.equal((await success(await client.request('GET', '/api/v1/credits'), CreditsSchema)).total, 0);
    await execute(join(directory, 'rowanctl'), ['credits', 'topup', '--workspace', workspaceId, '--credits', '100'],
      { cwd: root, env, timeout: 10_000, signal: t.signal });
    assert.equal((await success(await client.request('GET', '/api/v1/credits'), CreditsSchema)).total, 100);

    // The standalone fake provider has no configured hits. Seed its cache using the Go
    // SourceRecordInput wire shape so the real resolver/evaluator can produce a company.
    const now = new Date().toISOString();
    const payload = JSON.stringify([{
      SourceSystem: 'fake', ExternalRef: 'e2e-company', Facts: { LegalName: candidates[0]!.legal_name, State: 'OH' },
      FetchedAt: now, Citations: [
        { field_name: 'state', field_value: 'OH', data_quality_status: 'verified_filing', export_policy: 'allow' },
        { field_name: 'revenue_cents', field_value: 300000000, data_quality_status: 'commercial_estimate', export_policy: 'deal_team_plus' },
      ].map((citation) => ({ ...citation, origin_type: 'raw_source', source_system: 'fake', source_ref: 'e2e', retrieved_at: now })),
    }]);
    const identityHash = createHash('sha256').update('zephyr plumbing|OH|').digest('hex');
    await pool.query(`INSERT INTO source_fetch_cache(provider, cache_key, content_hash, payload, zero_hit, fetched_at)
      VALUES ($1, $2, $3, $4, $5, $6)`,
    ['fake', `${workspaceId}:${identityHash}`, createHash('sha256').update(payload).digest(), payload, false, now]);
  });
  await t.test('buy box, discovery, quote, and enrichment use the typed contract', async () => {
    const buyBox = await success(await client.request('POST', '/api/v1/buy-boxes', {
      headers: { 'Idempotency-Key': 'bb-1' },
      body: { name: 'OH HVAC', criteria: { states_in: ['OH'], revenue_min_cents: 200000000 } },
    }), BuyBoxSchema, 201);
    buyBoxId = buyBox.id;
    const discovery = await success(await client.request('POST', '/api/v1/discovery/search', {
      body: { buy_box_id: buyBoxId },
    }), DiscoveryResultSchema);
    assert.deepEqual(discovery.results, []);
    const quote = await success(await client.request('POST', '/api/v1/enrichment/quote', { body: { candidates } }), QuoteSchema);
    assert.equal(quote.credits_required, 1);
    runBody = { buy_box_id: buyBoxId, candidates };
    run = await success(await client.request('POST', '/api/v1/enrichment/run', {
      body: runBody, headers: { 'Idempotency-Key': 'run-1' },
    }), EnrichmentRunSchema);
    assert.equal(run.status, 'completed');
    assert.equal(run.cached, 1);
    assert.equal(run.zero_hits, 1);
    assert.equal(run.credits_committed, 0);
    assert.equal(run.credits_refunded, 1);
    assert.equal(run.companies.length, 1);
  });
  await t.test('company, evaluations, review queue, credits, and CSV export', async () => {
    assert.ok(run);
    const company = await success(await client.request('GET', `/api/v1/companies/${run.companies[0]}`), CompanySchema);
    assert.equal(company.citations.length, 2);
    const evaluations = await success(await client.request('GET', `/api/v1/buy-boxes/${buyBoxId}/evaluations`), EvaluationsSchema);
    assert.equal(evaluations.evaluations.length, 1);
    const revenue = evaluations.evaluations[0]!.criteria_breakdown.find((criterion) => criterion.criterion === 'revenue_min_cents');
    assert.equal(revenue?.status, 'UNVERIFIED_LOW_CONFIDENCE');
    assert.equal(revenue?.data_quality_status, 'commercial_estimate');
    assert.equal(revenue?.observed, '[REDACTED]');
    await success(await client.request('GET', '/api/v1/review-queue'), ReviewQueueSchema);
    creditsAfter = (await success(await client.request('GET', '/api/v1/credits'), CreditsSchema)).total;
    // Fake lookups are zero-hit and cached hits are free; the reserved credit must be refunded.
    assert.equal(creditsAfter, 100);
    const exported = await client.request('POST', '/api/v1/exports', {
      body: { buy_box_id: buyBoxId, columns: ['state', 'revenue_cents'] }, headers: { 'Idempotency-Key': 'export-1' },
    });
    assert.equal(exported.status, 200);
    assert.equal(exported.headers.get('Content-Type'), 'text/csv; charset=utf-8');
    // The core currently does not send Content-Disposition.
    assert.equal(exported.headers.get('Content-Disposition'), null);
    const csv = await exported.text();
    assert.match(csv, /revenue_cents__data_quality_status/);
    assert.match(csv, /commercial_estimate/);
    assert.match(csv, /\[REDACTED\]/);
    assert.doesNotMatch(csv, /300000000/);
  });
  await t.test('same enrichment key and body replay without changing credits', async () => {
    const replay = await client.request('POST', '/api/v1/enrichment/run', {
      body: runBody, headers: { 'Idempotency-Key': 'run-1' },
    });
    assert.equal(replay.headers.get('Idempotent-Replay'), 'true');
    assert.deepEqual(await success(replay, EnrichmentRunSchema), run);
    assert.equal((await success(await client.request('GET', '/api/v1/credits'), CreditsSchema)).total, creditsAfter);
  });
  await t.test('agent key creation and revocation apply directly at core', async () => {
    const agent = await success(await client.request('POST', '/api/keys', {
      body: { label: 'e2e-agent', monthly_spend_cap: 100 },
    }), agentSchema, 201);
    await success(await directCredits(agent.token), CreditsSchema);
    assert.equal((await client.request('DELETE', `/api/keys/${agent.key.id}`)).status, 204);
    const denied = await directCredits(agent.token);
    assert.equal(denied.status, 401);
    assert.equal(ErrorBodySchema.parse(await denied.json()).error.code, 'unauthorized');
  });
  await t.test('logout revokes the captured session token directly at core', async () => {
    await success(await directCredits(sessionToken), CreditsSchema);
    assert.equal((await client.request('POST', '/api/auth/logout')).status, 204);
    const denied = await directCredits(sessionToken);
    assert.equal(denied.status, 401);
    assert.equal(ErrorBodySchema.parse(await denied.json()).error.code, 'unauthorized');
  });
});
