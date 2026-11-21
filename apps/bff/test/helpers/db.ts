import { randomBytes } from 'node:crypto';
import { readFile, readdir } from 'node:fs/promises';
import type { TestContext } from 'node:test';
import pg from 'pg';
import { z } from 'zod';

export async function newTestDB(t: TestContext): Promise<pg.Pool> {
  const env = z.object({ ROWAN_TEST_DATABASE_URL: z.url({ protocol: /^postgres(ql)?$/ }) }).parse(process.env);
  const admin = new pg.Pool({ connectionString: env.ROWAN_TEST_DATABASE_URL });
  const name = `rowan_bfftest_${randomBytes(8).toString('hex')}`;
  let created = false;
  let pool: pg.Pool | undefined;
  t.after(async () => {
    try {
      await pool?.end();
      // Only a generated hex suffix enters these quoted identifiers; SQL parameters cannot name databases.
      if (created) await admin.query(`DROP DATABASE "${name}" WITH (FORCE)`);
    } finally {
      await admin.end();
    }
  });
  await admin.query(`CREATE DATABASE "${name}"`);
  created = true;
  const url = new URL(env.ROWAN_TEST_DATABASE_URL);
  url.pathname = `/${name}`;
  pool = new pg.Pool({ connectionString: url.toString() });
  const directory = new URL('../../../../migrations/', import.meta.url);
  const files = (await readdir(directory)).filter((file) => file.endsWith('.sql')).sort();
  const client = await pool.connect();
  try {
    for (const file of files) {
      await client.query('BEGIN');
      try {
        await client.query('SELECT pg_advisory_xact_lock(726692413001)');
        await client.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
          version text PRIMARY KEY,
          applied_at timestamptz NOT NULL DEFAULT now()
        )`);
        const applied = await client.query<{ exists: boolean }>(
          'SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)', [file],
        );
        if (!applied.rows[0]?.exists) {
          await client.query(await readFile(new URL(file, directory), 'utf8'));
          await client.query('INSERT INTO schema_migrations (version, applied_at) VALUES ($1, now())', [file]);
        }
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      }
    }
  } finally {
    client.release();
  }
  return pool;
}
