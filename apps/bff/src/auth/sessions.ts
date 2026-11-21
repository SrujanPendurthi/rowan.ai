import type { IncomingMessage } from 'node:http';
import type pg from 'pg';
import type { Deps } from '../app.ts';
import { getCookie } from '../http.ts';
import { hashToken, newToken } from './tokens.ts';

export type Session = {
  keyId: string;
  userId: string;
  email: string;
  workspace: { id: string; name: string; tier: string };
};

export async function createSession(
  db: pg.Pool | pg.PoolClient, userId: string, workspaceId: string, now: Date,
): Promise<{ token: string; expiresAt: Date }> {
  const token = newToken('rk_sess_');
  const expiresAt = new Date(now.getTime() + 14 * 24 * 60 * 60 * 1000);
  await db.query(`INSERT INTO api_keys
    (workspace_id, user_id, key_hash, kind, label, monthly_spend_cap, rate_per_minute, expires_at, created_at)
    VALUES ($1, $2, $3, 'session', 'session', 9223372036854775807, 300, $4, $5)`,
  [workspaceId, userId, hashToken(token), expiresAt, now]);
  await db.query(`UPDATE api_keys SET revoked_at = $2
    WHERE id IN (
      SELECT id FROM api_keys
      WHERE user_id = $1 AND kind = 'session' AND revoked_at IS NULL AND expires_at > $2
      ORDER BY created_at DESC, id DESC OFFSET 5
    )`, [userId, now]);
  return { token, expiresAt };
}

export async function findSession(pool: pg.Pool, token: string | undefined, now: Date): Promise<Session | null> {
  if (!token) return null;
  const result = await pool.query<{
    key_id: string; user_id: string; email: string; workspace_id: string; name: string; tier: string;
  }>(`SELECT k.id AS key_id, u.id AS user_id, u.email, w.id AS workspace_id, w.name, w.tier
    FROM api_keys k JOIN users u ON u.id = k.user_id AND u.workspace_id = k.workspace_id
    JOIN workspaces w ON w.id = k.workspace_id
    WHERE k.key_hash = $1 AND k.kind = 'session' AND k.revoked_at IS NULL AND k.expires_at > $2`,
  [hashToken(token), now]);
  const row = result.rows[0];
  return row ? {
    keyId: row.key_id, userId: row.user_id, email: row.email,
    workspace: { id: row.workspace_id, name: row.name, tier: row.tier },
  } : null;
}

export function requireSession(req: IncomingMessage, deps: Deps): Promise<Session | null> {
  return findSession(deps.pool, getCookie(req, 'rowan_session'), (deps.now ?? (() => new Date()))());
}
