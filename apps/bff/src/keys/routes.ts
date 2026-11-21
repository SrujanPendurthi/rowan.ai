import type { IncomingMessage, ServerResponse } from 'node:http';
import { z } from 'zod';
import type { Deps } from '../app.ts';
import { requireSession } from '../auth/sessions.ts';
import { hashToken, newToken } from '../auth/tokens.ts';
import { parseJSON, readBody, sendError, sendJSON } from '../http.ts';

const createBody = z.object({
  label: z.string().trim().min(1).max(100),
  monthly_spend_cap: z.number().int().min(0).max(1_000_000),
  rate_per_minute: z.number().int().min(1).max(600).default(120),
});
const uuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i);
type KeyRow = {
  id: string;
  label: string;
  monthly_spend_cap: string;
  rate_per_minute: number;
  created_at: Date;
  revoked_at: Date | null;
};

function keyResponse(row: KeyRow) {
  return {
    id: row.id,
    label: row.label,
    // CLI-created keys may exceed the safe integer range; the API still returns a number.
    monthly_spend_cap: Number(row.monthly_spend_cap),
    rate_per_minute: row.rate_per_minute,
    created_at: row.created_at,
    revoked_at: row.revoked_at,
  };
}

export function keysRoutes(deps: Deps) {
  const now = deps.now ?? (() => new Date());
  return [
    {
      method: 'GET', pattern: /^\/api\/keys$/,
      async handler(req: IncomingMessage, res: ServerResponse): Promise<void> {
        const session = await requireSession(req, deps);
        if (!session) {
          sendError(res, 401, 'unauthorized', 'authentication required');
          return;
        }
        const result = await deps.pool.query<KeyRow>(
          `SELECT id, label, monthly_spend_cap, rate_per_minute, created_at, revoked_at
           FROM api_keys WHERE workspace_id = $1 AND kind = 'agent' ORDER BY created_at DESC, id`,
          [session.workspace.id],
        );
        sendJSON(res, 200, { keys: result.rows.map(keyResponse) });
      },
    },
    {
      method: 'POST', pattern: /^\/api\/keys$/,
      async handler(req: IncomingMessage, res: ServerResponse): Promise<void> {
        const session = await requireSession(req, deps);
        if (!session) {
          sendError(res, 401, 'unauthorized', 'authentication required');
          return;
        }
        const body = parseJSON(await readBody(req), createBody);
        const token = newToken('rk_live_');
        const result = await deps.pool.query<KeyRow>(
          `INSERT INTO api_keys
           (workspace_id, user_id, key_hash, kind, label, monthly_spend_cap, rate_per_minute, expires_at, created_at)
           VALUES ($1, $2, $3, 'agent', $4, $5, $6, NULL, $7)
           RETURNING id, label, monthly_spend_cap, rate_per_minute, created_at, revoked_at`,
          [session.workspace.id, session.userId, hashToken(token), body.label,
            body.monthly_spend_cap, body.rate_per_minute, now()],
        );
        sendJSON(res, 201, { key: keyResponse(result.rows[0]!), token });
      },
    },
    {
      method: 'DELETE', pattern: /^\/api\/keys\/[^/]+$/,
      async handler(req: IncomingMessage, res: ServerResponse): Promise<void> {
        const session = await requireSession(req, deps);
        if (!session) {
          sendError(res, 401, 'unauthorized', 'authentication required');
          return;
        }
        const path = new URL(req.url ?? '/', 'http://localhost').pathname;
        const id = uuid.safeParse(path.slice('/api/keys/'.length));
        if (!id.success) {
          sendError(res, 404, 'not_found', 'not found');
          return;
        }
        const result = await deps.pool.query(
          `UPDATE api_keys SET revoked_at = COALESCE(revoked_at, $1)
           WHERE id = $2 AND workspace_id = $3 AND kind = 'agent' RETURNING id`,
          [now(), id.data, session.workspace.id],
        );
        if (result.rowCount === 0) {
          sendError(res, 404, 'not_found', 'not found');
          return;
        }
        res.writeHead(204);
        res.end();
      },
    },
  ];
}
