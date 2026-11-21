import type { IncomingMessage, ServerResponse } from 'node:http';
import pg from 'pg';
import { z } from 'zod';
import type { Deps } from '../app.ts';
import { getCookie, parseJSON, readBody, sendError, sendJSON } from '../http.ts';
import { LoginLimiter } from './limiter.ts';
import { DUMMY_HASH, hashPassword, verifyPassword } from './password.ts';
import { createSession, requireSession } from './sessions.ts';
import { hashToken } from './tokens.ts';

const credentials = z.object({
  email: z.string().trim().toLowerCase().pipe(z.email().max(254)),
  password: z.string().min(12).max(256),
});
const signupBody = credentials.extend({ workspace_name: z.string().min(1).max(100) });
type Workspace = { id: string; name: string; tier: string };
type User = { id: string; email: string };

function cookie(token: string, secure: boolean, maxAge = 1209600): string {
  return `rowan_session=${token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${maxAge}${secure ? '; Secure' : ''}`;
}

export function authRoutes(deps: Deps) {
  const now = deps.now ?? (() => new Date());
  const limiter = new LoginLimiter();
  return [
    {
      method: 'POST', pattern: /^\/api\/auth\/signup$/,
      async handler(req: IncomingMessage, res: ServerResponse): Promise<void> {
        const body = parseJSON(await readBody(req), signupBody);
        const passwordHash = await hashPassword(body.password);
        const db = await deps.pool.connect();
        try {
          await db.query('BEGIN');
          const timestamp = now();
          const workspace = (await db.query<Workspace>(
            "INSERT INTO workspaces (name, tier, created_at) VALUES ($1, 'solo', $2) RETURNING id, name, tier",
            [body.workspace_name, timestamp],
          )).rows[0]!;
          const user = (await db.query<User>(
            'INSERT INTO users (workspace_id, email, password_hash, created_at) VALUES ($1, $2, $3, $4) RETURNING id, email',
            [workspace.id, body.email, passwordHash, timestamp],
          )).rows[0]!;
          const session = await createSession(db, user.id, workspace.id, timestamp);
          await db.query('COMMIT');
          sendJSON(res, 201, { user, workspace }, { 'Set-Cookie': cookie(session.token, deps.config.cookieSecure) });
        } catch (error: unknown) {
          await db.query('ROLLBACK');
          if (error instanceof pg.DatabaseError && error.code === '23505' && error.constraint === 'users_email_lower_idx') {
            sendError(res, 409, 'email_taken', 'email already taken');
          } else throw error;
        } finally {
          db.release();
        }
      },
    },
    {
      method: 'POST', pattern: /^\/api\/auth\/login$/,
      async handler(req: IncomingMessage, res: ServerResponse): Promise<void> {
        const body = parseJSON(await readBody(req), credentials);
        const wait = limiter.check(body.email, now());
        if (wait > 0) {
          sendError(res, 429, 'rate_limited', 'too many login attempts', { 'Retry-After': String(wait) });
          return;
        }
        const user = (await deps.pool.query<User & { password_hash: string; workspace_id: string; name: string; tier: string }>(
          `SELECT u.id, u.email, u.password_hash, u.workspace_id, w.name, w.tier
           FROM users u JOIN workspaces w ON w.id = u.workspace_id WHERE lower(u.email) = $1`, [body.email],
        )).rows[0];
        const valid = await verifyPassword(body.password, user?.password_hash ?? DUMMY_HASH);
        if (!valid || !user) {
          limiter.fail(body.email, now());
          sendError(res, 401, 'invalid_credentials', 'invalid email or password');
          return;
        }
        limiter.reset(body.email);
        const session = await createSession(deps.pool, user.id, user.workspace_id, now());
        sendJSON(res, 200, {
          user: { id: user.id, email: user.email },
          workspace: { id: user.workspace_id, name: user.name, tier: user.tier },
        }, { 'Set-Cookie': cookie(session.token, deps.config.cookieSecure) });
      },
    },
    {
      method: 'POST', pattern: /^\/api\/auth\/logout$/,
      async handler(req: IncomingMessage, res: ServerResponse): Promise<void> {
        const token = getCookie(req, 'rowan_session');
        if (token) await deps.pool.query(
          "UPDATE api_keys SET revoked_at = $1 WHERE key_hash = $2 AND kind = 'session' AND revoked_at IS NULL",
          [now(), hashToken(token)],
        );
        res.writeHead(204, { 'Set-Cookie': cookie('', deps.config.cookieSecure, 0) });
        res.end();
      },
    },
    {
      method: 'GET', pattern: /^\/api\/me$/,
      async handler(req: IncomingMessage, res: ServerResponse): Promise<void> {
        const session = await requireSession(req, deps);
        if (!session) {
          sendError(res, 401, 'unauthorized', 'authentication required');
          return;
        }
        sendJSON(res, 200, { user: { id: session.userId, email: session.email }, workspace: session.workspace });
      },
    },
  ];
}
