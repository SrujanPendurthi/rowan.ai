import type http from 'node:http';
import { authRoutes } from './auth/routes.ts';
import { keysRoutes } from './keys/routes.ts';
import { proxyRoutes } from './proxy.ts';
import type pg from 'pg';
import type { Config } from './config.ts';
import { BadRequestError, checkOrigin, PayloadTooLargeError, sendError, sendJSON } from './http.ts';

export type Deps = { pool: pg.Pool; config: Config; now?: () => Date; fetch?: typeof fetch };

type Route = {
  method: string;
  pattern: RegExp;
  handler: (req: http.IncomingMessage, res: http.ServerResponse) => void | Promise<void>;
};

export function createApp(deps: Deps): http.RequestListener {
  const routes: Route[] = [
    ...authRoutes(deps),
    ...keysRoutes(deps),
    ...proxyRoutes(deps),
    { method: 'GET', pattern: /^\/healthz$/, handler: (_req, res) => sendJSON(res, 200, { ok: true }) },
  ];
  return (req, res) => {
    void (async () => {
      let path: string;
      try {
        path = new URL(req.url ?? '/', 'http://localhost').pathname;
      } catch {
        sendError(res, 404, 'not_found', 'not found');
        return;
      }
      if ((path === '/api' || path.startsWith('/api/')) && req.method !== 'GET' && req.method !== 'HEAD'
          && !checkOrigin(req, deps.config.webOrigin)) {
        sendError(res, 403, 'origin_forbidden', 'origin forbidden');
        return;
      }
      const route = routes.find((entry) => (entry.method === '*' || entry.method === req.method) && entry.pattern.test(path));
      if (route) await route.handler(req, res);
      else sendError(res, 404, 'not_found', 'not found');
    })().catch((error: unknown) => {
      if (res.headersSent) {
        console.error(error);
        res.destroy();
      } else if (error instanceof PayloadTooLargeError) {
        sendError(res, 413, 'payload_too_large', 'payload too large');
      } else if (error instanceof BadRequestError) {
        sendError(res, 400, 'invalid_request', error.message);
      } else {
        console.error(error);
        sendError(res, 500, 'internal', 'internal error');
      }
    });
  };
}
