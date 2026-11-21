import type { IncomingMessage, ServerResponse } from 'node:http';
import type { Deps } from './app.ts';
import { getCookie, readBody, sendError } from './http.ts';

const requestHeaders = ['content-type', 'idempotency-key', 'accept'];
const responseHeaders = ['content-type', 'content-disposition', 'retry-after', 'idempotent-replay', 'www-authenticate'];

export function proxyRoutes(deps: Deps) {
  return [{
    method: '*', pattern: /^\/api\/v1\//,
    async handler(req: IncomingMessage, res: ServerResponse): Promise<void> {
      const token = getCookie(req, 'rowan_session');
      if (!token) {
        sendError(res, 401, 'unauthorized', 'authentication required');
        return;
      }
      const headers = new Headers({ Authorization: `Bearer ${token}` });
      for (const name of requestHeaders) {
        const value = req.headers[name];
        if (typeof value === 'string') headers.set(name, value);
      }
      const method = req.method ?? 'GET';
      // Keep body errors outside the upstream catch so app.ts can map size errors to 413.
      const body = method === 'GET' || method === 'HEAD' ? undefined : new Uint8Array(await readBody(req));
      try {
        const upstream = await (deps.fetch ?? globalThis.fetch)(`${deps.config.coreUrl}${(req.url ?? '').slice('/api'.length)}`, {
          method, headers, body, signal: AbortSignal.timeout(30000), redirect: 'manual',
        });
        const bytes = Buffer.from(await upstream.arrayBuffer());
        const copied: Record<string, string> = {};
        for (const name of responseHeaders) {
          const value = upstream.headers.get(name);
          if (value !== null) copied[name] = value;
        }
        res.writeHead(upstream.status, copied);
        res.end(bytes);
      } catch (error: unknown) {
        console.error(error);
        sendError(res, 502, 'core_unavailable', 'core service unavailable');
      }
    },
  }];
}
