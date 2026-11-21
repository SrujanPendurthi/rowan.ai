import http from 'node:http';
import { once } from 'node:events';
import type { TestContext } from 'node:test';
import { createApp } from '../../src/app.ts';
import type { Deps } from '../../src/app.ts';

type RequestOptions = { body?: unknown; headers?: Record<string, string>; origin?: string | null };

export type Client = {
  request(method: string, path: string, opts?: RequestOptions): Promise<Response>;
};

export async function startApp(t: TestContext, deps: Deps): Promise<Client> {
  const server = http.createServer(createApp(deps));
  t.after(() => new Promise<void>((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve());
    server.closeAllConnections();
  }));
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('expected TCP address');
  const baseURL = `http://127.0.0.1:${address.port}`;
  const cookies = new Map<string, string>();
  return {
    async request(method, path, opts = {}) {
      const headers = new Headers();
      if (cookies.size) headers.set('Cookie', [...cookies].map(([name, value]) => `${name}=${value}`).join('; '));
      if (opts.origin !== null) headers.set('Origin', opts.origin ?? deps.config.webOrigin);
      if (opts.body !== undefined) headers.set('Content-Type', 'application/json');
      for (const [name, value] of Object.entries(opts.headers ?? {})) headers.set(name, value);
      const response = await fetch(new URL(path, baseURL), {
        method,
        headers,
        ...(opts.body !== undefined ? { body: typeof opts.body === 'string' ? opts.body
          : Buffer.isBuffer(opts.body) ? new Uint8Array(opts.body) : JSON.stringify(opts.body) } : {}),
        redirect: 'manual',
      });
      for (const cookie of response.headers.getSetCookie()) {
        const pair = cookie.split(';')[0] ?? '';
        const index = pair.indexOf('=');
        if (index < 1) continue;
        const name = pair.slice(0, index);
        const value = pair.slice(index + 1);
        if (/;\s*max-age=0(?:;|$)/i.test(cookie)) cookies.delete(name);
        else cookies.set(name, value);
      }
      return response;
    },
  };
}
