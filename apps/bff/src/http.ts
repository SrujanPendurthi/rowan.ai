import type { IncomingMessage, ServerResponse } from 'node:http';
import type { z } from 'zod';

export class PayloadTooLargeError extends Error {
  constructor() {
    super('payload too large');
    this.name = 'PayloadTooLargeError';
  }
}

export class BadRequestError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BadRequestError';
  }
}

export function sendJSON(res: ServerResponse, status: number, body: unknown, headers?: Record<string, string>): void {
  res.writeHead(status, { 'Content-Type': 'application/json', ...headers, 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(body));
}

export function sendError(res: ServerResponse, status: number, code: string, message: string, headers?: Record<string, string>): void {
  sendJSON(res, status, { error: { code, message } }, headers);
}

export async function readBody(req: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let size = 0;
  // Keep the socket alive after a size rejection so the caller can send a 413.
  for await (const chunk of req.iterator({ destroyOnReturn: false })) {
    const value: unknown = chunk;
    if (!Buffer.isBuffer(value)) throw new BadRequestError('invalid request body');
    size += value.length;
    if (size > 1_048_576) {
      req.resume();
      throw new PayloadTooLargeError();
    }
    chunks.push(value);
  }
  return Buffer.concat(chunks, size);
}

export function parseJSON<T>(body: Buffer, schema: z.ZodType<T>): T {
  let value: unknown;
  try {
    value = JSON.parse(body.toString('utf8'));
  } catch {
    throw new BadRequestError('invalid JSON');
  }
  const result = schema.safeParse(value);
  if (!result.success) {
    const issue = result.error.issues[0];
    throw new BadRequestError(issue ? `${issue.path.join('.')}: ${issue.message}` : 'invalid request');
  }
  return result.data;
}

export function getCookie(req: IncomingMessage, name: string): string | undefined {
  for (const cookie of req.headers.cookie?.split(';') ?? []) {
    const index = cookie.indexOf('=');
    if (index >= 0 && cookie.slice(0, index).trim() === name) return cookie.slice(index + 1).trim();
  }
  return undefined;
}

export function checkOrigin(req: IncomingMessage, webOrigin: string): boolean {
  return req.headers.origin === webOrigin;
}
