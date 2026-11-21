import { createHash, randomBytes } from 'node:crypto';

export function newToken(prefix: 'rk_sess_' | 'rk_live_'): string {
  return prefix + randomBytes(32).toString('base64url');
}

export function hashToken(token: string): Buffer {
  return createHash('sha256').update(token).digest();
}
