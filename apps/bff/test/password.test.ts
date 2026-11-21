import assert from 'node:assert/strict';
import test from 'node:test';
import { DUMMY_HASH, hashPassword, verifyPassword } from '../src/auth/password.ts';

test('scrypt hashes use the specified parameters, random salts, and safe verification', async () => {
  const password = 'correct horse battery staple';
  const stored = await hashPassword(password);
  assert.equal(await verifyPassword(password, stored), true);
  assert.equal(await verifyPassword('wrong password', stored), false);
  assert.equal(stored.split('$').length, 6);
  assert.ok(stored.startsWith('scrypt$131072$8$1$'));
  assert.notEqual(await hashPassword(password), stored);
  for (const malformed of ['garbage', stored.replace('131072', '2'), `${stored}$extra`, 'scrypt$131072$8$1$$']) {
    assert.equal(await verifyPassword('x', malformed), false);
  }
  assert.ok(DUMMY_HASH.startsWith('scrypt$131072$8$1$'));
  assert.equal(await verifyPassword('wrong password', DUMMY_HASH), false);
});
