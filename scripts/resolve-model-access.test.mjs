import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { test } from 'node:test';

const resolver = new URL('./resolve-model-access.mjs', import.meta.url);

test('expired ambient OIDC gives way to another ready credential', () => {
  const expiredOidc = 'header.eyJleHAiOjF9.signature';
  const anthropicKey = 'test-anthropic-credential';
  const stdout = execFileSync(process.execPath, [resolver.pathname, 'resolve'], {
    encoding: 'utf8',
    env: {
      ...process.env,
      AI_GATEWAY_API_KEY: '',
      VERCEL_OIDC_TOKEN: expiredOidc,
      ANTHROPIC_API_KEY: anthropicKey
    }
  });

  const resolution = JSON.parse(stdout);
  assert.equal(resolution.status, 'ready');
  assert.equal(resolution.source, 'anthropic-api-key');
  assert.deepEqual(Object.keys(resolution.environment), ['ANTHROPIC_API_KEY']);
  assert.equal(resolution.environment.ANTHROPIC_API_KEY === anthropicKey, true);
  assert.equal(stdout.includes(expiredOidc), false);
});
