import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
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

test('readiness probe recognizes a project link without resolving a token', () => {
  const workspace = mkdtempSync(join(tmpdir(), 'agent-lab-model-access-'));
  mkdirSync(join(workspace, '.vercel'));
  writeFileSync(
    join(workspace, '.vercel/project.json'),
    JSON.stringify({ projectId: 'project-id', orgId: 'team-id' })
  );
  try {
    const stdout = execFileSync(process.execPath, [resolver.pathname, 'probe'], {
      cwd: workspace,
      encoding: 'utf8',
      env: {
        PATH: process.env.PATH ?? '',
        AI_GATEWAY_API_KEY: '',
        VERCEL_OIDC_TOKEN: '',
        ANTHROPIC_API_KEY: ''
      }
    });
    const resolution = JSON.parse(stdout);
    assert.equal(resolution.status, 'ready');
    assert.equal(resolution.source, 'vercel-project-link');
    assert.equal('environment' in resolution, false);
  } finally {
    rmSync(workspace, { recursive: true, force: true });
  }
});
