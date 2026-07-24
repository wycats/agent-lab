import { randomUUID } from 'node:crypto';
import { defineConfig } from '@playwright/test';

process.env.AGENT_LAB_WEB_E2E_RUN_ID ??= randomUUID();

export default defineConfig({
  testDir: './tests',
  timeout: 60_000,
  expect: { timeout: 12_000 },
  globalTeardown: './tests/global-teardown.mjs',
  use: {
    baseURL: 'http://127.0.0.1:4173',
    viewport: { width: 1440, height: 900 },
    trace: 'retain-on-failure'
  },
  reporter: [['list'], ['html', { open: 'never' }]],
  webServer: {
    command: 'node tests/run-e2e-server.mjs',
    url: 'http://127.0.0.1:4173',
    reuseExistingServer: false,
    timeout: 180_000
  }
});
