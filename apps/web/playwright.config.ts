import { randomUUID } from 'node:crypto';
import { defineConfig } from '@playwright/test';

process.env.AGENT_LAB_WEB_E2E_RUN_ID ??= randomUUID();
process.env.AGENT_LAB_WEB_E2E_PORT ??= String(
  20_000 + (Number.parseInt(process.env.AGENT_LAB_WEB_E2E_RUN_ID.slice(0, 8), 16) % 20_000)
);
const port = Number(process.env.AGENT_LAB_WEB_E2E_PORT);
if (!Number.isSafeInteger(port) || port < 1024 || port > 65_535) {
  throw new Error('AGENT_LAB_WEB_E2E_PORT must be a valid unprivileged TCP port');
}
const baseURL = `http://127.0.0.1:${port}`;

export default defineConfig({
  testDir: './tests',
  timeout: 60_000,
  expect: { timeout: 12_000 },
  globalTeardown: './tests/global-teardown.mjs',
  use: {
    baseURL,
    viewport: { width: 1440, height: 900 },
    trace: 'retain-on-failure'
  },
  reporter: [['list'], ['html', { open: 'never' }]],
  webServer: {
    command: 'node tests/run-e2e-server.mjs',
    url: baseURL,
    reuseExistingServer: false,
    timeout: 180_000
  }
});
