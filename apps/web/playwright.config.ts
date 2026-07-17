import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  timeout: 60_000,
  expect: { timeout: 12_000 },
  use: {
    baseURL: 'http://127.0.0.1:4173',
    viewport: { width: 1440, height: 900 },
    trace: 'retain-on-failure'
  },
  reporter: [['list'], ['html', { open: 'never' }]],
  webServer: {
    command:
      'cd ../.. && cargo build -p agent-lab-nushell-mcp -p agent-lab-driver-protocol -p agent-lab-web --bins && cargo run -p agent-lab-web -- --port 4173 --data .agent-lab/e2e-runs --model fixture/model',
    url: 'http://127.0.0.1:4173',
    reuseExistingServer: false,
    timeout: 180_000
  }
});
