import { expect, test, type Page } from '@playwright/test';

async function submit(page: Page, source: string): Promise<void> {
  const input = page.locator('[data-testid="terminal"] textarea');
  await input.pressSequentially(source, { delay: 2 });
  await input.press('Enter');
}

function dimensions(text: string): { columns: number; rows: number } {
  const match = text.match(/(\d+)×(\d+)/);
  if (!match) throw new Error(`terminal dimensions not found in ${JSON.stringify(text)}`);
  return { columns: Number(match[1]), rows: Number(match[2]) };
}

test('the browser preserves the visual Nushell and MCP steel thread', async ({ page }) => {
  const socketUrls: string[] = [];
  page.on('websocket', (socket) => socketUrls.push(socket.url()));
  await page.goto('/');
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  expect(socketUrls).toHaveLength(1);
  expect(new URL(socketUrls[0]).searchParams.has('token')).toBe(false);
  await expect(page.getByRole('definition').filter({ hasText: 'fixture' })).toBeVisible();
  await expect(page.locator('[data-testid="terminal"] canvas')).toBeVisible();
  const screen = page.getByTestId('terminal-text');
  await expect(screen).toContainText('Agent Lab visual shell');
  await expect(screen).toContainText('agent-lab>');

  const startedEvent = page.locator('.events li').filter({ hasText: 'started' });
  const initialSize = dimensions(await startedEvent.innerText());
  const canvas = page.locator('[data-testid="terminal"] canvas');
  const initialCanvasWidth = await canvas.evaluate((element) => (element as HTMLCanvasElement).width);
  await page.setViewportSize({ width: 860, height: 900 });
  await expect
    .poll(() => canvas.evaluate((element) => (element as HTMLCanvasElement).width))
    .not.toBe(initialCanvasWidth);
  const resizedEvent = page.locator('.events li').filter({ hasText: 'resized' }).last();
  await expect
    .poll(async () => dimensions(await resizedEvent.innerText()).columns)
    .toBeLessThan(initialSize.columns);

  await submit(page, 'mut session_value = 41');
  await submit(page, '$session_value += 1; $session_value');
  await expect(screen).toContainText('42');

  await submit(page, "tool fixture catalog { probe: 'browser' } | get items | where active | get name");
  await expect(screen).toContainText('alpha');
  await expect(screen).toContainText('gamma');

  await submit(page, 'help tool fixture catalog');
  await expect(screen).toContainText('Return a nested structured catalog');

  await submit(page, 'tool fixture schedule_extra {}');
  await expect(screen).toContainText('[fixture capability change observed]');
  await submit(page, 'tool fixture extra {}');
  await expect(screen).toContainText('[capabilities refreshed: fixture]');
  await expect(screen).toContainText('available');

  await submit(page, 'tool fixture fail {}');
  await expect(screen).toContainText('MCP tool failed');
  await expect(screen).toContainText('intentional tool failure');

  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  await page.screenshot({ path: 'test-results/shared-perception.png', fullPage: true });
});
