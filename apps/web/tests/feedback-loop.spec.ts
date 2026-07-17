import { expect, test, type Page } from '@playwright/test';

async function submit(page: Page, source: string): Promise<void> {
  const input = page.locator('[data-testid="terminal"] textarea');
  await input.pressSequentially(source, { delay: 2 });
  await input.press('Enter');
}

test('a catalog run remains explorable and reopens from durable evidence', async ({ page }) => {
  const socketUrls: string[] = [];
  const terminalFrames: string[] = [];
  page.on('websocket', (socket) => {
    socketUrls.push(socket.url());
    socket.on('framereceived', ({ payload }) => {
      terminalFrames.push(typeof payload === 'string' ? payload : payload.toString('utf8'));
    });
  });

  await page.goto('/');
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  await expect(page.locator('[data-testid="terminal"] canvas')).toBeVisible();
  const screen = page.getByTestId('terminal-text');
  await expect(screen).toContainText('Agent Lab visual shell');
  await expect(screen).toContainText('agent-lab>');
  await expect(screen).toContainText('MCP namespaces: analysis, catalog');
  await expect(page.locator('.terminal-footer')).not.toContainText('local fixture');

  const input = page.locator('[data-testid="terminal"] textarea');
  await input.pressSequentially('mcp cat', { delay: 8 });
  await input.press('Tab');
  await expect(screen).toContainText('agent-lab> mcp catalog tools');
  await input.press('Enter');
  await expect(screen).toContainText('Return the controlled product catalog');

  const layout = await page.evaluate(() => {
    const bench = document.querySelector<HTMLElement>('.bench');
    const canvas = document.querySelector<HTMLCanvasElement>('[data-testid="terminal"] canvas');
    return {
      viewportHeight: window.innerHeight,
      bodyHeight: document.body.scrollHeight,
      benchHeight: bench?.getBoundingClientRect().height ?? 0,
      canvasHeight: canvas?.height ?? 0,
      devicePixelRatio: window.devicePixelRatio
    };
  });
  expect(layout.bodyHeight).toBeLessThanOrEqual(layout.viewportHeight + 2);
  expect(layout.benchHeight).toBeLessThan(layout.viewportHeight);
  expect(layout.canvasHeight).toBeLessThanOrEqual(layout.viewportHeight * layout.devicePixelRatio * 2);

  await page.getByLabel('Model ID').fill('fixture/model');
  await page.getByRole('button', { name: 'Run', exact: true }).click();
  await expect(page.locator('.run-status')).toHaveText('passed');
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  await expect(screen).toContainText('MCP namespaces: analysis, catalog');

  await input.focus();
  await expect(input).toBeFocused();
  terminalFrames.length = 0;
  await page.evaluate(() => {
    const output = document.querySelector('[data-testid="terminal-text"]');
    if (!output) throw new Error('terminal accessibility mirror was not mounted');
    const probe = window as typeof window & {
      __terminalMirrorObserver?: MutationObserver;
      __terminalMirrorUpdates?: number;
    };
    probe.__terminalMirrorUpdates = 0;
    probe.__terminalMirrorObserver = new MutationObserver(() => {
      probe.__terminalMirrorUpdates = (probe.__terminalMirrorUpdates ?? 0) + 1;
    });
    probe.__terminalMirrorObserver.observe(output, {
      childList: true,
      characterData: true,
      subtree: true
    });
  });
  await input.pressSequentially('catalog list', { delay: 8 });
  await expect
    .poll(() => /\u001b\[[0-9;]*38;2;/.test(terminalFrames.join('')), {
      timeout: 20_000
    })
    .toBe(true);
  await expect
    .poll(() =>
      page.evaluate(() => {
        const probe = window as typeof window & { __terminalMirrorUpdates?: number };
        return probe.__terminalMirrorUpdates ?? 0;
      })
    )
    .toBeGreaterThan(0);
  expect(
    await page.evaluate(() => {
      const probe = window as typeof window & {
        __terminalMirrorObserver?: MutationObserver;
        __terminalMirrorUpdates?: number;
      };
      probe.__terminalMirrorObserver?.disconnect();
      return probe.__terminalMirrorUpdates ?? 0;
    })
  ).toBeLessThanOrEqual(2);
  await input.press('Control+U');

  await submit(page, 'mcp analysis tools | to json');
  await expect(screen).toContainText('"name": "summarize"');
  await submit(page, 'help catalog list');
  await expect(screen).toContainText('Return the controlled product catalog');
  await submit(
    page,
    'catalog list | analysis summarize | to json'
  );
  await expect(screen).toContainText('"activeCount": 2');
  await expect(screen).toContainText('"totalScore": 11');

  await page.getByRole('button', { name: 'Workspace' }).click();
  await expect(page.locator('.artifact')).toContainText('result.json');
  await expect(page.locator('.artifact')).toContainText('alpha');
  await expect(page.locator('.artifact')).toContainText('gamma');
  await page.getByRole('button', { name: 'Evidence' }).click();
  await expect(page.locator('.artifact')).toContainText('"passed": true');

  expect(socketUrls).toHaveLength(1);
  for (const url of socketUrls) expect(new URL(url).searchParams.has('token')).toBe(false);

  await page.reload();
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  const historyRun = page.locator('.history-list button').filter({ hasText: 'fixture/model' }).first();
  await expect(historyRun).toContainText('passed');
  await historyRun.click();
  await page.getByRole('button', { name: 'Evidence' }).click();
  await expect(page.locator('.artifact')).toContainText('"passed": true');
});
