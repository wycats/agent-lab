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
  const assembly = page.getByTestId('assembly');
  await expect(assembly).toContainText('How does this harness discover and compose shared capabilities');
  await expect(assembly).toContainText('catalog-v2');
  await expect(assembly).toContainText('analysis-v1');
  await expect(assembly).toContainText('nushell + agent-mcp');

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
  await expect(assembly).toContainText('agent-lab-fixture');
  await expect(assembly).toContainText('fixture/model');
  const review = page.getByTestId('run-review');
  await expect(review).toContainText('Harness ready');
  await expect(review).toContainText('catalog · list');
  await expect(review).toContainText('analysis · summarize');
  await expect(review).toContainText('Created result.json');
  await expect(review).toContainText('Evaluation passed');
  await expect(review).toContainText('2 active items · total score 11');
  await page.getByRole('button', { name: 'Raw trace' }).click();
  await expect(page.getByRole('list', { name: 'Agent run events' })).toContainText('run · finished');
  await expect(page.getByRole('list', { name: 'Agent run events' })).toContainText('[REDACTED]');
  await expect(page.getByRole('list', { name: 'Agent run events' })).not.toContainText('Bearer 000000');
  await page.getByRole('button', { name: 'Review', exact: true }).click();
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
  await expect(page.getByTestId('assembly')).toContainText('agent-lab-fixture');
  await expect(page.getByTestId('assembly')).toContainText('catalog-v2');
  await expect(page.getByTestId('run-review')).toContainText('Evaluation passed');
  await expect(page.getByTestId('run-review')).toContainText('Created result.json');
  await page.getByRole('button', { name: 'Evidence' }).click();
  await expect(page.locator('.artifact')).toContainText('"passed": true');
});

test('stacked surfaces keep their scroll owners inside the viewport', async ({ page }) => {
  await page.setViewportSize({ width: 824, height: 639 });
  await page.goto('/');
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');

  const terminal = page.getByTestId('terminal');
  const screen = page.getByTestId('terminal-text');
  const footer = page.locator('.terminal-footer');
  await expect(terminal.locator('canvas')).toBeVisible();
  await expect(footer).toBeInViewport();

  await submit(page, 'help');
  await expect(screen).toContainText('You can also learn more');
  const bottomView = await screen.textContent();

  const canvas = await terminal.locator('canvas').boundingBox();
  expect(canvas).not.toBeNull();
  await page.mouse.move(canvas!.x + canvas!.width / 2, canvas!.y + canvas!.height / 2);
  await page.mouse.wheel(0, -800);
  await expect.poll(() => screen.textContent()).not.toBe(bottomView);
  await expect(screen).toContainText('Agent Lab visual shell');

  await expect(footer).toBeInViewport();
  expect(await page.evaluate(() => window.scrollY)).toBe(0);

  await page.setViewportSize({ width: 593, height: 406 });
  await page.getByRole('button', { name: 'Workspace' }).click();
  const runPanel = page.locator('.run-panel');
  await runPanel.evaluate((element) => element.scrollIntoView({ block: 'start' }));
  const runBounds = await runPanel.evaluate((element) => {
    const bounds = element.getBoundingClientRect();
    return { top: bounds.top, bottom: bounds.bottom, height: bounds.height };
  });
  expect(runBounds.top).toBeGreaterThanOrEqual(-1);
  expect(runBounds.bottom).toBeLessThanOrEqual(407);
  expect(runBounds.height).toBeLessThanOrEqual(407);

  const content = page.locator('.tab-content');
  const contentSize = await content.evaluate((element) => ({
    clientHeight: element.clientHeight,
    scrollHeight: element.scrollHeight
  }));
  expect(contentSize.clientHeight).toBeGreaterThan(180);
  expect(contentSize.scrollHeight).toBeGreaterThan(contentSize.clientHeight);
  await content.hover();
  await page.mouse.wheel(0, 500);
  await expect.poll(() => content.evaluate((element) => element.scrollTop)).toBeGreaterThan(0);
  await expect(page.locator('.history')).toBeInViewport();
});
