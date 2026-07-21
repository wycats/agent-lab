import { expect, test, type Page } from '@playwright/test';

async function submit(page: Page, source: string): Promise<void> {
  const input = page.locator('[data-testid="terminal"] textarea');
  await input.pressSequentially(source, { delay: 2 });
  await input.press('Enter');
}

test('terminal session errors remain visible after the socket closes', async ({ page }) => {
  await page.routeWebSocket(/\/api\/terminal(?:\?|$)/, (socket) => {
    setTimeout(() => {
      socket.send(JSON.stringify({ type: 'error', message: 'run terminal is unavailable' }));
      socket.close();
    }, 50);
  });

  await page.goto('/');
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'error');
  await expect(page.getByRole('alert')).toContainText('run terminal is unavailable');
});

test('model access surfaces the provider blocking the shared selection', async ({ page }) => {
  await page.route(/\/api\/workbench\/[^/]+$/, async (route) => {
    const response = await route.fetch();
    const workbench = await response.json();
    workbench.modelAccess = [
      ...workbench.modelAccess,
      {
        id: 'blocking-provider',
        displayName: 'Blocking provider',
        harnessIds: ['eve'],
        status: 'needs-setup',
        source: null,
        expiresAtMs: null,
        message: 'Connect the second provider.',
        setupHint: 'Complete the second provider setup.'
      }
    ];
    await route.fulfill({ response, json: workbench });
  });

  await page.goto('/');
  const access = page.locator('.model-access-pill');
  await expect(access).toContainText('Connect');
  await expect(access).toHaveAttribute('title', 'Connect the second provider.');
  await page.getByLabel('Harness').selectOption('v0');
  await expect(page.getByRole('button', { name: 'Run harness', exact: true })).toBeEnabled();
  await expect(page.getByRole('button', { name: 'Compare v0 with Eve' })).toBeDisabled();
  await page.unrouteAll({ behavior: 'wait' });
});

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
  await expect(screen).toContainText('Agent Lab');
  await expect(screen).toContainText('Explore the active workspace');
  await expect(screen).toContainText('catalog list | where active');
  await expect(screen).toContainText('lab compare');
  await expect(screen).toContainText('agent-lab>');
  await expect(screen).toContainText('MCP namespaces: analysis, catalog');
  await expect(page.locator('.terminal-footer')).not.toContainText('local fixture');
  await expect(page.locator('.model-access-pill')).toContainText('Model access');
  await expect(page.locator('.model-access-pill')).toContainText('Ready');
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
  await submit(page, 'catalog list | where active | get name | str join ","');
  await expect(screen).toContainText('alpha,gamma');
  await submit(page, 'catalog list | analysis summarize | to json');
  await expect(screen).toContainText('"activeCount": 2');
  await expect(screen).toContainText('"totalScore": 11');

  await page.getByLabel('Harness').selectOption('v0');
  await expect(page.getByLabel('Harness')).toHaveValue('v0');
  await expect(page.getByLabel('Model')).toHaveValue('fixture');
  await page.getByRole('button', { name: 'Run harness', exact: true }).click();
  await expect(page.locator('.run-status')).toHaveText('passed');
  await expect(assembly).toContainText('agent-lab-fixture');
  await expect(assembly).toContainText('fixture/model');
  const review = page.getByTestId('run-review');
  await expect(review).toContainText('Driver process');
  await expect(review).toContainText('Adapter loaded');
  await expect(review).toContainText('Driver protocol ready');
  await expect(review).toContainText('Harness session ready');
  await expect(review).toContainText('catalog · list');
  await expect(review).toContainText('analysis · summarize');
  await expect(
    review.locator('.review-metrics div').filter({ hasText: 'Capabilities' }).locator('dd')
  ).toHaveText('2');
  await expect(review).toContainText('Created result.json');
  await expect(review).toContainText('Evaluation passed');
  await expect(review).toContainText('2 active items · total score 11');
  await expect(page.getByRole('button', { name: 'New workspace' })).toBeVisible();
  await page.getByRole('button', { name: 'Raw trace' }).click();
  await expect(page.getByRole('list', { name: 'Agent run events' })).toContainText('run · finished');
  await expect(page.getByRole('list', { name: 'Agent run events' })).toContainText('[REDACTED]');
  await expect(page.getByRole('list', { name: 'Agent run events' })).not.toContainText('Bearer 000000');
  await page.getByRole('button', { name: 'Review', exact: true }).click();
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');

  await page.getByRole('button', { name: 'Workspace', exact: true }).click();
  await expect(page.locator('.artifact')).toContainText('result.json');
  await expect(page.locator('.artifact')).toContainText('alpha');
  await expect(page.locator('.artifact')).toContainText('gamma');
  await page.getByRole('button', { name: 'Evidence' }).click();
  await expect(page.locator('.artifact')).toContainText('"passed": true');

  expect(socketUrls).toHaveLength(1);
  for (const url of socketUrls) expect(new URL(url).searchParams.has('token')).toBe(false);

  const completedRunId = await page.locator('.terminal-footer span').nth(1).textContent();
  await page.getByRole('button', { name: 'New workspace' }).click();
  await expect(page.locator('.run-status')).toHaveText('exploring');
  await expect(page.getByRole('button', { name: 'Run harness', exact: true })).toBeVisible();
  await expect(page.locator('.terminal-footer span').nth(1)).not.toHaveText(completedRunId ?? '');

  await page.reload();
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  const socketsBeforeReplay = socketUrls.length;
  const historyRun = page.locator('.history-list button').filter({ hasText: 'fixture/model' }).first();
  await expect(historyRun).toContainText('passed');
  await historyRun.click();
  await expect(page.getByTestId('assembly')).toContainText('agent-lab-fixture');
  await expect(page.getByTestId('assembly')).toContainText('catalog-v2');
  await expect(page.getByTestId('run-review')).toContainText('Evaluation passed');
  await expect(page.getByTestId('run-review')).toContainText('Created result.json');
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  expect(socketUrls).toHaveLength(socketsBeforeReplay);
  await page.getByRole('button', { name: 'Evidence' }).click();
  await expect(page.locator('.artifact')).toContainText('"passed": true');
});

test('a paired harness evaluation streams, compares, and reopens', async ({ page }) => {
  await page.goto('/');
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  await page.getByLabel('Harness').selectOption('v0');
  await expect(page.getByLabel('Harness')).toHaveValue('v0');
  await expect(page.getByLabel('Model')).toHaveValue('fixture');

  await page.reload();
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  await expect(page.getByLabel('Harness')).toHaveValue('v0');
  await expect(page.getByLabel('Model')).toHaveValue('fixture');

  const screen = page.getByTestId('terminal-text');
  await submit(page, 'lab assembly | get selection.modelProfileId');
  await expect(screen).toContainText('fixture');
  await submit(page, 'lab compare | get phase');
  const evaluation = page.getByTestId('evaluation-view');
  await expect(evaluation).toContainText('Behavioral comparison');
  await expect(evaluation.locator('.run-status')).toHaveText('passed');
  await expect(screen).toContainText('comparison-finished');
  await submit(page, 'lab evaluation | get summary.status');
  await expect(screen).toContainText('passed');
  const behavioralDiff = evaluation.getByTestId('behavioral-diff');
  await expect(behavioralDiff.locator('.arm-summary')).toHaveCount(2);
  await expect(behavioralDiff).toContainText('v0');
  await expect(behavioralDiff).toContainText('eve');
  await expect(behavioralDiff).toContainText('catalog · list');
  await expect(behavioralDiff).toContainText('analysis · summarize');
  await expect(behavioralDiff).toContainText('Created result.json');
  await expect(behavioralDiff.locator('.clock-axis')).toHaveCount(2);
  await expect.poll(() => behavioralDiff.locator('.phase-clock').count()).toBeGreaterThan(0);
  await expect(behavioralDiff.locator('.phase-clock').first()).toContainText('+');
  await expect.poll(() => behavioralDiff.locator('.clock-end-label').count()).toBeGreaterThan(2);
  await expect(behavioralDiff).toContainText('Driver process');
  await expect(behavioralDiff).toContainText('Adapter loaded');
  await expect(behavioralDiff).toContainText('Driver protocol ready');
  await expect(behavioralDiff).toContainText('Harness session ready');
  await expect(behavioralDiff.locator('.result-cell')).toHaveCount(2);
  await expect(behavioralDiff.locator('.result-cell').first()).toContainText('"activeCount": 2');
  await expect(behavioralDiff.locator('.result-cell').last()).toContainText('"activeCount": 2');
  await expect(evaluation.locator('.paired-result')).toContainText('Same evaluated artifact');
  await expect(evaluation.locator('.comparison-context')).toContainText('Same revision');
  await expect(evaluation.locator('.native-replays')).toContainText('Native replays and raw evidence');

  const exploreRunLabel = await page.locator('.terminal-footer span').nth(1).textContent();
  const screenBeforeInspection = await screen.textContent();
  await evaluation.locator('.native-replays').getByText('Native replays and raw evidence').click();
  await evaluation.locator('.native-replays').getByRole('button', { name: 'Open v0 replay' }).click();
  await expect(page.locator('.terminal-footer span').nth(1)).toHaveText(exploreRunLabel ?? '');
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  await submit(page, 'catalog list | where active | get name | str join ","');
  await expect.poll(() => screen.textContent()).not.toBe(screenBeforeInspection);
  await expect(screen).toContainText('alpha,gamma');

  const history = page.locator('.evaluation-history .history-list button').first();
  await expect(history).toContainText('v0 / eve');
  await expect(history).toContainText('passed');

  await page.reload();
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  const reopened = page.locator('.evaluation-history .history-list button').first();
  await expect(reopened).toContainText('passed');
  await reopened.click();
  await expect(page.getByTestId('evaluation-view').locator('.paired-result')).toContainText(
    'Same evaluated artifact'
  );
  await expect(page.getByTestId('behavioral-diff')).toContainText('analysis · summarize');
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
  await expect(screen).toContainText('Agent Lab');

  await expect(footer).toBeInViewport();
  expect(await page.evaluate(() => window.scrollY)).toBe(0);

  await page.setViewportSize({ width: 593, height: 406 });
  await page.getByRole('button', { name: 'Workspace', exact: true }).click();
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
  expect(contentSize.clientHeight).toBeGreaterThan(150);
  expect(contentSize.scrollHeight).toBeGreaterThan(contentSize.clientHeight);
  await content.hover();
  await page.mouse.wheel(0, 500);
  await expect.poll(() => content.evaluate((element) => element.scrollTop)).toBeGreaterThan(0);
  await expect(page.locator('.histories')).toBeInViewport();
});
