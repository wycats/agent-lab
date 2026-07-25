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
  await page.getByLabel('Default harness').selectOption('v0');
  await expect(page.getByRole('button', { name: 'Run harness', exact: true })).toBeEnabled();
  await expect(page.getByRole('button', { name: 'Compare v0 with Eve' })).toBeDisabled();
  await page.unrouteAll({ behavior: 'wait' });
});

test('an interrupted replay selection reopens durable rendered and source evidence', async ({ page }) => {
  let replayStreamRequests = 0;
  page.on('request', (request) => {
    if (/\/agent-sessions\/replay-session\/events$/.test(new URL(request.url()).pathname)) {
      replayStreamRequests += 1;
    }
  });
  const summary = {
    id: 'replay-session',
    workspaceId: '',
    harnessId: 'v0',
    modelProfileId: 'fixture',
    modelId: 'fixture/v0',
    status: 'interrupted',
    active: false,
    createdAtMs: 1,
    updatedAtMs: 3,
    turnCount: 1,
    error: 'the server restarted; start a new agent session to continue'
  };
  await page.route(/\/api\/workbench\/[^/]+$/, async (route) => {
    const response = await route.fetch();
    const workbench = await response.json();
    summary.workspaceId = workbench.workspaceId;
    workbench.activeAgentSession = null;
    workbench.replayAgentSession = summary;
    workbench.agentSessions = [summary];
    await route.fulfill({ response, json: workbench });
  });
  await page.route(/\/api\/workbench\/[^/]+\/agent-sessions\/replay-session$/, async (route) => {
    await route.fulfill({
      json: {
        projectionVersion: 2,
        summary,
        turns: [{
          id: 'replay-turn',
          sessionId: summary.id,
          prompt: 'Markdown report',
          sourceRevision: 'sha256:replay',
          capabilityRevisions: {},
          status: 'completed',
          startedAtMs: 1,
          finishedAtMs: 2,
          outcome: 'completed',
          presentation: {
            schemaVersion: 2,
            response: '# Durable answer\n\n**Reopened** after restart.',
            messages: [{
              id: 'replay-message',
              text: '# Durable answer\n\n**Reopened** after restart.',
              complete: true,
              sourceEventSequences: [1]
            }],
            activity: [{
              kind: 'capability-call',
              title: 'catalog · list',
              detail: null,
              status: 'completed',
              source: 'catalog',
              path: null,
              operation: 'list',
              callId: 'call-1',
              arguments: {},
              result: {
                items: [
                  { name: 'alpha', score: 3 },
                  { name: 'beta', score: 5 },
                  { name: 'gamma', score: 8 }
                ]
              },
              sourceEventSequences: [2, 3]
            }],
            usage: null,
            completeness: {
              assistantOutput: 'complete',
              capabilityActivity: 'complete',
              nativeActivity: 'complete',
              workspaceEffects: 'complete',
              usage: 'unavailable'
            },
            sourceEventSequences: [1],
            sourceDigest: 'sha256:replay-answer'
          }
        }],
        events: []
      }
    });
  });
  await page.goto('/');
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  const session = page.getByTestId('interactive-agent-session');
  await expect(page.locator('.run-heading')).toContainText('Session replay');
  await expect(page.locator('.run-heading')).toContainText('interrupted');
  await expect(session).toContainText('Reopened from durable evidence. Start a new agent session to continue.');
  await expect(session.getByRole('heading', { name: 'Durable answer', level: 3 })).toBeVisible();
  const activity = session.getByLabel('Turn activity');
  await expect(activity).toContainText('catalog · list');
  await expect(activity).toContainText('Returned 3 items');
  await expect(activity).not.toContainText('{"items"');
  const presentation = session.getByRole('group', { name: 'Agent answer presentation' });
  await presentation.getByRole('button', { name: 'Source' }).click();
  await expect(session.locator('.response-source')).toContainText('# Durable answer');
  await expect(session.getByTestId('agent-live-status')).toHaveCount(0);
  await expect(session.getByRole('button', { name: 'Cancel turn' })).toHaveCount(0);
  await page.waitForTimeout(100);
  expect(replayStreamRequests).toBe(0);
  await page.unrouteAll({ behavior: 'wait' });
});

test('a session that fails while opening becomes retained history', async ({ page }) => {
  let failedStreamRequests = 0;
  const startingSummary = {
    id: 'failed-session',
    workspaceId: '',
    harnessId: 'v0',
    modelProfileId: 'fixture',
    modelId: 'fixture/v0',
    status: 'starting',
    active: true,
    createdAtMs: 1,
    updatedAtMs: 1,
    turnCount: 0
  };
  const failedSummary = {
    ...startingSummary,
    status: 'failed',
    active: false,
    updatedAtMs: 2,
    error: 'fixture driver could not open the session'
  };
  page.on('request', (request) => {
    if (/\/agent-sessions\/failed-session\/events$/.test(new URL(request.url()).pathname)) {
      failedStreamRequests += 1;
    }
  });
  await page.route(/\/api\/workbench\/[^/]+$/, async (route) => {
    const response = await route.fetch();
    const workbench = await response.json();
    startingSummary.workspaceId = workbench.workspaceId;
    failedSummary.workspaceId = workbench.workspaceId;
    workbench.activeAgentSession = startingSummary;
    workbench.replayAgentSession = null;
    workbench.agentSessions = [failedSummary];
    await route.fulfill({ response, json: workbench });
  });
  await page.route(/\/api\/workbench\/[^/]+\/agent-sessions\/failed-session$/, async (route) => {
    await route.fulfill({
      json: {
        projectionVersion: 1,
        summary: failedSummary,
        turns: [],
        events: []
      }
    });
  });

  await page.goto('/');
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  await expect(page.locator('.run-heading')).toContainText('Session history');
  await expect(page.locator('.run-heading')).toContainText('failed');
  await expect(page.getByTestId('interactive-agent-session')).toContainText(
    'Retained as durable evidence. Start a new agent session to continue.'
  );
  await page.waitForTimeout(100);
  expect(failedStreamRequests).toBe(0);
  await page.unrouteAll({ behavior: 'wait' });
});

test('activating an already displayed session refreshes its ready lifecycle', async ({ page }) => {
  const sessionId = 'activating-session';
  let workspaceId = '';
  let activated = false;
  let sessionDetailRequests = 0;

  await page.addInitScript(() => {
    const nativeFetch = window.fetch.bind(window);
    window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
      const requestUrl = new URL(
        typeof input === 'string'
          ? input
          : input instanceof URL
            ? input.href
            : input.url,
        window.location.href
      );
      if (/^\/api\/runs\/[^/]+\/events$/.test(requestUrl.pathname)) {
        const encoder = new TextEncoder();
        let closed = false;
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            (window as Window & { __publishWorkspaceAgentEvent?: (event: unknown) => void })
              .__publishWorkspaceAgentEvent = (event) => {
                if (closed || init?.signal?.aborted) return;
                controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
              };
            init?.signal?.addEventListener('abort', () => {
              if (closed) return;
              closed = true;
              try {
                controller.close();
              } catch {
                // The stream may already have been closed by navigation.
              }
            }, { once: true });
          },
          cancel() {
            closed = true;
          }
        });
        return new Response(stream, {
          status: 200,
          headers: { 'Content-Type': 'text/event-stream' }
        });
      }
      return nativeFetch(input, init);
    };
  });

  await page.route(/\/api\/workbench\/[^/]+$/, async (route) => {
    const response = await route.fetch();
    const workbench = await response.json();
    workspaceId = workbench.workspaceId;
    workbench.activeAgentSession = null;
    workbench.replayAgentSession = null;
    workbench.agentSessions = [];
    await route.fulfill({ response, json: workbench });
  });
  await page.route(
    new RegExp(`/api/workbench/[^/]+/agent-sessions/${sessionId}$`),
    async (route) => {
      sessionDetailRequests += 1;
      await route.fulfill({
        json: {
          projectionVersion: 1,
          summary: {
            id: sessionId,
            workspaceId,
            harnessId: 'v0',
            modelProfileId: 'fixture',
            modelId: 'fixture/v0',
            status: activated ? 'ready' : 'starting',
            active: activated,
            createdAtMs: 1,
            updatedAtMs: activated ? 3 : 2,
            turnCount: 0
          },
          turns: [],
          events: []
        }
      });
    }
  );
  await page.route(
    new RegExp(`/api/workbench/[^/]+/agent-sessions/${sessionId}/events$`),
    async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'text/event-stream',
        body: ''
      });
    }
  );

  await page.goto('/');
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  await expect.poll(() =>
    page.evaluate(() =>
      typeof (window as Window & { __publishWorkspaceAgentEvent?: unknown })
        .__publishWorkspaceAgentEvent
    )
  ).toBe('function');

  await page.evaluate(({ sessionId }) => {
    (window as Window & { __publishWorkspaceAgentEvent?: (event: unknown) => void })
      .__publishWorkspaceAgentEvent?.({
        sequence: 1_000_000,
        atMs: 2,
        type: 'workbench.agent.session.started',
        payload: { sessionId, origin: 'nushell' }
      });
  }, { sessionId });

  const session = page.getByTestId('interactive-agent-session');
  await expect(session).toBeVisible();
  await expect(page.locator('.run-heading')).toContainText('Agent session');
  await expect(page.locator('.run-heading')).toContainText('starting');
  await expect(session).toContainText('This session is getting ready.');
  await expect.poll(() => sessionDetailRequests).toBe(1);

  activated = true;
  await page.evaluate(({ sessionId }) => {
    (window as Window & { __publishWorkspaceAgentEvent?: (event: unknown) => void })
      .__publishWorkspaceAgentEvent?.({
        sequence: 1_000_001,
        atMs: 3,
        type: 'workbench.agent.session.activated',
        payload: { sessionId, origin: 'nushell' }
      });
  }, { sessionId });

  await expect.poll(() => sessionDetailRequests).toBe(2);
  await expect(page.locator('.run-heading')).toContainText('Active session');
  await expect(page.locator('.run-heading')).toContainText('ready');
  await expect(session).toContainText('Ask the harness in Explore.');
  await expect(session.getByTestId('session-turn')).toHaveCount(0);
  await page.unrouteAll({ behavior: 'wait' });
});

test('a sustained agent stream stays incremental while run inspection remains independently navigable', async ({ page }) => {
  const sessionId = 'stream-session';
  const turnId = 'stream-turn';
  const messageId = 'stream-message';
  const startedAtMs = Date.now() - 1_500;
  const chunks = Array.from({ length: 96 }, (_, index) => `${index + 1} `);
  const completedText = `# Stream complete\n\n${chunks.join('')}`;
  const streamEvents: Array<{
    sequence: number;
    atMs: number;
    type: string;
    payload: Record<string, unknown>;
    progress?: {
      phase: 'reasoning' | 'responding' | 'finalizing';
      detail: string;
      source: string;
      sourceEventSequence: number;
      sourceEventType: string;
    };
  }> = [{
    sequence: 1,
    atMs: startedAtMs,
    type: 'agent.turn.started',
    payload: {
      sessionId,
      turnId,
      prompt: 'Stream a long answer',
      input: null
    },
    progress: {
      phase: 'reasoning',
      detail: 'Reading the shared workspace.',
      source: 'v0',
      sourceEventSequence: 1,
      sourceEventType: 'agent.turn.started'
    }
  }, ...chunks.map((text, index) => ({
    sequence: index + 2,
    atMs: startedAtMs + index + 1,
    type: 'observation.assistant.delta',
    payload: {
      sessionId,
      turnId,
      event: { messageId, text: index === 0 ? '# Stream complete\n\n1 ' : text }
    },
    progress: {
      phase: 'responding' as const,
      detail: 'Streaming the answer.',
      source: 'assistant',
      sourceEventSequence: index + 2,
      sourceEventType: 'observation.assistant.delta'
    }
  }))];
  streamEvents.push({
    sequence: streamEvents.length + 1,
    atMs: startedAtMs + streamEvents.length + 1,
    type: 'observation.assistant.completed',
    payload: { sessionId, turnId, event: { messageId, text: completedText } },
    progress: {
      phase: 'finalizing',
      detail: 'Finalizing durable evidence.',
      source: 'assistant',
      sourceEventSequence: streamEvents.length + 1,
      sourceEventType: 'observation.assistant.completed'
    }
  });
  streamEvents.push({
    sequence: streamEvents.length + 1,
    atMs: startedAtMs + streamEvents.length + 1,
    type: 'agent.turn.finished',
    payload: { sessionId, turnId, outcome: 'completed' },
    progress: {
      phase: 'finalizing',
      detail: 'Durable evidence is ready.',
      source: 'controller',
      sourceEventSequence: streamEvents.length + 1,
      sourceEventType: 'agent.turn.finished'
    }
  });
  const sourceSequences = streamEvents.map((event) => event.sequence);
  let workspaceId = '';
  let sessionDetailRequests = 0;

  await page.addInitScript(({ sessionId, streamEvents }) => {
    const nativeFetch = window.fetch.bind(window);
    window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
      const requestUrl = new URL(
        typeof input === 'string'
          ? input
          : input instanceof URL
            ? input.href
            : input.url,
        window.location.href
      );
      if (requestUrl.pathname.endsWith(`/agent-sessions/${sessionId}/events`)) {
        const encoder = new TextEncoder();
        let timer: ReturnType<typeof setTimeout> | undefined;
        let index = 0;
        let paused = false;
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            const emit = () => {
              if (init?.signal?.aborted) {
                controller.close();
                return;
              }
              const event = streamEvents[index++];
              if (!event) {
                controller.close();
                return;
              }
              controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
              (window as Window & { __agentStreamDeliveredSequence?: number })
                .__agentStreamDeliveredSequence = event.sequence;
              if (event.sequence === 2) {
                paused = true;
                (window as Window & { __agentStreamPaused?: boolean })
                  .__agentStreamPaused = true;
                return;
              }
              timer = setTimeout(emit, 25);
            };
            (window as Window & { __resumeAgentStream?: () => void })
              .__resumeAgentStream = () => {
                if (!paused) return;
                paused = false;
                (window as Window & { __agentStreamPaused?: boolean })
                  .__agentStreamPaused = false;
                timer = setTimeout(emit, 25);
              };
            timer = setTimeout(emit, 25);
            init?.signal?.addEventListener('abort', () => {
              if (timer !== undefined) clearTimeout(timer);
              try {
                controller.close();
              } catch {
                // The stream may already have closed after its terminal event.
              }
            }, { once: true });
          },
          cancel() {
            if (timer !== undefined) clearTimeout(timer);
          }
        });
        return new Response(stream, {
          status: 200,
          headers: { 'Content-Type': 'text/event-stream' }
        });
      }
      return nativeFetch(input, init);
    };
  }, { sessionId, streamEvents });

  const summary = {
    id: sessionId,
    workspaceId,
    harnessId: 'v0',
    modelProfileId: 'fixture',
    modelId: 'fixture/v0',
    status: 'ready',
    active: true,
    createdAtMs: startedAtMs - 500,
    updatedAtMs: startedAtMs - 500,
    turnCount: 0
  };
  const sessionDetail = (events: typeof streamEvents) => {
    const started = events.some((event) => event.type === 'agent.turn.started');
    const complete = events.some((event) => event.type === 'agent.turn.finished');
    let response = '';
    const assistantSequences: number[] = [];
    let messageComplete = false;
    for (const event of events) {
      if (
        event.type !== 'observation.assistant.delta' &&
        event.type !== 'observation.assistant.completed'
      ) continue;
      const body = event.payload.event as { text: string };
      assistantSequences.push(event.sequence);
      if (event.type === 'observation.assistant.completed') {
        response = body.text;
        messageComplete = true;
      } else {
        response += body.text;
      }
    }
    const presentation = {
      schemaVersion: 1,
      response: response || null,
      messages: response
        ? [{
          id: messageId,
          text: response,
          complete: messageComplete,
          sourceEventSequences: assistantSequences
        }]
        : [],
      activity: [],
      usage: null,
      completeness: {
        assistantOutput: messageComplete ? 'complete' : response ? 'partial' : 'unavailable',
        capabilityActivity: complete ? 'complete' : 'partial',
        nativeActivity: complete ? 'complete' : 'partial',
        workspaceEffects: complete ? 'complete' : 'partial',
        usage: 'unavailable'
      },
      sourceEventSequences: events.map((event) => event.sequence),
      sourceDigest: complete ? 'sha256:stream-complete' : 'sha256:stream-pending'
    };
    return {
      projectionVersion: 1,
      summary: {
        ...summary,
        workspaceId,
        status: started && !complete ? 'running' : 'ready',
        updatedAtMs: events.at(-1)?.atMs ?? startedAtMs - 500,
        turnCount: started ? 1 : 0
      },
      turns: started
        ? [{
            id: turnId,
            sessionId,
            prompt: 'Stream a long answer',
            sourceRevision: 'sha256:explore',
            capabilityRevisions: { catalog: 'catalog-v2' },
            status: complete ? 'completed' : 'running',
            startedAtMs,
            finishedAtMs: complete ? events.at(-1)?.atMs : undefined,
            outcome: complete ? 'completed' : undefined,
            presentation
          }]
        : [],
      events
    };
  };

  await page.route(/\/api\/runs$/, async (route) => {
    const response = await route.fetch();
    const runs = await response.json();
    runs.push({
      id: 'inspection-run',
      scenarioId: 'catalog-to-file',
      scenarioTitle: 'Historical inspection',
      modelId: 'fixture/history',
      harnessId: 'eve',
      modelProfileId: 'fixture',
      status: 'passed',
      startedAtMs: 1,
      finishedAtMs: 2,
      eventCount: 1
    });
    await route.fulfill({ response, json: runs });
  });
  await page.route(/\/api\/runs\/inspection-run$/, async (route) => {
    await route.fulfill({
      json: {
        summary: {
          id: 'inspection-run',
          scenarioId: 'catalog-to-file',
          scenarioTitle: 'Historical inspection',
          modelId: 'fixture/history',
          harnessId: 'eve',
          modelProfileId: 'fixture',
          status: 'passed',
          startedAtMs: 1,
          finishedAtMs: 2,
          eventCount: 1
        },
        assembly: {
          question: 'Historical question',
          scenario: {
            id: 'catalog-to-file',
            title: 'Historical inspection',
            description: 'Historical run',
            version: 1,
            output: 'result.json'
          },
          harness: {
            adapter: 'external-jsonl',
            modelId: 'fixture/history',
            driver: { name: 'history-driver', version: '1', features: [] }
          },
          workspace: {
            id: 'inspection-run/workspace',
            seed: 'historical-seed',
            seedRevision: 'sha256:historical',
            attachment: 'physical-workspace',
            changeTracking: 'initial-final-diff'
          },
          capabilitySources: [],
          limits: {
            maxDurationMs: 1_000,
            maxCommandCount: 1,
            maxOrchestratorInvocations: 1,
            maxToolInvocations: 1
          }
        },
        review: {
          version: 1,
          status: 'passed',
          metrics: {
            modelTurns: 1,
            capabilityCalls: 0,
            nativeActions: 0,
            workspaceChanges: 0,
            durationMs: 1
          },
          steps: [{
            ordinal: 1,
            kind: 'outcome',
            title: 'Historical review',
            detail: 'Durable run evidence',
            status: 'passed',
            eventSequences: [1],
            source: null,
            path: null
          }]
        },
        events: [{
          sequence: 1,
          atMs: 1,
          type: 'run.finished',
          payload: { status: 'passed' }
        }],
        score: { passed: true },
        output: { activeCount: 2 }
      }
    });
  });
  await page.route(/\/api\/workbench\/[^/]+$/, async (route) => {
    const response = await route.fetch();
    const workbench = await response.json();
    workspaceId = workbench.workspaceId;
    summary.workspaceId = workspaceId;
    workbench.activeAgentSession = summary;
    workbench.replayAgentSession = null;
    workbench.agentSessions = [summary];
    await route.fulfill({ response, json: workbench });
  });
  await page.route(new RegExp(`/api/workbench/[^/]+/agent-sessions/${sessionId}$`), async (route) => {
    sessionDetailRequests += 1;
    const deliveredSequence = await page.evaluate(() =>
      (window as Window & { __agentStreamDeliveredSequence?: number })
        .__agentStreamDeliveredSequence ?? 0
    );
    await route.fulfill({
      json: sessionDetail(streamEvents.filter((event) => event.sequence <= deliveredSequence))
    });
  });

  await page.goto('/');
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  const session = page.getByTestId('interactive-agent-session');
  await expect(session.getByRole('heading', { name: 'Stream complete', level: 3 })).toBeVisible();
  await expect.poll(() =>
    page.evaluate(() =>
      (window as Window & { __agentStreamPaused?: boolean }).__agentStreamPaused ?? false
    )
  ).toBe(true);
  await expect.poll(() => sessionDetailRequests).toBe(2);
  const liveStatus = session.getByTestId('agent-live-status');
  await expect(liveStatus).toHaveAttribute('data-phase', 'responding');
  await expect(liveStatus).toContainText('Streaming the answer.');
  await expect(liveStatus).toContainText('assistant');
  await expect(liveStatus.getByRole('button', { name: 'Cancel turn' })).toBeEnabled();
  const initialElapsed = await liveStatus.locator('time').textContent();
  await expect.poll(() => liveStatus.locator('time').textContent()).not.toBe(initialElapsed);
  await page.waitForTimeout(200);
  expect(sessionDetailRequests).toBe(2);
  const requestsBeforeResume = sessionDetailRequests;
  await expect(session.getByTestId('session-turn')).toHaveAttribute('data-status', 'running');
  await page.evaluate(() => {
    const resume = (window as Window & { __resumeAgentStream?: () => void }).__resumeAgentStream;
    if (!resume) throw new Error('agent stream resume hook is unavailable');
    resume();
  });
  await expect(session.getByTestId('session-turn')).toHaveAttribute('data-status', 'completed');
  await expect.poll(() => sessionDetailRequests).toBe(requestsBeforeResume + 1);
  await page.waitForTimeout(200);
  expect(sessionDetailRequests).toBe(requestsBeforeResume + 1);
  await expect(session.getByTestId('agent-live-status')).toHaveCount(0);

  const evidence = session.locator('.turn-evidence');
  await evidence.locator(':scope > summary').click();
  await evidence.locator('.turn-raw-events > summary').click();
  const observedSequences = await evidence.locator('.run-events .sequence').allTextContents();
  expect(observedSequences).toEqual(sourceSequences.map((sequence) => String(sequence).padStart(2, '0')));

  const historicalRun = page.locator('.history-list button').filter({ hasText: 'Historical inspection' });
  await historicalRun.click();
  await expect(page.getByRole('button', { name: 'Agent Run', exact: true })).toHaveClass(/active/);
  await expect(page.getByTestId('run-review')).toContainText('Historical review');
  await expect(page.getByTestId('interactive-agent-session')).toHaveCount(0);

  await page.getByRole('button', { name: 'Session', exact: true }).click();
  await expect(page.getByTestId('interactive-agent-session')).toBeVisible();
  await expect(page.locator('.run-heading')).toContainText('Active session');
  await expect(session.locator('.session-environment > summary')).toContainText('Session environment');
  await session.locator('.session-environment > summary').click();
  await expect(session.locator('.session-environment')).not.toContainText('sha256:historical');
  await page.unrouteAll({ behavior: 'wait' });
});

test('an active turn can be cancelled from compact progress without polling', async ({ page }) => {
  const sessionId = 'cancel-session';
  const turnId = 'cancel-turn';
  const startedAtMs = Date.now() - 1_250;
  const startedEvent = {
    sequence: 1,
    atMs: startedAtMs,
    type: 'agent.turn.started',
    payload: {
      sessionId,
      turnId,
      prompt: 'Inspect the catalog until cancelled',
      input: null
    }
  };
  const progressEvent = {
    sequence: 2,
    atMs: startedAtMs + 500,
    type: 'fixture.opaque-event',
    payload: { sessionId, turnId },
    progress: {
      phase: 'acting',
      detail: 'Inspecting the active catalog.',
      source: 'catalog · list',
      sourceEventSequence: 14,
      sourceEventType: 'mcp.tool.started'
    }
  };
  const finishedEvent = {
    sequence: 3,
    atMs: startedAtMs + 2_000,
    type: 'agent.turn.finished',
    payload: { sessionId, turnId, outcome: 'cancelled' },
    progress: {
      phase: 'finalizing',
      detail: 'Retaining cancelled turn evidence.',
      source: 'controller',
      sourceEventSequence: 15,
      sourceEventType: 'agent.turn.finished'
    }
  };
  let workspaceId = '';
  let sessionDetailRequests = 0;
  let cancelRequests = 0;

  await page.addInitScript(({ sessionId, startedEvent, progressEvent, finishedEvent }) => {
    const nativeFetch = window.fetch.bind(window);
    window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
      const requestUrl = new URL(
        typeof input === 'string'
          ? input
          : input instanceof URL
            ? input.href
            : input.url,
        window.location.href
      );
      if (requestUrl.pathname.endsWith(`/agent-sessions/${sessionId}/events`)) {
        const encoder = new TextEncoder();
        let closed = false;
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            const publish = (
              event: typeof startedEvent | typeof progressEvent | typeof finishedEvent,
              close = false
            ) => {
              if (closed || init?.signal?.aborted) return;
              controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
              (window as Window & { __cancelStreamDeliveredSequence?: number })
                .__cancelStreamDeliveredSequence = event.sequence;
              if (close) {
                closed = true;
                controller.close();
              }
            };
            setTimeout(() => publish(startedEvent), 25);
            (window as Window & { __publishAgentProgress?: () => void })
              .__publishAgentProgress = () => publish(progressEvent);
            (window as Window & { __finishCancelledAgentTurn?: () => void })
              .__finishCancelledAgentTurn = () => publish(finishedEvent, true);
            init?.signal?.addEventListener('abort', () => {
              if (closed) return;
              closed = true;
              try {
                controller.close();
              } catch {
                // The stream may already be closed by the terminal event.
              }
            }, { once: true });
          },
          cancel() {
            closed = true;
          }
        });
        return new Response(stream, {
          status: 200,
          headers: { 'Content-Type': 'text/event-stream' }
        });
      }
      return nativeFetch(input, init);
    };
  }, { sessionId, startedEvent, progressEvent, finishedEvent });

  const summary = {
    id: sessionId,
    workspaceId,
    harnessId: 'v0',
    modelProfileId: 'fixture',
    modelId: 'fixture/v0',
    status: 'ready',
    active: true,
    createdAtMs: startedAtMs - 500,
    updatedAtMs: startedAtMs - 500,
    turnCount: 0
  };
  const sessionDetail = (deliveredSequence: number) => {
    const started = deliveredSequence >= startedEvent.sequence;
    const progressed = deliveredSequence >= progressEvent.sequence;
    const finished = deliveredSequence >= finishedEvent.sequence;
    return {
      projectionVersion: 1,
      summary: {
        ...summary,
        workspaceId,
        status: started && !finished ? 'running' : 'ready',
        updatedAtMs: finished
          ? finishedEvent.atMs
          : started
            ? startedEvent.atMs
            : summary.updatedAtMs,
        turnCount: started ? 1 : 0
      },
      turns: started
        ? [{
            id: turnId,
            sessionId,
            prompt: 'Inspect the catalog until cancelled',
            sourceRevision: 'sha256:cancel',
            capabilityRevisions: { catalog: 'catalog-v2' },
            status: finished ? 'cancelled' : 'running',
            startedAtMs,
            finishedAtMs: finished ? finishedEvent.atMs : undefined,
            outcome: finished ? 'cancelled' : undefined,
            presentation: {
              schemaVersion: 1,
              response: null,
              messages: [],
              activity: [],
              usage: null,
              completeness: {
                assistantOutput: 'unavailable',
                capabilityActivity: 'partial',
                nativeActivity: 'partial',
                workspaceEffects: 'partial',
                usage: 'unavailable'
              },
              sourceEventSequences: [
                ...(started ? [1] : []),
                ...(progressed ? [2] : []),
                ...(finished ? [3] : [])
              ],
              sourceDigest: finished ? 'sha256:cancelled' : 'sha256:cancelling'
            }
          }]
        : [],
      events: [
        ...(started ? [startedEvent] : []),
        ...(progressed ? [progressEvent] : []),
        ...(finished ? [finishedEvent] : [])
      ]
    };
  };

  await page.route(/\/api\/workbench\/[^/]+$/, async (route) => {
    const response = await route.fetch();
    const workbench = await response.json();
    workspaceId = workbench.workspaceId;
    summary.workspaceId = workspaceId;
    workbench.activeAgentSession = summary;
    workbench.replayAgentSession = null;
    workbench.agentSessions = [summary];
    await route.fulfill({ response, json: workbench });
  });
  await page.route(new RegExp(`/api/workbench/[^/]+/agent-sessions/${sessionId}$`), async (route) => {
    sessionDetailRequests += 1;
    const deliveredSequence = await page.evaluate(() =>
      (window as Window & { __cancelStreamDeliveredSequence?: number })
        .__cancelStreamDeliveredSequence ?? 0
    );
    await route.fulfill({ json: sessionDetail(deliveredSequence) });
  });
  await page.route(
    new RegExp(`/api/workbench/[^/]+/agent-sessions/${sessionId}/cancel$`),
    async (route) => {
      cancelRequests += 1;
      expect(route.request().method()).toBe('POST');
      await route.fulfill({ status: 202 });
    }
  );

  await page.setViewportSize({ width: 320, height: 700 });
  await page.goto('/');
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  await expect.poll(() => sessionDetailRequests).toBe(2);
  const session = page.getByTestId('interactive-agent-session');
  const liveStatus = session.getByTestId('agent-live-status');
  await expect(liveStatus).toHaveAttribute('data-phase', 'running');
  await expect(liveStatus).toContainText('Waiting for the next progress update.');
  expect(sessionDetailRequests).toBe(2);

  await page.evaluate(() =>
    (window as Window & { __publishAgentProgress?: () => void })
      .__publishAgentProgress?.()
  );
  await expect(liveStatus).toHaveAttribute('data-phase', 'acting');
  await expect(liveStatus).toContainText('Inspecting the active catalog.');
  await expect(liveStatus).toContainText('catalog · list');
  await expect(liveStatus).toHaveAttribute('data-source-event-sequence', '14');
  await expect(liveStatus).toHaveAttribute('data-source-event-type', 'mcp.tool.started');

  await liveStatus.evaluate((element) => element.scrollIntoView({ block: 'center' }));
  const narrowGeometry = await page.evaluate(() => {
    const status = document.querySelector<HTMLElement>('[data-testid="agent-live-status"]');
    const panel = document.querySelector<HTMLElement>('.run-panel');
    const button = status?.querySelector<HTMLElement>('button');
    const statusBounds = status?.getBoundingClientRect();
    const panelBounds = panel?.getBoundingClientRect();
    const buttonBounds = button?.getBoundingClientRect();
    return {
      viewportWidth: window.innerWidth,
      documentWidth: document.documentElement.scrollWidth,
      status: statusBounds ? { left: statusBounds.left, right: statusBounds.right } : null,
      panel: panelBounds ? { left: panelBounds.left, right: panelBounds.right } : null,
      button: buttonBounds ? { left: buttonBounds.left, right: buttonBounds.right } : null
    };
  });
  expect(narrowGeometry.documentWidth).toBeLessThanOrEqual(narrowGeometry.viewportWidth);
  expect(narrowGeometry.status).not.toBeNull();
  expect(narrowGeometry.panel).not.toBeNull();
  expect(narrowGeometry.button).not.toBeNull();
  expect(narrowGeometry.status!.left).toBeGreaterThanOrEqual(narrowGeometry.panel!.left - 1);
  expect(narrowGeometry.status!.right).toBeLessThanOrEqual(narrowGeometry.panel!.right + 1);
  expect(narrowGeometry.button!.left).toBeGreaterThanOrEqual(narrowGeometry.status!.left - 1);
  expect(narrowGeometry.button!.right).toBeLessThanOrEqual(narrowGeometry.status!.right + 1);

  await liveStatus.getByRole('button', { name: 'Cancel turn' }).click();
  await expect.poll(() => cancelRequests).toBe(1);
  await expect(liveStatus).toHaveAttribute('data-phase', 'cancelling');
  await expect(liveStatus.getByRole('button', { name: 'Cancelling…' })).toBeDisabled();
  expect(sessionDetailRequests).toBe(2);

  await page.evaluate(() =>
    (window as Window & { __finishCancelledAgentTurn?: () => void })
      .__finishCancelledAgentTurn?.()
  );
  await expect(session.getByTestId('session-turn')).toHaveAttribute('data-status', 'cancelled');
  await expect(session.getByTestId('agent-live-status')).toHaveCount(0);
  await expect.poll(() => sessionDetailRequests).toBe(3);

  const evidence = session.locator('.turn-evidence');
  await evidence.locator(':scope > summary').click();
  await evidence.locator('.turn-raw-events > summary').click();
  await expect(evidence.locator('.run-events .sequence')).toHaveText(['01', '02', '03']);
  expect(cancelRequests).toBe(1);
  await page.unrouteAll({ behavior: 'wait' });
});

test('agent answers render constrained Markdown and retain inspectable source', async ({ page }) => {
  const terminalFramesSent: string[] = [];
  page.on('websocket', (socket) => {
    if (!/\/api\/terminal(?:\?|$)/.test(socket.url())) return;
    socket.on('framesent', ({ payload }) => {
      if (typeof payload === 'string') terminalFramesSent.push(payload);
    });
  });
  const firstMessage = [
    '# Findings',
    '',
    '- **Alpha** is active',
    '- `gamma` scored 8',
    '',
    '```nu',
    'catalog list | where active',
    '```',
    '',
    '| item | score |',
    '| --- | ---: |',
    '| gamma | 8 |',
    '',
    '[Guide](https://example.com/guide)',
    '[Protocol relative](//example.com/guide)',
    '[Dial](tel:+15551212)',
    '[Unsafe](javascript:alert(1))',
    '![Tracking pixel](https://example.com/agent-image.png)',
    '<img src="https://example.com/raw-image.png" onerror="alert(1)">',
    '<script>window.agentMarkdownXss = true</script>'
  ].join('\n');
  const streamingSecondMessage = '## Next\n\nWaiting for **authoritative';
  const completedSecondMessage = '## Next\n\nAuthoritative completion keeps the **same evidence**.';
  let authoritative = false;
  const resourceRequests: string[] = [];
  page.on('request', (request) => {
    if (request.url().includes('agent-image.png') || request.url().includes('raw-image.png')) {
      resourceRequests.push(request.url());
    }
  });
  await page.route(/\/api\/workbench\/[^/]+\/agent-sessions\/[^/]+$/, async (route) => {
    const response = await route.fetch();
    const detail = (await response.json()) as {
      turns?: Array<{
        id: string;
        presentation?: {
          response: string | null;
          messages: Array<{
            id: string;
            text: string;
            complete: boolean;
            sourceEventSequences: number[];
          }>;
          sourceEventSequences: number[];
        };
      }>;
    };
    if (!Array.isArray(detail.turns)) {
      await route.fulfill({ response });
      return;
    }
    for (const turn of detail.turns) {
      if (!turn.presentation?.messages.length) continue;
      const sourceSequences = turn.presentation.sourceEventSequences;
      turn.presentation.response = 'Flattened response should not replace message boundaries.';
      turn.presentation.messages = [
        { id: `${turn.id}-one`, text: firstMessage, complete: true, sourceEventSequences: sourceSequences },
        {
          id: `${turn.id}-two`,
          text: authoritative ? completedSecondMessage : streamingSecondMessage,
          complete: authoritative,
          sourceEventSequences: sourceSequences
        }
      ];
    }
    await route.fulfill({ response, json: detail });
  });

  await page.goto('/');
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  const terminalInput = page.locator('[data-testid="terminal"] textarea');
  const terminalCanvas = page.locator('[data-testid="terminal"] canvas');
  await expect(terminalInput).toBeFocused();
  await page.getByLabel('Default harness').selectOption('v0');
  await submit(page, 'agent "Summarize the workspace"');

  const session = page.getByTestId('interactive-agent-session');
  const answer = session.getByTestId('agent-response').first();
  await expect(answer.locator('.assistant-message')).toHaveCount(2);
  await expect(answer).not.toContainText('Flattened response should not replace message boundaries.');
  await expect(answer.getByRole('heading', { name: 'Findings', level: 3 })).toBeVisible();
  await expect(answer.getByRole('heading', { name: 'Next', level: 4 })).toBeVisible();
  await expect(answer.locator('strong').filter({ hasText: 'Alpha' })).toBeVisible();
  await expect(answer.getByRole('region', { name: 'Table in agent response' })).toContainText('gamma');
  await expect(answer.getByRole('region', { name: 'nu code from agent response' })).toContainText(
    'catalog list | where active'
  );
  const guide = answer.getByRole('link', { name: 'Guide' });
  await expect(guide).toHaveAttribute('href', 'https://example.com/guide');
  await expect(guide).toHaveAttribute('target', '_blank');
  await expect(guide).toHaveAttribute('rel', 'noopener noreferrer');
  const protocolRelative = answer.getByRole('link', { name: 'Protocol relative' });
  await expect(protocolRelative).toHaveAttribute('href', '//example.com/guide');
  await expect(protocolRelative).toHaveAttribute('target', '_blank');
  await expect(protocolRelative).toHaveAttribute('rel', 'noopener noreferrer');
  await expect(answer.getByRole('link', { name: 'Dial' })).toHaveCount(0);
  await expect(answer.getByRole('link', { name: 'Unsafe' })).toHaveCount(0);
  await expect(answer.locator('.blocked-link')).toContainText(['Dial', 'Unsafe']);
  await expect(answer.getByRole('note', { name: 'Image omitted: Tracking pixel' })).toBeVisible();
  expect(
    await answer.evaluate((element) =>
      [...element.querySelectorAll('img, script')].map((unsafeElement) => unsafeElement.outerHTML)
    )
  ).toEqual([]);
  await expect(answer.locator('[data-streaming="true"]')).toHaveCount(1);
  expect(resourceRequests).toEqual([]);
  expect(await page.evaluate(() => (window as Window & { agentMarkdownXss?: boolean }).agentMarkdownXss)).toBeUndefined();

  await terminalCanvas.evaluate((canvas) => (canvas.dataset.markdownSessionProbe = 'same-canvas'));
  authoritative = true;
  await submit(page, 'agent "Confirm the conclusion"');
  await expect(answer.locator('.assistant-message').last()).toHaveAttribute('data-complete', 'true');
  await expect(answer).toContainText('Authoritative completion keeps the same evidence.');
  await expect(answer.locator('[data-streaming="true"]')).toHaveCount(0);
  await expect(terminalCanvas).toHaveAttribute('data-markdown-session-probe', 'same-canvas');
  await expect(terminalInput).toBeFocused();

  const presentation = session.getByRole('group', { name: 'Agent answer presentation' });
  await presentation.getByRole('button', { name: 'Source' }).click();
  await expect(answer.locator('.response-source')).toHaveCount(2);
  await expect(answer.locator('.response-source').first()).toContainText('# Findings');
  await expect(answer.locator('.response-source').first()).toContainText('<script>window.agentMarkdownXss = true</script>');
  await expect(answer.locator('.response-source').last()).toContainText('Authoritative completion');
  await expect(presentation.getByRole('button', { name: 'Source' })).toHaveAttribute('aria-pressed', 'true');
  await presentation.getByRole('button', { name: 'Rendered' }).click();
  await expect(answer.getByRole('heading', { name: 'Findings', level: 3 })).toBeVisible();

  await terminalCanvas.click({ position: { x: 10, y: 10 } });
  await expect(terminalInput).toBeFocused();
  const humanInputBeforePaste = terminalFramesSent.filter(
    (frame) => frame === JSON.stringify({ type: 'human_input' })
  ).length;
  await terminalInput.evaluate((input) => {
    const clipboard = new DataTransfer();
    clipboard.setData('text/plain', '40 + 2');
    input.dispatchEvent(
      new ClipboardEvent('paste', {
        bubbles: true,
        cancelable: true,
        clipboardData: clipboard
      })
    );
  });
  await expect.poll(
    () =>
      terminalFramesSent.filter(
        (frame) => frame === JSON.stringify({ type: 'human_input' })
      ).length
  ).toBe(humanInputBeforePaste + 1);
  await page.keyboard.press('Enter');
  await expect(page.getByTestId('terminal-text')).toContainText('42');

  // Keep the persistent e2e workbench neutral for the catalog walkthrough that follows.
  await submit(page, 'agent close');
  await expect(page.getByTestId('terminal-text')).toContainText('closing');
  await expect(page.locator('.run-heading')).toContainText('Session history');
  await expect(page.locator('.run-heading')).toContainText('closed');
  await expect(page.getByTestId('interactive-agent-session')).toContainText(
    'Retained as durable evidence. Start a new agent session to continue.'
  );
  await page.reload();
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  await expect(page.getByTestId('interactive-agent-session')).toHaveCount(0);
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
  await expect(screen).toContainText('agent "What matters about this workspace?"');
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

  await page.getByLabel('Default harness').selectOption('v0');
  await expect(page.getByLabel('Default harness')).toHaveValue('v0');
  await submit(page, 'help agent');
  await expect(screen).toContainText('Continue the active harness-native agent session');
  await submit(page, 'agent "Find the active catalog items and explain what matters"');
  await expect(screen).toContainText('Alpha and gamma are active');
  await expect(screen).toContainText('Gamma matters most');
  const interactiveSession = page.getByTestId('interactive-agent-session');
  await expect(page.locator('.run-heading')).toContainText('Active session');
  await expect(page.locator('.run-heading')).toContainText('v0 · fixture');
  await expect(interactiveSession).toContainText('Find the active catalog items and explain what matters');
  await expect(interactiveSession.getByTestId('agent-response')).toContainText('Alpha and gamma are active');
  await expect(interactiveSession).toContainText('catalog · list');
  await expect(interactiveSession).toContainText('analysis · summarize');
  await expect(page.getByTestId('run-review')).toHaveCount(0);
  await expect(interactiveSession.getByText('Provided context')).toHaveCount(0);
  await expect(interactiveSession.getByText(/Human input observed/)).toHaveCount(0);
  const answerPresentation = interactiveSession.getByRole('group', { name: 'Agent answer presentation' });
  await expect(answerPresentation.getByRole('button', { name: 'Rendered' })).toHaveAttribute('aria-pressed', 'true');
  await answerPresentation.getByRole('button', { name: 'Source' }).click();
  await expect(interactiveSession.locator('.response-source')).toContainText('**Alpha** and **gamma** are active.');
  await expect(answerPresentation.getByRole('button', { name: 'Source' })).toHaveAttribute('aria-pressed', 'true');
  await answerPresentation.getByRole('button', { name: 'Rendered' }).click();

  await submit(page, 'agent "Why did you prioritize gamma?"');
  await expect(screen).toContainText('Gamma matters most');
  await expect(interactiveSession).toContainText('Why did you prioritize gamma?');
  await expect(interactiveSession.getByTestId('session-turn')).toHaveCount(2);

  await submit(page, 'catalog list | where active | agent "Use exactly these items and compare them"');
  await expect(screen).toContainText('Gamma matters most');
  await expect(interactiveSession).toContainText('Use exactly these items and compare them');
  await expect(interactiveSession.getByText('Provided context')).toHaveCount(1);
  await expect(interactiveSession.getByTestId('session-turn')).toHaveCount(3);

  await submit(page, 'let first = (agent | get id); let second = (agent new | get id); agent switch $first | get id');
  await expect(interactiveSession).toContainText('Find the active catalog items and explain what matters');
  await expect(interactiveSession.getByTestId('agent-response').first()).toContainText('Alpha and gamma are active');
  await submit(page, 'let first = (agent | get id); let sessions = (agent sessions); let second = ($sessions | where id != $first | get id | first); agent close; agent switch $second; agent close');

  await page.getByLabel('Default harness').selectOption('v0');
  await expect(page.getByLabel('Default harness')).toHaveValue('v0');
  await expect(page.getByLabel('Default model')).toHaveValue('fixture');
  await page.getByRole('button', { name: 'Run harness', exact: true }).click();
  await expect(page.locator('.run-heading > .run-status')).toHaveText('passed');
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
  await expect(page.locator('.run-heading > .run-status')).toHaveText('exploring');
  await expect(page.getByRole('button', { name: 'Run harness', exact: true })).toBeVisible();
  await expect(page.locator('.terminal-footer span').nth(1)).not.toHaveText(completedRunId ?? '');

  await page.reload();
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  const socketsBeforeReplay = socketUrls.length;
  const historyRun = page
    .locator('.history-list button')
    .filter({ hasText: (completedRunId ?? '').replace(/^run\s+/, '') });
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
  await page.getByLabel('Default harness').selectOption('v0');
  await expect(page.getByLabel('Default harness')).toHaveValue('v0');
  await expect(page.getByLabel('Default model')).toHaveValue('fixture');

  await page.reload();
  await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');
  await expect(page.getByLabel('Default harness')).toHaveValue('v0');
  await expect(page.getByLabel('Default model')).toHaveValue('fixture');

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
  await expect(behavioralDiff).toContainText('Artifact');
  await expect(behavioralDiff).toContainText('result.json');
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

  await page.setViewportSize({ width: 320, height: 700 });
  await runPanel.evaluate((element) => element.scrollIntoView({ block: 'start' }));
  const tabs = page.getByRole('navigation', { name: 'Run views' });
  const evaluationTab = tabs.getByRole('button', { name: 'Evaluation', exact: true });
  await evaluationTab.evaluate((element) =>
    element.scrollIntoView({ block: 'nearest', inline: 'end' })
  );
  const narrowGeometry = await page.evaluate(() => {
    const tabs = document.querySelector<HTMLElement>('.tabs');
    const lastTab = tabs?.lastElementChild?.getBoundingClientRect();
    const bounds = tabs?.getBoundingClientRect();
    return {
      viewportWidth: window.innerWidth,
      documentWidth: document.documentElement.scrollWidth,
      tabs: bounds ? { left: bounds.left, right: bounds.right } : null,
      lastTab: lastTab ? { left: lastTab.left, right: lastTab.right } : null,
      scrollLeft: tabs?.scrollLeft ?? 0,
      overflowX: tabs ? getComputedStyle(tabs).overflowX : ''
    };
  });
  expect(narrowGeometry.documentWidth).toBeLessThanOrEqual(narrowGeometry.viewportWidth);
  expect(narrowGeometry.tabs).not.toBeNull();
  expect(narrowGeometry.lastTab).not.toBeNull();
  expect(narrowGeometry.lastTab!.left).toBeGreaterThanOrEqual(narrowGeometry.tabs!.left - 1);
  expect(narrowGeometry.lastTab!.right).toBeLessThanOrEqual(narrowGeometry.tabs!.right + 1);
  expect(narrowGeometry.scrollLeft).toBeGreaterThan(0);
  expect(narrowGeometry.overflowX).toBe('auto');
});

test('workbench controls stay compact and unclipped at desktop widths', async ({ page }) => {
  for (const viewport of [
    { width: 990, height: 624 },
    { width: 1200, height: 760 },
    { width: 1440, height: 900 }
  ]) {
    await page.setViewportSize(viewport);
    await page.goto('/');
    await expect(page.locator('.connection')).toHaveAttribute('data-state', 'connected');

    const controls = page.locator('.run-controls');
    const metrics = await controls.evaluate((container) => {
      const containerBounds = container.getBoundingClientRect();
      const items = Array.from(container.children).map((item) => {
        const bounds = item.getBoundingClientRect();
        const control = item.matches('button, select')
          ? item
          : item.querySelector<HTMLElement>('button, select');
        return {
          left: bounds.left,
          right: bounds.right,
          top: bounds.top,
          bottom: bounds.bottom,
          clientWidth: control?.clientWidth ?? 0,
          scrollWidth: control?.scrollWidth ?? 0,
          clientHeight: control?.clientHeight ?? 0,
          scrollHeight: control?.scrollHeight ?? 0,
          tagName: control?.tagName ?? '',
          whiteSpace: control ? getComputedStyle(control).whiteSpace : ''
        };
      });
      return {
        container: {
          left: containerBounds.left,
          right: containerBounds.right,
          top: containerBounds.top,
          bottom: containerBounds.bottom,
          height: containerBounds.height
        },
        items
      };
    });

    expect(metrics.container.left).toBeGreaterThanOrEqual(-1);
    expect(metrics.container.right).toBeLessThanOrEqual(viewport.width + 1);
    expect(metrics.container.height).toBeLessThanOrEqual(52);
    for (const item of metrics.items) {
      expect(item.left).toBeGreaterThanOrEqual(metrics.container.left - 1);
      expect(item.right).toBeLessThanOrEqual(metrics.container.right + 1);
      expect(item.top).toBeGreaterThanOrEqual(metrics.container.top - 1);
      expect(item.bottom).toBeLessThanOrEqual(metrics.container.bottom + 1);
      if (item.tagName === 'BUTTON') {
        expect(item.scrollWidth).toBeLessThanOrEqual(item.clientWidth + 1);
        expect(item.scrollHeight).toBeLessThanOrEqual(item.clientHeight + 1);
        expect(item.whiteSpace).toBe('nowrap');
      }
    }
    if (viewport.width === 1200) {
      const panels = await page.locator('.bench').evaluate((bench) => {
        const terminal = bench.querySelector<HTMLElement>('.terminal-panel')?.getBoundingClientRect();
        const run = bench.querySelector<HTMLElement>('.run-panel')?.getBoundingClientRect();
        return terminal && run
          ? {
              terminal: { left: terminal.left, right: terminal.right, top: terminal.top },
              run: { left: run.left, right: run.right, top: run.top }
            }
          : null;
      });
      expect(panels).not.toBeNull();
      expect(panels!.run.left).toBeGreaterThanOrEqual(panels!.terminal.right - 1);
      expect(Math.abs(panels!.run.top - panels!.terminal.top)).toBeLessThanOrEqual(1);
      expect(panels!.run.right).toBeLessThanOrEqual(viewport.width + 1);
    }
  }
});
