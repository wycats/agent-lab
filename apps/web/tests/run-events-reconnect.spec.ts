import { expect, test } from '@playwright/test';
import { createRunClient, type RunEvent } from '../src/lib/runs';

test('run event streams reconnect and replay only events after the last delivered sequence', async () => {
  const nativeFetch = globalThis.fetch;
  const first = {
    sequence: 1,
    atMs: 1,
    type: 'workbench.agent.session.started',
    payload: { sessionId: 'session-1' }
  } satisfies RunEvent;
  const second = {
    sequence: 2,
    atMs: 2,
    type: 'workbench.agent.session.updated',
    payload: { session: { id: 'session-1', status: 'ready' } }
  } satisfies RunEvent;
  let streamRequests = 0;
  let tokenRequests = 0;
  let resetCalls = 0;
  let secondDeliveryAttempts = 0;
  const streamRequestTimes: number[] = [];

  globalThis.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(
      typeof input === 'string'
        ? input
        : input instanceof URL
          ? input.href
          : input.url,
      'http://127.0.0.1'
    );
    if (url.pathname === '/api/session-token') {
      tokenRequests += 1;
      return Response.json({ token: 'test-token' });
    }
    if (url.pathname === '/api/runs/run-1/events') {
      streamRequests += 1;
      streamRequestTimes.push(Date.now());
      expect(new Headers(init?.headers).get('Authorization')).toBe('Bearer test-token');
      const events = streamRequests === 1 ? [first] : [first, second];
      const encoder = new TextEncoder();
      return new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          for (const event of events) {
            controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
          }
          controller.close();
        }
      }), {
        status: 200,
        headers: {
          'Content-Type': 'text/event-stream',
          'X-Agent-Lab-Event-Stream-Epoch': 'boot-1'
        }
      });
    }
    throw new Error(`unexpected request: ${url.pathname}`);
  };

  try {
    const delivered: number[] = [];
    let finish: (() => void) | undefined;
    const receivedReplay = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const controller = createRunClient().events(
      'run-1',
      (event) => {
        if (event.sequence === second.sequence) {
          secondDeliveryAttempts += 1;
          if (secondDeliveryAttempts === 1) {
            throw new Error('simulate a failed browser event application');
          }
        }
        delivered.push(event.sequence);
        if (event.sequence === second.sequence) finish?.();
      },
      () => {
        resetCalls += 1;
        return 0;
      }
    );

    await receivedReplay;
    controller.abort();

    expect(delivered).toEqual([first.sequence, second.sequence]);
    expect(streamRequests).toBe(3);
    expect(tokenRequests).toBe(3);
    expect(resetCalls).toBe(0);
    expect(secondDeliveryAttempts).toBe(2);
    expect(streamRequestTimes[1] - streamRequestTimes[0]).toBeGreaterThanOrEqual(75);
  } finally {
    globalThis.fetch = nativeFetch;
  }
});

test('transient terminal events bypass durable sequence deduplication without advancing its cursor', async () => {
  const nativeFetch = globalThis.fetch;
  const running = {
    sequence: 1,
    atMs: 1,
    type: 'evaluation-validation.status',
    payload: { validationId: 'validation-1', status: 'running' }
  } satisfies RunEvent;
  const transientFinished = {
    sequence: 1,
    atMs: 2,
    type: 'evaluation-validation.finished',
    payload: {
      validationId: 'validation-1',
      executionStatus: 'inconclusive',
      assertionStatus: 'not-evaluated',
      durable: false
    }
  } satisfies RunEvent;
  const durableFollowup = {
    sequence: 2,
    atMs: 3,
    type: 'workbench.evaluation-library.changed',
    payload: { draftId: 'draft-1', change: 'validation-finished' }
  } satisfies RunEvent;
  let streamRequests = 0;

  globalThis.fetch = async (input: RequestInfo | URL) => {
    const url = new URL(
      typeof input === 'string'
        ? input
        : input instanceof URL
          ? input.href
          : input.url,
      'http://127.0.0.1'
    );
    if (url.pathname === '/api/session-token') {
      return Response.json({ token: 'test-token' });
    }
    if (url.pathname === '/api/workbench/workspace-1/evaluation-drafts/draft-1/events') {
      streamRequests += 1;
      const events = streamRequests === 1
        ? [running, transientFinished]
        : [running, durableFollowup];
      const encoder = new TextEncoder();
      return new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          for (const event of events) {
            controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
          }
          controller.close();
        }
      }), {
        status: 200,
        headers: {
          'Content-Type': 'text/event-stream',
          'X-Agent-Lab-Event-Stream-Epoch': 'boot-1'
        }
      });
    }
    throw new Error(`unexpected request: ${url.pathname}`);
  };

  try {
    const delivered: RunEvent[] = [];
    let finish: (() => void) | undefined;
    const receivedFollowup = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const controller = createRunClient().evaluationDraftEvents(
      'workspace-1',
      'draft-1',
      (event) => {
        delivered.push(event);
        if (event.sequence === durableFollowup.sequence) finish?.();
      }
    );

    await receivedFollowup;
    controller.abort();

    expect(delivered).toEqual([running, transientFinished, durableFollowup]);
    expect(streamRequests).toBe(2);
  } finally {
    globalThis.fetch = nativeFetch;
  }
});

test('a new server epoch reconciles authoritative terminal history before resetting sequence delivery', async () => {
  const nativeFetch = globalThis.fetch;
  const initial = {
    sequence: 1,
    atMs: 1,
    type: 'run.status',
    payload: { status: 'running' }
  } satisfies RunEvent;
  const highWater = {
    sequence: 42,
    atMs: 42,
    type: 'observation.assistant.delta',
    payload: { text: 'pre-restart activity' }
  } satisfies RunEvent;
  const recovered = {
    sequence: 1,
    atMs: 100,
    type: 'run.finished',
    payload: {
      status: 'cancelled',
      error: 'controller stopped before the run finalized',
      recovered: true
    }
  } satisfies RunEvent;
  let streamRequests = 0;
  let detailRequests = 0;
  let resetAttempts = 0;
  let recoveredDeliveryAttempts = 0;
  const delivered: Array<{ sequence: number; type: string }> = [];
  const phases: string[] = [];
  let projection = {
    status: 'running',
    eventCount: highWater.sequence,
    events: [initial, highWater] as RunEvent[]
  };

  globalThis.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(
      typeof input === 'string'
        ? input
        : input instanceof URL
          ? input.href
          : input.url,
      'http://127.0.0.1'
    );
    if (url.pathname === '/api/session-token') {
      return Response.json({ token: 'test-token' });
    }
    if (url.pathname === '/api/runs/run-1') {
      detailRequests += 1;
      phases.push('detail');
      return Response.json({
        summary: {
          id: 'run-1',
          status: 'cancelled',
          eventCount: highWater.sequence
        },
        events: [recovered]
      });
    }
    if (url.pathname === '/api/runs/run-1/events') {
      streamRequests += 1;
      expect(new Headers(init?.headers).get('Authorization')).toBe('Bearer test-token');
      const firstBoot = streamRequests === 1;
      const events = firstBoot ? [initial, highWater] : [recovered];
      const encoder = new TextEncoder();
      return new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          for (const event of events) {
            controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
          }
          controller.close();
        }
      }), {
        status: 200,
        headers: {
          'Content-Type': 'text/event-stream',
          'X-Agent-Lab-Event-Stream-Epoch': firstBoot ? 'boot-1' : 'boot-2'
        }
      });
    }
    throw new Error(`unexpected request: ${url.pathname}`);
  };

  try {
    let finish: (() => void) | undefined;
    const receivedRecovery = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const client = createRunClient();
    const controller = client.events(
      'run-1',
      (event) => {
        if (event.type === 'run.finished') {
          recoveredDeliveryAttempts += 1;
          phases.push('unexpected-recovered-delivery');
        }
        delivered.push({ sequence: event.sequence, type: event.type });
      },
      async () => {
        resetAttempts += 1;
        phases.push(`reset:${resetAttempts}`);
        if (resetAttempts === 1) {
          throw new Error('simulate a failed authoritative reset');
        }
        const detail = await client.detail('run-1');
        const reconciledSequence = detail.events.reduce(
          (latest, event) => Math.max(latest, event.sequence),
          0
        );
        projection = {
          status: detail.summary.status,
          eventCount: reconciledSequence,
          events: detail.events
        };
        phases.push('reconciled');
        setTimeout(() => finish?.(), 25);
        return reconciledSequence;
      }
    );

    await receivedRecovery;
    controller.abort();

    expect(delivered).toEqual([
      { sequence: initial.sequence, type: initial.type },
      { sequence: highWater.sequence, type: highWater.type }
    ]);
    expect(projection).toEqual({
      status: 'cancelled',
      eventCount: 1,
      events: [recovered]
    });
    expect(resetAttempts).toBe(2);
    expect(detailRequests).toBe(1);
    expect(recoveredDeliveryAttempts).toBe(0);
    expect(streamRequests).toBe(3);
    expect(phases).toEqual([
      'reset:1',
      'reset:2',
      'detail',
      'reconciled'
    ]);
  } finally {
    globalThis.fetch = nativeFetch;
  }
});

test('successful accepted responses preserve their JSON resource', async () => {
  const nativeFetch = globalThis.fetch;
  const attempt = {
    id: 'validation-1',
    draftId: 'draft-1',
    revisionId: 'revision-1',
    executionStatus: 'queued',
    assertionStatus: 'not-evaluated',
    harnessId: 'v0',
    modelProfileId: 'fixture',
    startedAtMs: 1
  } as const;

  globalThis.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(
      typeof input === 'string'
        ? input
        : input instanceof URL
          ? input.href
          : input.url,
      'http://127.0.0.1'
    );
    if (url.pathname === '/api/session-token') {
      return Response.json({ token: 'test-token' });
    }
    if (
      url.pathname ===
      '/api/workbench/workspace-1/evaluation-drafts/draft-1/validate'
    ) {
      expect(init?.method).toBe('POST');
      return Response.json(attempt, { status: 202 });
    }
    throw new Error(`unexpected request: ${url.pathname}`);
  };

  try {
    await expect(
      createRunClient().validateEvaluationDraft('workspace-1', 'draft-1', 'revision-1')
    ).resolves.toEqual(attempt);
  } finally {
    globalThis.fetch = nativeFetch;
  }
});

test('evaluation library reads remain global when the active workspace changes', async () => {
  const nativeFetch = globalThis.fetch;
  const requested: string[] = [];

  globalThis.fetch = async (input: RequestInfo | URL) => {
    const url = new URL(
      typeof input === 'string'
        ? input
        : input instanceof URL
          ? input.href
          : input.url,
      'http://127.0.0.1'
    );
    if (url.pathname === '/api/session-token') {
      return Response.json({ token: 'test-token' });
    }
    requested.push(url.pathname);
    if (url.pathname === '/api/evaluation-drafts') return Response.json([]);
    if (url.pathname === '/api/evaluation-drafts/draft-1') {
      return Response.json({ summary: { id: 'draft-1' } });
    }
    if (url.pathname === '/api/evaluation-definitions') return Response.json([]);
    if (url.pathname === '/api/evaluation-definitions/definition-1') {
      return Response.json({ summary: { id: 'definition-1' } });
    }
    throw new Error(`unexpected request: ${url.pathname}`);
  };

  try {
    const client = createRunClient();
    await client.evaluationLibraryDrafts();
    await client.evaluationLibraryDraft('draft-1');
    await client.evaluationLibraryDefinitions();
    await client.evaluationLibraryDefinition('definition-1');
    expect(requested).toEqual([
      '/api/evaluation-drafts',
      '/api/evaluation-drafts/draft-1',
      '/api/evaluation-definitions',
      '/api/evaluation-definitions/definition-1'
    ]);
  } finally {
    globalThis.fetch = nativeFetch;
  }
});

test('a healthy server epoch change does not replay already reconciled workbench side effects', async () => {
  const nativeFetch = globalThis.fetch;
  const prepared = {
    sequence: 1,
    atMs: 1,
    type: 'run.prepared',
    payload: { scenario: 'catalog-to-file' }
  } satisfies RunEvent;
  const reveal = {
    sequence: 2,
    atMs: 2,
    type: 'workbench.agent.session.activated',
    payload: { sessionId: 'session-1', origin: 'nushell' }
  } satisfies RunEvent;
  let streamRequests = 0;

  globalThis.fetch = async (input: RequestInfo | URL) => {
    const url = new URL(
      typeof input === 'string'
        ? input
        : input instanceof URL
          ? input.href
          : input.url,
      'http://127.0.0.1'
    );
    if (url.pathname === '/api/session-token') {
      return Response.json({ token: 'test-token' });
    }
    if (url.pathname === '/api/runs/run-1/events') {
      streamRequests += 1;
      const encoder = new TextEncoder();
      return new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          for (const event of [prepared, reveal]) {
            controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
          }
          controller.close();
        }
      }), {
        status: 200,
        headers: {
          'Content-Type': 'text/event-stream',
          'X-Agent-Lab-Event-Stream-Epoch': streamRequests === 1 ? 'boot-1' : 'boot-2'
        }
      });
    }
    throw new Error(`unexpected request: ${url.pathname}`);
  };

  try {
    let finish: (() => void) | undefined;
    const reconciled = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const delivered: number[] = [];
    let revealSideEffects = 0;
    let resetCalls = 0;
    const controller = createRunClient().events(
      'run-1',
      (event) => {
        delivered.push(event.sequence);
        if (event.type === reveal.type) revealSideEffects += 1;
      },
      () => {
        resetCalls += 1;
        setTimeout(() => finish?.(), 25);
        return reveal.sequence;
      }
    );

    await reconciled;
    controller.abort();

    expect(delivered).toEqual([prepared.sequence, reveal.sequence]);
    expect(revealSideEffects).toBe(1);
    expect(resetCalls).toBe(1);
    expect(streamRequests).toBe(2);
  } finally {
    globalThis.fetch = nativeFetch;
  }
});

test('failed reset and event callbacks cancel a non-closing response before reconnecting', async () => {
  const nativeFetch = globalThis.fetch;
  const initial = {
    sequence: 1,
    atMs: 1,
    type: 'run.status',
    payload: { status: 'running' }
  } satisfies RunEvent;
  const recovered = {
    sequence: 1,
    atMs: 2,
    type: 'run.finished',
    payload: { status: 'cancelled', recovered: true }
  } satisfies RunEvent;
  const cancelledBodies = new Set<number>();
  let streamRequests = 0;

  globalThis.fetch = async (input: RequestInfo | URL) => {
    const url = new URL(
      typeof input === 'string'
        ? input
        : input instanceof URL
          ? input.href
          : input.url,
      'http://127.0.0.1'
    );
    if (url.pathname === '/api/session-token') {
      return Response.json({ token: 'test-token' });
    }
    if (url.pathname === '/api/runs/run-1/events') {
      streamRequests += 1;
      const request = streamRequests;
      if (request === 3) expect(cancelledBodies.has(2)).toBe(true);
      if (request === 4) expect(cancelledBodies.has(3)).toBe(true);
      const event = request === 1 ? initial : recovered;
      const encoder = new TextEncoder();
      return new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
          if (request === 1) controller.close();
        },
        cancel() {
          cancelledBodies.add(request);
        }
      }), {
        status: 200,
        headers: {
          'Content-Type': 'text/event-stream',
          'X-Agent-Lab-Event-Stream-Epoch': request === 1 ? 'boot-1' : 'boot-2'
        }
      });
    }
    throw new Error(`unexpected request: ${url.pathname}`);
  };

  try {
    let finish: (() => void) | undefined;
    const recoveredAfterCleanup = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const delivered: string[] = [];
    let resetAttempts = 0;
    let recoveredAttempts = 0;
    const controller = createRunClient().events(
      'run-1',
      (event) => {
        if (event.type === recovered.type) {
          recoveredAttempts += 1;
          if (recoveredAttempts === 1) {
            throw new Error('simulate a failed event callback');
          }
        }
        delivered.push(event.type);
        if (event.type === recovered.type) finish?.();
      },
      () => {
        resetAttempts += 1;
        if (resetAttempts === 1) {
          throw new Error('simulate a failed reset callback');
        }
        return 0;
      }
    );

    await recoveredAfterCleanup;
    controller.abort();

    expect(streamRequests).toBe(4);
    expect(resetAttempts).toBe(2);
    expect(recoveredAttempts).toBe(2);
    expect(delivered).toEqual([initial.type, recovered.type]);
    expect(cancelledBodies.has(1)).toBe(false);
    expect(cancelledBodies.has(2)).toBe(true);
    expect(cancelledBodies.has(3)).toBe(true);
  } finally {
    globalThis.fetch = nativeFetch;
  }
});

test('an epoch-bearing 404 clears a stale run projection without waiting for replay', async () => {
  const nativeFetch = globalThis.fetch;
  const initial = {
    sequence: 1,
    atMs: 1,
    type: 'run.status',
    payload: { status: 'running' }
  } satisfies RunEvent;
  const highWater = {
    sequence: 42,
    atMs: 42,
    type: 'observation.assistant.delta',
    payload: { text: 'stale projection' }
  } satisfies RunEvent;
  let streamRequests = 0;

  globalThis.fetch = async (input: RequestInfo | URL) => {
    const url = new URL(
      typeof input === 'string'
        ? input
        : input instanceof URL
          ? input.href
          : input.url,
      'http://127.0.0.1'
    );
    if (url.pathname === '/api/session-token') {
      return Response.json({ token: 'test-token' });
    }
    if (url.pathname === '/api/runs/run-1/events') {
      streamRequests += 1;
      if (streamRequests > 1) {
        return new Response('', {
          status: 404,
          headers: { 'X-Agent-Lab-Event-Stream-Epoch': 'boot-1' }
        });
      }
      const encoder = new TextEncoder();
      return new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          for (const event of [initial, highWater]) {
            controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
          }
          controller.close();
        }
      }), {
        status: 200,
        headers: {
          'Content-Type': 'text/event-stream',
          'X-Agent-Lab-Event-Stream-Epoch': 'boot-1'
        }
      });
    }
    throw new Error(`unexpected request: ${url.pathname}`);
  };

  try {
    let finish: (() => void) | undefined;
    const cleared = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const delivered: number[] = [];
    let projection = [initial, highWater] as RunEvent[];
    const resets: Array<{ previousEpoch?: string; epoch: string; responseStatus: number }> = [];
    const controller = createRunClient().events(
      'run-1',
      (event) => {
        delivered.push(event.sequence);
      },
      (reset) => {
        resets.push(reset);
        projection = [];
        setTimeout(() => finish?.(), 25);
        return 0;
      }
    );

    await cleared;
    controller.abort();

    expect(delivered).toEqual([initial.sequence, highWater.sequence]);
    expect(projection).toEqual([]);
    expect(resets).toEqual([{
      previousEpoch: 'boot-1',
      epoch: 'boot-1',
      responseStatus: 404
    }]);
    expect(streamRequests).toBe(2);
  } finally {
    globalThis.fetch = nativeFetch;
  }
});

test('agent-session epoch changes reconcile lower authoritative detail before replay', async () => {
  const nativeFetch = globalThis.fetch;
  const started = {
    sequence: 1,
    atMs: 1,
    type: 'agent.turn.started',
    payload: { sessionId: 'session-1', turnId: 'turn-1' }
  } satisfies RunEvent;
  const staleHighWater = {
    sequence: 9,
    atMs: 9,
    type: 'observation.assistant.delta',
    payload: { text: 'stale answer' }
  } satisfies RunEvent;
  const interrupted = {
    sequence: 1,
    atMs: 20,
    type: 'agent.session.interrupted',
    payload: { sessionId: 'session-1', recovered: true }
  } satisfies RunEvent;
  let streamRequests = 0;
  let detailRequests = 0;

  globalThis.fetch = async (input: RequestInfo | URL) => {
    const url = new URL(
      typeof input === 'string'
        ? input
        : input instanceof URL
          ? input.href
          : input.url,
      'http://127.0.0.1'
    );
    if (url.pathname === '/api/session-token') {
      return Response.json({ token: 'test-token' });
    }
    if (url.pathname === '/api/workbench/workspace-1/agent-sessions/session-1') {
      detailRequests += 1;
      return Response.json({
        summary: { id: 'session-1', status: 'interrupted' },
        turns: [],
        events: [interrupted]
      });
    }
    if (url.pathname === '/api/workbench/workspace-1/agent-sessions/session-1/events') {
      streamRequests += 1;
      const firstBoot = streamRequests === 1;
      const encoder = new TextEncoder();
      return new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          for (const event of firstBoot ? [started, staleHighWater] : [interrupted]) {
            controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
          }
          controller.close();
        }
      }), {
        status: 200,
        headers: {
          'Content-Type': 'text/event-stream',
          'X-Agent-Lab-Event-Stream-Epoch': firstBoot ? 'boot-1' : 'boot-2'
        }
      });
    }
    throw new Error(`unexpected request: ${url.pathname}`);
  };

  try {
    let finish: (() => void) | undefined;
    const reconciled = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const client = createRunClient();
    const delivered: number[] = [];
    let projection = [started, staleHighWater] as RunEvent[];
    const controller = client.agentSessionEvents(
      'workspace-1',
      'session-1',
      (event) => {
        delivered.push(event.sequence);
      },
      async () => {
        const detail = await client.agentSession('workspace-1', 'session-1');
        projection = detail.events;
        const watermark = detail.events.reduce(
          (sequence, event) => Math.max(sequence, event.sequence),
          0
        );
        setTimeout(() => finish?.(), 25);
        return watermark;
      }
    );

    await reconciled;
    controller.abort();

    expect(delivered).toEqual([started.sequence, staleHighWater.sequence]);
    expect(projection).toEqual([interrupted]);
    expect(detailRequests).toBe(1);
    expect(streamRequests).toBe(2);
  } finally {
    globalThis.fetch = nativeFetch;
  }
});
