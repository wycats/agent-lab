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
        headers: { 'Content-Type': 'text/event-stream' }
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
    const controller = createRunClient().events('run-1', (event) => {
      delivered.push(event.sequence);
      if (event.sequence === second.sequence) finish?.();
    });

    await receivedReplay;
    controller.abort();

    expect(delivered).toEqual([first.sequence, second.sequence]);
    expect(streamRequests).toBe(2);
    expect(tokenRequests).toBe(2);
    expect(streamRequestTimes[1] - streamRequestTimes[0]).toBeGreaterThanOrEqual(75);
  } finally {
    globalThis.fetch = nativeFetch;
  }
});
