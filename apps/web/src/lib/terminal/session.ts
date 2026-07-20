import type { Disposable, TerminalSurface } from './surface';

export type ConnectionState = 'starting' | 'connected' | 'closed' | 'error';

export type SessionEvent =
  | { type: 'started'; provider: string; cols: number; rows: number }
  | { type: 'resized'; cols: number; rows: number }
  | { type: 'exited' }
  | { type: 'error'; message: string };

export interface SessionCallbacks {
  onState(state: ConnectionState): void;
  onEvent(event: SessionEvent): void;
  onScreen(text: string): void;
}

export interface BrowserSession extends Disposable {}

function parseSessionEvent(payload: string): SessionEvent | undefined {
  let value: unknown;
  try {
    value = JSON.parse(payload);
  } catch {
    return undefined;
  }
  if (!value || typeof value !== 'object') return undefined;

  const event = value as Record<string, unknown>;
  if (event.type === 'exited') return { type: 'exited' };
  if (event.type === 'error' && typeof event.message === 'string') {
    return { type: 'error', message: event.message };
  }
  if (
    (event.type === 'started' || event.type === 'resized') &&
    Number.isSafeInteger(event.cols) &&
    Number.isSafeInteger(event.rows)
  ) {
    if (event.type === 'resized') {
      return { type: 'resized', cols: event.cols as number, rows: event.rows as number };
    }
    if (typeof event.provider === 'string') {
      return {
        type: 'started',
        provider: event.provider,
        cols: event.cols as number,
        rows: event.rows as number
      };
    }
  }
  return undefined;
}

export async function connectSession(
  surface: TerminalSurface,
  callbacks: SessionCallbacks,
  runId?: string
): Promise<BrowserSession> {
  callbacks.onState('starting');
  const tokenResponse = await fetch('/api/session-token', { cache: 'no-store' });
  if (!tokenResponse.ok) {
    throw new Error(`session token request failed with HTTP ${tokenResponse.status}`);
  }
  const tokenPayload: unknown = await tokenResponse.json();
  if (
    !tokenPayload ||
    typeof tokenPayload !== 'object' ||
    typeof (tokenPayload as Record<string, unknown>).token !== 'string' ||
    !/^[0-9a-f]{64}$/.test((tokenPayload as Record<string, unknown>).token as string)
  ) {
    throw new Error('session token response was malformed');
  }
  const token = (tokenPayload as { token: string }).token;
  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const dimensions = surface.dimensions;
  const url = new URL(`${protocol}//${location.host}/api/terminal`);
  url.searchParams.set('cols', String(dimensions.cols));
  url.searchParams.set('rows', String(dimensions.rows));
  if (runId) url.searchParams.set('runId', runId);

  const socket = new WebSocket(url, [`agent-lab.auth.${token}`]);
  socket.binaryType = 'arraybuffer';
  let disposed = false;
  let screenRefreshTimer: ReturnType<typeof setTimeout> | undefined;
  const flushScreen = () => {
    if (screenRefreshTimer !== undefined) {
      clearTimeout(screenRefreshTimer);
      screenRefreshTimer = undefined;
    }
    if (!disposed) callbacks.onScreen(surface.readText());
  };
  const scheduleScreenRefresh = () => {
    if (disposed) return;
    if (screenRefreshTimer !== undefined) clearTimeout(screenRefreshTimer);
    screenRefreshTimer = setTimeout(flushScreen, 150);
  };
  const encoder = new TextEncoder();
  const input = surface.onData((data) => {
    if (socket.readyState === WebSocket.OPEN) socket.send(encoder.encode(data));
  });
  const resize = surface.onResize(({ cols, rows }) => {
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ type: 'resize', cols, rows }));
    }
    flushScreen();
  });
  const scroll = surface.onScroll(flushScreen);
  let sawError = false;

  socket.addEventListener('open', () => {
    if (!disposed) callbacks.onState('connected');
  });
  socket.addEventListener('message', (message) => {
    if (disposed) return;
    if (typeof message.data === 'string') {
      const event = parseSessionEvent(message.data);
      if (event) {
        if (event.type === 'error') {
          sawError = true;
          callbacks.onState('error');
        }
        callbacks.onEvent(event);
      }
    } else if (message.data instanceof ArrayBuffer) {
      surface.write(new Uint8Array(message.data), scheduleScreenRefresh);
    } else if (message.data instanceof Blob) {
      void message.data.arrayBuffer().then((data) => {
        if (disposed) return;
        surface.write(new Uint8Array(data), scheduleScreenRefresh);
      });
    }
  });
  socket.addEventListener('error', () => {
    sawError = true;
    if (!disposed) callbacks.onState('error');
  });
  socket.addEventListener('close', () => {
    if (!disposed && !sawError) callbacks.onState('closed');
  });

  return {
    dispose() {
      disposed = true;
      if (screenRefreshTimer !== undefined) clearTimeout(screenRefreshTimer);
      input.dispose();
      resize.dispose();
      scroll.dispose();
      socket.close();
    }
  };
}
