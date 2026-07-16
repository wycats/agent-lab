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

export async function connectSession(
  surface: TerminalSurface,
  callbacks: SessionCallbacks
): Promise<BrowserSession> {
  callbacks.onState('starting');
  const tokenResponse = await fetch('/api/session-token', { cache: 'no-store' });
  if (!tokenResponse.ok) {
    throw new Error(`session token request failed with HTTP ${tokenResponse.status}`);
  }
  const { token } = (await tokenResponse.json()) as { token: string };
  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const dimensions = surface.dimensions;
  const url = new URL(`${protocol}//${location.host}/api/terminal`);
  url.searchParams.set('token', token);
  url.searchParams.set('cols', String(dimensions.cols));
  url.searchParams.set('rows', String(dimensions.rows));

  const socket = new WebSocket(url);
  socket.binaryType = 'arraybuffer';
  const encoder = new TextEncoder();
  const input = surface.onData((data) => {
    if (socket.readyState === WebSocket.OPEN) socket.send(encoder.encode(data));
  });
  const resize = surface.onResize(({ cols, rows }) => {
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ type: 'resize', cols, rows }));
    }
  });

  socket.addEventListener('open', () => callbacks.onState('connected'));
  socket.addEventListener('message', (message) => {
    if (typeof message.data === 'string') {
      callbacks.onEvent(JSON.parse(message.data) as SessionEvent);
    } else if (message.data instanceof ArrayBuffer) {
      surface.write(new Uint8Array(message.data));
      callbacks.onScreen(surface.readText());
    } else if (message.data instanceof Blob) {
      void message.data.arrayBuffer().then((data) => {
        surface.write(new Uint8Array(data));
        callbacks.onScreen(surface.readText());
      });
    }
  });
  socket.addEventListener('error', () => callbacks.onState('error'));
  socket.addEventListener('close', () => callbacks.onState('closed'));

  return {
    dispose() {
      input.dispose();
      resize.dispose();
      socket.close();
    }
  };
}
