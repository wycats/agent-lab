export type RunStatus = 'exploring' | 'starting' | 'running' | 'passed' | 'failed' | 'cancelled';

export interface ScenarioManifest {
  version: number;
  id: string;
  title: string;
  description: string;
  prompt: string;
}

export interface RunSummary {
  id: string;
  scenarioId: string;
  scenarioTitle: string;
  modelId: string;
  status: RunStatus;
  startedAtMs: number;
  finishedAtMs?: number;
  eventCount: number;
  error?: string;
}

export interface RunEvent {
  sequence: number;
  atMs: number;
  type: string;
  payload: unknown;
}

export interface RunDetail {
  summary: RunSummary;
  events: RunEvent[];
  score?: unknown;
  output?: unknown;
}

export interface RunClient {
  scenarios(): Promise<ScenarioManifest[]>;
  runs(): Promise<RunSummary[]>;
  prepare(scenarioId: string): Promise<RunSummary>;
  start(scenarioId: string, modelId: string): Promise<RunSummary>;
  startPrepared(id: string, modelId: string): Promise<RunSummary>;
  detail(id: string): Promise<RunDetail>;
  cancel(id: string): Promise<void>;
  events(id: string, onEvent: (event: RunEvent) => void): AbortController;
}

async function processToken(): Promise<string> {
  const response = await fetch('/api/session-token', { cache: 'no-store' });
  if (!response.ok) throw new Error(`session token request failed with HTTP ${response.status}`);
  const value: unknown = await response.json();
  if (!value || typeof value !== 'object' || typeof (value as { token?: unknown }).token !== 'string') {
    throw new Error('session token response was malformed');
  }
  return (value as { token: string }).token;
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = await processToken();
  const headers = new Headers(init.headers);
  headers.set('Authorization', `Bearer ${token}`);
  if (init.body) headers.set('Content-Type', 'application/json');
  const response = await fetch(path, { ...init, headers, cache: 'no-store' });
  if (!response.ok) {
    let message = `request failed with HTTP ${response.status}`;
    try {
      const payload = (await response.json()) as { error?: string };
      if (payload.error) message = payload.error;
    } catch {
      // Preserve the status-based error when the body is not JSON.
    }
    throw new Error(message);
  }
  if (response.status === 202 || response.status === 204) return undefined as T;
  return (await response.json()) as T;
}

async function streamEvents(
  id: string,
  signal: AbortSignal,
  onEvent: (event: RunEvent) => void
): Promise<void> {
  const token = await processToken();
  const response = await fetch(`/api/runs/${encodeURIComponent(id)}/events`, {
    headers: { Authorization: `Bearer ${token}` },
    cache: 'no-store',
    signal
  });
  if (!response.ok || !response.body) {
    throw new Error(`event stream failed with HTTP ${response.status}`);
  }
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = '';
  while (!signal.aborted) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += value;
    let boundary = buffer.indexOf('\n\n');
    while (boundary !== -1) {
      const frame = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      const data = frame
        .split('\n')
        .filter((line) => line.startsWith('data:'))
        .map((line) => line.slice(5).trimStart())
        .join('\n');
      if (data) onEvent(JSON.parse(data) as RunEvent);
      boundary = buffer.indexOf('\n\n');
    }
  }
}

export function createRunClient(): RunClient {
  return {
    scenarios: () => request('/api/scenarios'),
    runs: () => request('/api/runs'),
    prepare: (scenarioId) =>
      request('/api/explore', {
        method: 'POST',
        body: JSON.stringify({ scenarioId })
      }),
    start: (scenarioId, modelId) =>
      request('/api/runs', {
        method: 'POST',
        body: JSON.stringify({ scenarioId, modelId })
      }),
    startPrepared: (id, modelId) =>
      request(`/api/runs/${encodeURIComponent(id)}/start`, {
        method: 'POST',
        body: JSON.stringify({ modelId })
      }),
    detail: (id) => request(`/api/runs/${encodeURIComponent(id)}`),
    cancel: (id) => request(`/api/runs/${encodeURIComponent(id)}/cancel`, { method: 'POST' }),
    events(id, onEvent) {
      const controller = new AbortController();
      void streamEvents(id, controller.signal, onEvent).catch((error) => {
        if (!controller.signal.aborted) console.error(error);
      });
      return controller;
    }
  };
}
