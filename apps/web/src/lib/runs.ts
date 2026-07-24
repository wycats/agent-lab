export type RunStatus = 'exploring' | 'starting' | 'running' | 'passed' | 'failed' | 'cancelled';

export interface ScenarioManifest {
  version: number;
  id: string;
  title: string;
  description: string;
  question: string;
  prompt: string;
}

export interface DriverDescriptor {
  name: string;
  version: string;
  revision?: string;
  features: string[];
}

export interface AssemblySnapshot {
  question: string;
  scenario: {
    id: string;
    title: string;
    description: string;
    version: number;
    output: string;
  };
  harness: {
    adapter: string;
    modelId?: string;
    driver?: DriverDescriptor;
  };
  workspace: {
    id: string;
    seed: string;
    seedRevision: string;
    attachment: string;
    changeTracking: string;
  };
  capabilitySources: Array<{
    id: string;
    revision: string;
    protocol: string;
    projections: string[];
  }>;
  limits: {
    maxDurationMs: number;
    maxCommandCount: number;
    maxOrchestratorInvocations: number;
    maxToolInvocations: number;
  };
}

export interface RunSummary {
  id: string;
  scenarioId: string;
  scenarioTitle: string;
  modelId: string;
  harnessId?: string;
  modelProfileId?: string;
  status: RunStatus;
  startedAtMs: number;
  finishedAtMs?: number;
  eventCount: number;
  error?: string;
}

export interface HarnessMetadata {
  id: string;
  displayName: string;
  modelProfileIds: string[];
}

export interface ModelProfileMetadata {
  id: string;
  displayName: string;
  harnessIds: string[];
}

export interface WorkbenchSelection {
  harnessId?: string;
  modelProfileId?: string;
  comparisonHarnessIds: string[];
}

export interface ModelAccessSnapshot {
  id: string;
  displayName: string;
  harnessIds: string[];
  status: 'ready' | 'needs-setup';
  source?: string;
  expiresAtMs?: number;
  message?: string;
  setupHint: string;
}

export interface WorkbenchSnapshot {
  workspaceId: string;
  assembly: AssemblySnapshot;
  selection: WorkbenchSelection;
  harnesses: HarnessMetadata[];
  modelProfiles: ModelProfileMetadata[];
  modelAccess: ModelAccessSnapshot[];
  latestEvaluation?: EvaluationSummary;
  activeAgentSession?: AgentSessionSummary;
  replayAgentSession?: AgentSessionSummary;
  agentSessions: AgentSessionSummary[];
}

export type AgentSessionStatus = 'starting' | 'ready' | 'running' | 'closing' | 'failed' | 'closed' | 'interrupted';
export type AgentTurnStatus = 'queued' | 'running' | 'completed' | 'intervened' | 'failed' | 'cancelled';

export interface AgentSessionSummary {
  id: string;
  workspaceId: string;
  harnessId: string;
  modelProfileId: string;
  modelId: string;
  status: AgentSessionStatus;
  active: boolean;
  createdAtMs: number;
  updatedAtMs: number;
  turnCount: number;
  error?: string;
}

export interface AgentTurnSummary {
  id: string;
  sessionId: string;
  prompt: string;
  input?: unknown | null;
  sourceRevision: string;
  capabilityRevisions: Record<string, string>;
  status: AgentTurnStatus;
  startedAtMs: number;
  finishedAtMs?: number;
  outcome?: string;
  error?: string;
  humanInterventionAtMs?: number;
  presentation?: AgentTurnPresentation;
}

export interface AgentTurnMessagePresentation {
  id: string;
  text: string;
  complete: boolean;
  sourceEventSequences: number[];
}

export interface AgentTurnActivityPresentation {
  kind: string;
  title: string;
  detail: string | null;
  status: string;
  source: string | null;
  path: string | null;
  sourceEventSequences: number[];
}

export interface AgentTurnPresentationCompleteness {
  assistantOutput: 'complete' | 'partial' | 'unavailable';
  capabilityActivity: 'complete' | 'partial' | 'unavailable';
  nativeActivity: 'complete' | 'partial' | 'unavailable';
  workspaceEffects: 'complete' | 'partial' | 'unavailable';
  usage: 'complete' | 'partial' | 'unavailable';
}

export interface AgentTurnPresentation {
  schemaVersion: 1;
  response: string | null;
  messages: AgentTurnMessagePresentation[];
  activity: AgentTurnActivityPresentation[];
  usage: Record<string, unknown> | null;
  completeness: AgentTurnPresentationCompleteness;
  sourceEventSequences: number[];
  sourceDigest: string;
}

export interface AgentSessionDetail {
  projectionVersion: number;
  summary: AgentSessionSummary;
  turns: AgentTurnSummary[];
  events: RunEvent[];
}

export type EvaluationStatus = 'queued' | 'running' | 'passed' | 'failed' | 'cancelled';

export interface EvaluationSummary {
  id: string;
  scenarioId: string;
  modelProfileId: string;
  sourceWorkspaceId: string;
  sourceRevision: string;
  harnessIds: string[];
  arms: Array<{ harnessId: string; runId?: string; status: string }>;
  status: EvaluationStatus;
  startedAtMs: number;
  finishedAtMs?: number;
}

export interface EvaluationDetail {
  summary: EvaluationSummary;
  events: RunEvent[];
  comparison?: {
    version: number;
    sourceRevision: string;
    modelProfileId: string;
    arms: EvaluationComparisonArm[];
    outputsMatch: boolean;
    artifactComparison?: 'same' | 'different' | 'missing';
    outputDiff?: unknown;
  };
}

export interface EvaluationComparisonArm {
  harnessId: string;
  runId?: string;
  status: string;
  score?: Record<string, unknown>;
  metrics?: {
    modelTurns: number;
    capabilityCalls: number;
    nativeActions: number;
    workspaceChanges: number;
    durationMs: number | null;
  };
  output?: unknown;
  firstUsefulAction?: {
    title: string;
    detail?: string | null;
    kind: string;
  };
  evidenceComplete: boolean;
  usage: 'not reported' | { inputTokens: number; outputTokens: number };
  cache: 'not reported' | { readTokens: number; writeTokens: number };
}

export type RunProgressPhase =
  | 'starting'
  | 'preparing'
  | 'reasoning'
  | 'responding'
  | 'acting'
  | 'waiting'
  | 'finalizing';

export interface RunEventProgress {
  phase: RunProgressPhase;
  detail?: string | null;
  source?: string | null;
  sourceEventSequence?: number | null;
  sourceEventType?: string | null;
}

export interface RunEvent {
  sequence: number;
  atMs: number;
  type: string;
  payload: unknown;
  progress?: RunEventProgress;
}

export interface RunReview {
  version: number;
  status: RunStatus;
  metrics: {
    modelTurns: number;
    capabilityCalls: number;
    nativeActions: number;
    workspaceChanges: number;
    durationMs: number | null;
  };
  steps: RunReviewStep[];
}

export interface RunReviewStep {
  ordinal: number;
  kind: string;
  title: string;
  detail: string | null;
  status: string;
  eventSequences: number[];
  source: string | null;
  path: string | null;
}

export interface RunDetail {
  summary: RunSummary;
  assembly: AssemblySnapshot;
  review: RunReview;
  events: RunEvent[];
  score?: unknown;
  output?: unknown;
  outputError?: string;
}

export interface RunClient {
  models(): Promise<string[]>;
  harnesses(): Promise<HarnessMetadata[]>;
  modelProfiles(): Promise<ModelProfileMetadata[]>;
  scenarios(): Promise<ScenarioManifest[]>;
  runs(): Promise<RunSummary[]>;
  prepare(scenarioId: string): Promise<RunSummary>;
  start(scenarioId: string, modelId: string): Promise<RunSummary>;
  startPrepared(id: string, modelId: string): Promise<RunSummary>;
  startPreparedHarness(id: string, harnessId: string, modelProfileId: string): Promise<RunSummary>;
  detail(id: string): Promise<RunDetail>;
  cancel(id: string): Promise<void>;
  events(id: string, onEvent: (event: RunEvent) => void): AbortController;
  evaluations(): Promise<EvaluationSummary[]>;
  startEvaluation(input: {
    scenarioId: string;
    modelProfileId: string;
    sourceWorkspaceId: string;
    harnessIds: string[];
  }): Promise<EvaluationSummary>;
  evaluation(id: string): Promise<EvaluationDetail>;
  cancelEvaluation(id: string): Promise<void>;
  evaluationEvents(id: string, onEvent: (event: RunEvent) => void): AbortController;
  workbench(id: string): Promise<WorkbenchSnapshot>;
  updateWorkbenchSelection(
    id: string,
    input: Partial<WorkbenchSelection>
  ): Promise<WorkbenchSelection>;
  compareWorkbench(
    id: string,
    input?: { modelProfileId?: string; harnessIds?: string[] }
  ): Promise<EvaluationSummary>;
  agentSession(workspaceId: string, sessionId: string): Promise<AgentSessionDetail>;
  cancelAgentTurn(workspaceId: string, sessionId: string): Promise<void>;
  agentSessionEvents(
    workspaceId: string,
    sessionId: string,
    onEvent: (event: RunEvent) => void
  ): AbortController;
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
  path: string,
  signal: AbortSignal,
  onEvent: (event: RunEvent) => void
): Promise<void> {
  const token = await processToken();
  const response = await fetch(path, {
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
    models: () => request('/api/models'),
    harnesses: () => request('/api/harnesses'),
    modelProfiles: () => request('/api/model-profiles'),
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
    startPreparedHarness: (id, harnessId, modelProfileId) =>
      request(`/api/runs/${encodeURIComponent(id)}/start`, {
        method: 'POST',
        body: JSON.stringify({ harnessId, modelProfileId })
      }),
    detail: (id) => request(`/api/runs/${encodeURIComponent(id)}`),
    cancel: (id) => request(`/api/runs/${encodeURIComponent(id)}/cancel`, { method: 'POST' }),
    events(id, onEvent) {
      const controller = new AbortController();
      void streamEvents(`/api/runs/${encodeURIComponent(id)}/events`, controller.signal, onEvent).catch((error) => {
        if (!controller.signal.aborted) console.error(error);
      });
      return controller;
    },
    evaluations: () => request('/api/evaluations'),
    startEvaluation: (input) =>
      request('/api/evaluations', { method: 'POST', body: JSON.stringify(input) }),
    evaluation: (id) => request(`/api/evaluations/${encodeURIComponent(id)}`),
    cancelEvaluation: (id) =>
      request(`/api/evaluations/${encodeURIComponent(id)}/cancel`, { method: 'POST' }),
    evaluationEvents(id, onEvent) {
      const controller = new AbortController();
      void streamEvents(
        `/api/evaluations/${encodeURIComponent(id)}/events`,
        controller.signal,
        onEvent
      ).catch((error) => {
        if (!controller.signal.aborted) console.error(error);
      });
      return controller;
    },
    workbench: (id) => request(`/api/workbench/${encodeURIComponent(id)}`),
    updateWorkbenchSelection: (id, input) =>
      request(`/api/workbench/${encodeURIComponent(id)}/selection`, {
        method: 'PATCH',
        body: JSON.stringify(input)
      }),
    compareWorkbench: (id, input = {}) =>
      request(`/api/workbench/${encodeURIComponent(id)}/compare`, {
        method: 'POST',
        body: JSON.stringify(input)
      }),
    agentSession: (workspaceId, sessionId) =>
      request(`/api/workbench/${encodeURIComponent(workspaceId)}/agent-sessions/${encodeURIComponent(sessionId)}`),
    cancelAgentTurn: (workspaceId, sessionId) =>
      request(
        `/api/workbench/${encodeURIComponent(workspaceId)}/agent-sessions/${encodeURIComponent(sessionId)}/cancel`,
        { method: 'POST' }
      ),
    agentSessionEvents(workspaceId, sessionId, onEvent) {
      const controller = new AbortController();
      void streamEvents(
        `/api/workbench/${encodeURIComponent(workspaceId)}/agent-sessions/${encodeURIComponent(sessionId)}/events`,
        controller.signal,
        onEvent
      ).catch((error) => {
        if (!controller.signal.aborted) console.error(error);
      });
      return controller;
    }
  };
}
