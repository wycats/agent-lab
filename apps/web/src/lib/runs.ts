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
  agentTurnIndex: AgentTurnCompletionIndex;
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

export interface AgentTurnCompletionRef {
  id: string;
  sessionId: string;
  startedAtMs: number;
}

export interface AgentTurnCompletionIndex {
  entries: AgentTurnCompletionRef[];
  total: number;
  truncated: boolean;
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
  operation?: string | null;
  callId?: string | null;
  arguments?: unknown;
  result?: unknown;
  actionId?: string | null;
  changeKind?: string | null;
  entryType?: string | null;
  beforeMode?: string | null;
  afterMode?: string | null;
  sourceEventSequences: number[];
}

function countedActivityLabel(count: number, singular: string): string {
  return `${count} ${singular}${count === 1 ? '' : 's'}`;
}

function conciseActivityValue(value: string | number | boolean): string {
  const rendered = typeof value === 'string' ? value : String(value);
  return rendered.length <= 80 ? rendered : `${rendered.slice(0, 79)}…`;
}

function agentActivityArgumentsSummary(value: unknown): string | null {
  if (value === undefined || value === null) return null;
  if (Array.isArray(value)) return `Arguments: ${countedActivityLabel(value.length, 'item')}`;
  if (typeof value === 'object') {
    const keys = Object.keys(value);
    if (keys.length === 0) return null;
    return keys.length <= 3
      ? `Arguments: ${keys.join(', ')}`
      : `Arguments: ${countedActivityLabel(keys.length, 'field')}`;
  }
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return `Argument: ${conciseActivityValue(value)}`;
  }
  return null;
}

function agentActivityResultSummary(value: unknown, failed: boolean): string | null {
  if (value === undefined) return null;
  if (value === null) return failed ? 'Capability failed' : 'Returned no value';
  if (Array.isArray(value)) {
    return `${failed ? 'Failed with' : 'Returned'} ${countedActivityLabel(value.length, 'item')}`;
  }
  if (typeof value === 'object') {
    const record = value as Record<string, unknown>;
    const message = record.message ?? record.error;
    if (failed && typeof message === 'string') return `Failed: ${conciseActivityValue(message)}`;
    if (Array.isArray(record.items)) {
      return `${failed ? 'Failed with' : 'Returned'} ${countedActivityLabel(record.items.length, 'item')}`;
    }
    const fieldCount = Object.keys(record).length;
    return `${failed ? 'Failed with' : 'Returned'} ${countedActivityLabel(fieldCount, 'field')}`;
  }
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return `${failed ? 'Failed' : 'Returned'}: ${conciseActivityValue(value)}`;
  }
  return null;
}

export function agentTurnActivityDetail(activity: AgentTurnActivityPresentation): string | null {
  if (activity.detail) return activity.detail;
  if (activity.kind !== 'capability-call') return null;

  const parts = [
    agentActivityArgumentsSummary(activity.arguments),
    agentActivityResultSummary(activity.result, activity.status === 'failed')
  ].filter((part): part is string => part !== null);
  return parts.length > 0 ? parts.join(' · ') : null;
}

export interface AgentTurnPresentationCompleteness {
  assistantOutput: 'complete' | 'partial' | 'unavailable';
  capabilityActivity: 'complete' | 'partial' | 'unavailable';
  nativeActivity: 'complete' | 'partial' | 'unavailable';
  workspaceEffects: 'complete' | 'partial' | 'unavailable';
  usage: 'complete' | 'partial' | 'unavailable';
}

export interface AgentTurnPresentation {
  schemaVersion: 1 | 2;
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
  definitionId?: string;
  definitionRevisionId?: string;
  harnessIds: string[];
  arms: Array<{ harnessId: string; runId?: string; status: string }>;
  status: EvaluationStatus;
  startedAtMs: number;
  finishedAtMs?: number;
}

export interface ScenarioLimits {
  maxDurationMs: number;
  maxCommandCount: number;
  maxOrchestratorInvocations: number;
  maxToolInvocations: number;
}

export interface CatalogEvaluatorParameters {
  activeNames: string[];
  totalScore: number;
  requiredCapabilitySources: string[];
  outputPath: string;
  requireSchema: boolean;
}

export interface EvaluationEvaluator {
  id: string;
  version: number;
  parameters: CatalogEvaluatorParameters;
}

export interface EvaluationSourceProvenance {
  workspaceId: string;
  sessionId: string;
  turnIds: string[];
  sourceRevision: string;
  sourceDigest: string;
  capabilityRevisions: Record<string, string>;
  sourceEventSequences: number[];
  scenarioId: string;
  harnessId: string;
  modelProfileId: string;
  modelId: string;
  driver?: {
    descriptor: {
      name: string;
      version: string;
      revision?: string;
      features: string[];
    };
    launchDigest: string;
  };
}

export interface EvaluationRevision {
  schemaVersion: number;
  id: string;
  draftId: string;
  previousRevisionId?: string;
  createdAtMs: number;
  task: string;
  source: EvaluationSourceProvenance;
  limits: ScenarioLimits;
  evaluator: EvaluationEvaluator;
  measurements: string[];
  blockingIssues: string[];
}

export type EvaluationExecutionStatus =
  | 'queued'
  | 'running'
  | 'complete'
  | 'inconclusive'
  | 'cancelled'
  | 'intervened';
export type ValidationAssertionStatus = 'passed' | 'failed' | 'not-evaluated';

export interface EvaluationValidationAttempt {
  id: string;
  draftId: string;
  revisionId: string;
  executionStatus: EvaluationExecutionStatus;
  assertionStatus: ValidationAssertionStatus;
  harnessId: string;
  modelProfileId: string;
  runId?: string;
  startedAtMs: number;
  finishedAtMs?: number;
  error?: string;
  score?: unknown;
}

export interface EvaluationDraftSummary {
  id: string;
  workspaceId: string;
  name: string;
  currentRevisionId: string;
  status: string;
  saved: boolean;
  definitionId?: string;
  promotedRevisionId?: string;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface EvaluationDraftDetail {
  summary: EvaluationDraftSummary;
  revisions: EvaluationRevision[];
  validations: EvaluationValidationAttempt[];
  events: RunEvent[];
}

export interface EvaluationDefinitionSummary {
  id: string;
  name: string;
  draftId: string;
  revisionId: string;
  createdAtMs: number;
}

export interface EvaluationDefinitionDetail {
  summary: EvaluationDefinitionSummary;
  revision: EvaluationRevision;
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
  prepare(scenarioId: string, sourceWorkspaceId?: string): Promise<RunSummary>;
  start(scenarioId: string, modelId: string): Promise<RunSummary>;
  startPrepared(id: string, modelId: string): Promise<RunSummary>;
  startPreparedHarness(id: string, harnessId: string, modelProfileId: string): Promise<RunSummary>;
  detail(id: string): Promise<RunDetail>;
  cancel(id: string): Promise<void>;
  events(
    id: string,
    onEvent: (event: RunEvent) => void | Promise<void>,
    onReset?: (reset: RunEventStreamReset) => number | Promise<number>
  ): AbortController;
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
    onEvent: (event: RunEvent) => void | Promise<void>,
    onReset?: (reset: RunEventStreamReset) => number | Promise<number>
  ): AbortController;
  evaluationLibraryDrafts(): Promise<EvaluationDraftSummary[]>;
  createEvaluationDraft(
    workspaceId: string,
    input: { sessionId?: string; fromTurnId: string; throughTurnId: string }
  ): Promise<EvaluationDraftDetail>;
  evaluationLibraryDraft(draftId: string): Promise<EvaluationDraftDetail>;
  updateEvaluationDraft(
    workspaceId: string,
    draftId: string,
    input: {
      baseRevisionId: string;
      name?: string;
      revision: Partial<Pick<EvaluationRevision, 'task' | 'limits' | 'evaluator' | 'measurements'>>;
    }
  ): Promise<EvaluationDraftDetail>;
  validateEvaluationDraft(
    workspaceId: string,
    draftId: string,
    revisionId?: string
  ): Promise<EvaluationValidationAttempt>;
  cancelEvaluationValidation(
    workspaceId: string,
    draftId: string,
    validationId: string
  ): Promise<void>;
  saveEvaluationDraft(
    workspaceId: string,
    draftId: string,
    input?: { revisionId?: string; name?: string }
  ): Promise<EvaluationDraftDetail>;
  evaluationDraftEvents(
    workspaceId: string,
    draftId: string,
    onEvent: (event: RunEvent) => void | Promise<void>
  ): AbortController;
  evaluationLibraryDefinitions(): Promise<EvaluationDefinitionSummary[]>;
  evaluationLibraryDefinition(definitionId: string): Promise<EvaluationDefinitionDetail>;
  runEvaluationDefinition(
    workspaceId: string,
    definitionId: string,
    input?: { modelProfileId?: string; harnessIds?: string[] }
  ): Promise<EvaluationSummary>;
}

export interface RunEventStreamReset {
  previousEpoch?: string;
  epoch: string;
  responseStatus: number;
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
  if (response.status === 204) return undefined as T;
  const body = await response.text();
  if (body.length === 0) return undefined as T;
  return JSON.parse(body) as T;
}

async function streamEvents(
  path: string,
  signal: AbortSignal,
  onEvent: (event: RunEvent) => void | Promise<void>,
  onOpen?: (response: Response) => void | Promise<void>
): Promise<void> {
  const token = await processToken();
  const response = await fetch(path, {
    headers: { Authorization: `Bearer ${token}` },
    cache: 'no-store',
    signal
  });
  if (!response.body) {
    await onOpen?.(response);
    throw new Error(`event stream failed with HTTP ${response.status}`);
  }
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  let reachedEof = false;
  try {
    await onOpen?.(response);
    if (!response.ok) {
      throw new Error(`event stream failed with HTTP ${response.status}`);
    }
    let buffer = '';
    while (!signal.aborted) {
      const { value, done } = await reader.read();
      if (done) {
        reachedEof = true;
        break;
      }
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
        if (data) await onEvent(JSON.parse(data) as RunEvent);
        boundary = buffer.indexOf('\n\n');
      }
    }
  } finally {
    if (!reachedEof) {
      try {
        await reader.cancel();
      } catch {
        // Fetch aborts can close the body before cancellation reaches the reader.
      }
    }
    reader.releaseLock();
  }
}

const EVENT_STREAM_RECONNECT_INITIAL_MS = 100;
const EVENT_STREAM_RECONNECT_MAX_MS = 2_000;
const EVENT_STREAM_RECONNECT_STABLE_MS = 5_000;
const EVENT_STREAM_EPOCH_HEADER = 'X-Agent-Lab-Event-Stream-Epoch';

function waitForAbortableDelay(milliseconds: number, signal: AbortSignal): Promise<boolean> {
  if (signal.aborted) return Promise.resolve(false);
  return new Promise((resolve) => {
    let settled = false;
    const finish = (elapsed: boolean) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      signal.removeEventListener('abort', abort);
      resolve(elapsed);
    };
    const timer = setTimeout(() => finish(true), milliseconds);
    const abort = () => finish(false);
    signal.addEventListener('abort', abort, { once: true });
    if (signal.aborted) abort();
  });
}

function reconnectingRunEvents(
  path: string,
  onEvent: (event: RunEvent) => void | Promise<void>,
  onReset?: (reset: RunEventStreamReset) => number | Promise<number>
): AbortController {
  const controller = new AbortController();
  void (async () => {
    let retryDelayMs = EVENT_STREAM_RECONNECT_INITIAL_MS;
    let lastDeliveredSequence = 0;
    let eventStreamEpoch: string | undefined;
    let unavailableResponseHandled = false;
    while (!controller.signal.aborted) {
      let connectedAtMs: number | undefined;
      try {
        await streamEvents(
          path,
          controller.signal,
          async (event) => {
            if (event.sequence <= lastDeliveredSequence) return;
            await onEvent(event);
            lastDeliveredSequence = event.sequence;
          },
          async (response) => {
            connectedAtMs = Date.now();
            const responseEpoch = response.headers.get(EVENT_STREAM_EPOCH_HEADER) ?? undefined;
            if (!responseEpoch) return;
            const epochChanged = eventStreamEpoch !== undefined && responseEpoch !== eventStreamEpoch;
            const unavailable = !response.ok;
            const availabilityChanged = unavailable && !unavailableResponseHandled;
            if (!epochChanged && !availabilityChanged) {
              eventStreamEpoch = responseEpoch;
              if (!unavailable) unavailableResponseHandled = false;
              return;
            }
            const reconciledSequence = await onReset?.({
              previousEpoch: eventStreamEpoch,
              epoch: responseEpoch,
              responseStatus: response.status
            }) ?? 0;
            if (!Number.isSafeInteger(reconciledSequence) || reconciledSequence < 0) {
              throw new Error('run event stream reset returned an invalid sequence');
            }
            eventStreamEpoch = responseEpoch;
            lastDeliveredSequence = reconciledSequence;
            unavailableResponseHandled = unavailable;
          }
        );
      } catch {
        // A later connection replays the run's durable event history.
      }
      if (controller.signal.aborted) return;
      if (
        connectedAtMs !== undefined &&
        Date.now() - connectedAtMs >= EVENT_STREAM_RECONNECT_STABLE_MS
      ) {
        retryDelayMs = EVENT_STREAM_RECONNECT_INITIAL_MS;
      }
      if (!(await waitForAbortableDelay(retryDelayMs, controller.signal))) return;
      retryDelayMs = Math.min(retryDelayMs * 2, EVENT_STREAM_RECONNECT_MAX_MS);
    }
  })();
  return controller;
}

export function createRunClient(): RunClient {
  return {
    models: () => request('/api/models'),
    harnesses: () => request('/api/harnesses'),
    modelProfiles: () => request('/api/model-profiles'),
    scenarios: () => request('/api/scenarios'),
    runs: () => request('/api/runs'),
    prepare: (scenarioId, sourceWorkspaceId) =>
      request('/api/explore', {
        method: 'POST',
        body: JSON.stringify({ scenarioId, sourceWorkspaceId })
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
    events: (id, onEvent, onReset) =>
      reconnectingRunEvents(`/api/runs/${encodeURIComponent(id)}/events`, onEvent, onReset),
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
    agentSessionEvents(workspaceId, sessionId, onEvent, onReset) {
      const path = `/api/workbench/${encodeURIComponent(workspaceId)}/agent-sessions/${encodeURIComponent(sessionId)}/events`;
      return reconnectingRunEvents(path, onEvent, onReset);
    },
    // Library reads are intentionally process-scoped so saved work remains
    // inspectable while the builder explores another scenario. Mutations and
    // event streams remain authorized through their owning workspace.
    evaluationLibraryDrafts: () => request('/api/evaluation-drafts'),
    createEvaluationDraft: (workspaceId, input) =>
      request(`/api/workbench/${encodeURIComponent(workspaceId)}/evaluation-drafts`, {
        method: 'POST',
        body: JSON.stringify(input)
      }),
    evaluationLibraryDraft: (draftId) =>
      request(`/api/evaluation-drafts/${encodeURIComponent(draftId)}`),
    updateEvaluationDraft: (workspaceId, draftId, input) =>
      request(
        `/api/workbench/${encodeURIComponent(workspaceId)}/evaluation-drafts/${encodeURIComponent(draftId)}`,
        { method: 'PATCH', body: JSON.stringify(input) }
      ),
    validateEvaluationDraft: (workspaceId, draftId, revisionId) =>
      request(
        `/api/workbench/${encodeURIComponent(workspaceId)}/evaluation-drafts/${encodeURIComponent(draftId)}/validate`,
        { method: 'POST', body: JSON.stringify({ revisionId }) }
      ),
    cancelEvaluationValidation: (workspaceId, draftId, validationId) =>
      request(
        `/api/workbench/${encodeURIComponent(workspaceId)}/evaluation-drafts/${encodeURIComponent(draftId)}/validations/${encodeURIComponent(validationId)}/cancel`,
        { method: 'POST' }
      ),
    saveEvaluationDraft: (workspaceId, draftId, input = {}) =>
      request(
        `/api/workbench/${encodeURIComponent(workspaceId)}/evaluation-drafts/${encodeURIComponent(draftId)}/save`,
        { method: 'POST', body: JSON.stringify(input) }
      ),
    evaluationDraftEvents(workspaceId, draftId, onEvent) {
      const path = `/api/workbench/${encodeURIComponent(workspaceId)}/evaluation-drafts/${encodeURIComponent(draftId)}/events`;
      return reconnectingRunEvents(path, onEvent);
    },
    evaluationLibraryDefinitions: () => request('/api/evaluation-definitions'),
    evaluationLibraryDefinition: (definitionId) =>
      request(`/api/evaluation-definitions/${encodeURIComponent(definitionId)}`),
    runEvaluationDefinition: (workspaceId, definitionId, input = {}) =>
      request(
        `/api/workbench/${encodeURIComponent(workspaceId)}/evaluation-definitions/${encodeURIComponent(definitionId)}/run`,
        { method: 'POST', body: JSON.stringify(input) }
      )
  };
}
