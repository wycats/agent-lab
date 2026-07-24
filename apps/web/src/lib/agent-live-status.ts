import type {
  AgentSessionDetail,
  AgentTurnSummary,
  RunEventProgress,
  RunProgressPhase
} from './runs';

export type AgentLivePhase = RunProgressPhase | 'running' | 'cancelling';

export interface AgentSessionLiveStatusModel {
  phase: AgentLivePhase;
  detail: string | null;
  source: string | null;
  sourceEventSequence: number | null;
  sourceEventType: string | null;
  startedAtMs: number;
  cancellable: boolean;
}

const ACTIVE_TURN_STATUSES = new Set<AgentTurnSummary['status']>(['queued', 'running']);

function activeTurn(session: AgentSessionDetail): AgentTurnSummary | undefined {
  for (let index = session.turns.length - 1; index >= 0; index -= 1) {
    const turn = session.turns[index];
    if (ACTIVE_TURN_STATUSES.has(turn.status)) return turn;
  }
  return undefined;
}

function latestProgress(
  session: AgentSessionDetail,
  startedAtMs: number
): RunEventProgress | undefined {
  for (let index = session.events.length - 1; index >= 0; index -= 1) {
    const event = session.events[index];
    if (event.atMs >= startedAtMs && event.progress) return event.progress;
  }
  return undefined;
}

export function projectAgentSessionLiveStatus(
  session: AgentSessionDetail | undefined,
  cancelling: boolean
): AgentSessionLiveStatusModel | null {
  if (!session) return null;
  if (
    session.summary.status === 'failed' ||
    session.summary.status === 'closed' ||
    session.summary.status === 'interrupted'
  ) return null;

  const turn = activeTurn(session);
  const sessionStarting = session.summary.status === 'starting';
  const sessionRunning = session.summary.status === 'running';
  if (!turn && !sessionStarting && !sessionRunning) return null;

  const startedAtMs = turn?.startedAtMs ?? (
    sessionStarting ? session.summary.createdAtMs : session.summary.updatedAtMs
  );
  const progress = latestProgress(session, startedAtMs);

  if (cancelling && turn) {
    return {
      phase: 'cancelling',
      detail: 'Waiting for the harness to stop this turn.',
      source: progress?.source ?? null,
      sourceEventSequence: progress?.sourceEventSequence ?? null,
      sourceEventType: progress?.sourceEventType ?? null,
      startedAtMs,
      cancellable: true
    };
  }

  if (progress) {
    return {
      phase: progress.phase,
      detail: progress.detail ?? null,
      source: progress.source ?? null,
      sourceEventSequence: progress.sourceEventSequence ?? null,
      sourceEventType: progress.sourceEventType ?? null,
      startedAtMs,
      cancellable: Boolean(turn)
    };
  }

  return {
    phase: sessionStarting ? 'starting' : 'running',
    detail: sessionStarting
      ? 'Preparing the agent session.'
      : 'Waiting for the next progress update.',
    source: null,
    sourceEventSequence: null,
    sourceEventType: null,
    startedAtMs,
    cancellable: Boolean(turn)
  };
}
