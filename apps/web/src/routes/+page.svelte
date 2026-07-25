<script lang="ts">
  import '@fontsource-variable/geist';
  import '@fontsource-variable/geist-mono';
  import { onMount } from 'svelte';
  import AgentSessionLiveStatus from '$lib/AgentSessionLiveStatus.svelte';
  import AssistantMarkdown from '$lib/AssistantMarkdown.svelte';
  import { projectAgentSessionLiveStatus } from '$lib/agent-live-status';
  import { agentTurnActivityDetail, createRunClient, type AgentSessionDetail, type AgentSessionSummary, type AgentTurnActivityPresentation, type AgentTurnMessagePresentation, type AgentTurnSummary, type EvaluationComparisonArm, type EvaluationDetail, type EvaluationSummary, type HarnessMetadata, type ModelAccessSnapshot, type ModelProfileMetadata, type RunDetail, type RunEvent, type RunEventStreamReset, type RunReviewStep, type RunSummary, type ScenarioManifest, type WorkbenchSelection } from '$lib/runs';
  import { createGhosttySurface } from '$lib/terminal/ghostty';
  import { connectSession } from '$lib/terminal/session';
  import type { BrowserSession, ConnectionState, SessionEvent } from '$lib/terminal/session';
  import type { TerminalSurface } from '$lib/terminal/surface';

  type Tab = 'agent' | 'workspace' | 'editor' | 'evidence' | 'evaluation';
  type AgentInspectionMode = 'session' | 'run';
  type AgentSessionReconcileTarget = {
    workspaceId: string;
    sessionId: string;
    openVersion: number;
    evidenceGeneration: number;
    reveal: boolean;
    replaceCurrent: boolean;
  };
  type AgentView = 'review' | 'raw';
  type AgentAnswerView = 'rendered' | 'source';
  type UnavailableRunEvidence = {
    summary: RunSummary;
    score: {
      passed: boolean;
      evidenceQuarantined: boolean;
    };
  };
  type BehaviorSegment = {
    key: string;
    label: string;
    kind: string;
    steps: RunReviewStep[];
    startMs: number | null;
    endMs: number | null;
  };
  type BehaviorRow = {
    key: string;
    label: string;
    kind: string;
    segments: Record<string, BehaviorSegment | undefined>;
  };

  const AGENT_SESSION_RECONCILE_INITIAL_MS = 100;
  const AGENT_SESSION_RECONCILE_MAX_MS = 2_000;
  const RUN_EVIDENCE_UNAVAILABLE =
    'Run evidence is unavailable. It has been removed from this workbench.';
  const EVALUATION_EVIDENCE_UNAVAILABLE =
    'Evaluation evidence is unavailable. It has been removed from this workbench.';

  function agentSessionIsHistorical(summary: AgentSessionDetail['summary']): boolean {
    return summary.status === 'failed' || summary.status === 'closed' || summary.status === 'interrupted';
  }

  const runClient = createRunClient();
  let terminalHost: HTMLDivElement;
  let surface: TerminalSurface | undefined;
  let session: BrowserSession | undefined;
  let reviewRefreshTimer: ReturnType<typeof setTimeout> | undefined;
  let runReviewGeneration = 0;
  let exploreEventStream: AbortController | undefined;
  let inspectionEventStream: AbortController | undefined;
  let evaluationRefreshTimer: ReturnType<typeof setTimeout> | undefined;
  let agentSessionEventStream: AbortController | undefined;
  let agentSessionEventFlushTimer: ReturnType<typeof setTimeout> | undefined;
  let pendingAgentSessionEvents: RunEvent[] = [];
  let knownAgentSessionEventSequences = new Set<number>();
  let agentSessionReconcileInFlight: Promise<void> | undefined;
  let agentSessionReconcileTarget: AgentSessionReconcileTarget | undefined;
  let agentSessionReconcileRetryTimer: ReturnType<typeof setTimeout> | undefined;
  let agentSessionReconcileRetryDelayMs = AGENT_SESSION_RECONCILE_INITIAL_MS;
  let agentSessionOpenRequestVersion = 0;
  let agentSessionOpenVersion = 0;
  let agentSessionEvidenceGeneration = 0;
  let connectionState: ConnectionState = 'starting';
  let sessionEvents: SessionEvent[] = [];
  let screenText = '';
  let startupError = '';
  let scenarios: ScenarioManifest[] = [];
  let models: string[] = [];
  let scenarioId = '';
  let modelId = '';
  let harnesses: HarnessMetadata[] = [];
  let modelProfiles: ModelProfileMetadata[] = [];
  let harnessId = '';
  let modelProfileId = '';
  let comparisonHarnessIds: string[] = [];
  let modelAccess: ModelAccessSnapshot[] = [];
  let runs: RunSummary[] = [];
  let evaluations: EvaluationSummary[] = [];
  let selectedEvaluation: EvaluationDetail | undefined;
  let activeAgentSession: AgentSessionDetail | undefined;
  let agentSessions: AgentSessionSummary[] = [];
  let agentSessionsWorkspaceId = '';
  let evaluationRuns: Record<string, RunDetail> = {};
  let exploreRun: RunDetail | undefined;
  let selectedRun: RunDetail | undefined;
  let terminalRun: RunSummary | undefined;
  let runEvents: RunEvent[] = [];
  let unavailableRunEvidence: UnavailableRunEvidence | undefined;
  const unavailableRunIds = new Set<string>();
  const unavailableEvaluationIds = new Set<string>();
  let activeTab: Tab = 'agent';
  let agentInspectionMode: AgentInspectionMode = 'session';
  let agentView: AgentView = 'review';
  let agentAnswerView: AgentAnswerView = 'rendered';
  let actionError = '';
  let agentSessionSyncError = '';
  let preparing = false;
  let starting = false;
  let fixtureOnly = false;
  let comparing = false;
  let agentTurnCancelling = false;

  $: activeExplore = exploreRun?.summary ??
    (unavailableRunEvidence && unavailableRunEvidence.summary.id === terminalRun?.id
      ? unavailableRunEvidence.summary
      : undefined);
  $: inspectedRun = selectedRun?.summary ?? unavailableRunEvidence?.summary;
  $: running = activeExplore?.status === 'starting' || activeExplore?.status === 'running';
  $: finished = activeExplore?.status === 'passed' || activeExplore?.status === 'failed' || activeExplore?.status === 'cancelled';
  $: historicalAgentSession = Boolean(activeAgentSession && agentSessionIsHistorical(activeAgentSession.summary));
  $: showingAgentSession = Boolean(activeAgentSession && agentInspectionMode === 'session');
  $: sessionAssembly = exploreRun?.assembly;
  $: agentLiveStatus = projectAgentSessionLiveStatus(activeAgentSession, agentTurnCancelling);
  $: agentSessionLifecycleActive =
    agentSessions.some((summary) => !agentSessionIsHistorical(summary)) ||
    Boolean(activeAgentSession && !agentSessionIsHistorical(activeAgentSession.summary));
  $: scenarioSwitchBlocked = preparing ||
    running ||
    agentSessionLifecycleActive;
  $: if (!activeAgentSession?.turns.some((turn) => turn.status === 'queued' || turn.status === 'running')) {
    agentTurnCancelling = false;
  }
  $: agentSessionHeading = activeAgentSession?.summary.status === 'interrupted'
    ? 'Session replay'
    : historicalAgentSession
      ? 'Session history'
      : activeAgentSession?.summary.active
        ? 'Active session'
        : 'Agent session';
  $: agentSessionIntro = activeAgentSession?.summary.status === 'interrupted'
    ? 'Reopened from durable evidence. Start a new agent session to continue.'
    : historicalAgentSession
      ? 'Retained as durable evidence. Start a new agent session to continue.'
      : activeAgentSession?.summary.active
        ? 'Ask the harness in Explore. Its response, actions, and effects stay together here.'
        : 'This session is getting ready. Its response, actions, and effects will stay together here.';
  $: evaluationRunning = selectedEvaluation?.summary.status === 'queued' || selectedEvaluation?.summary.status === 'running';
  $: behaviorRows = buildBehaviorRows(selectedEvaluation, evaluationRuns);
  $: comparisonClockMs = maxComparisonDuration(selectedEvaluation, evaluationRuns);
  $: selectableModelProfiles = modelProfiles.filter(
    (profile) =>
      profile.harnessIds.includes(harnessId) &&
      comparisonHarnessIds.every((id) => profile.harnessIds.includes(id))
  );
  $: comparisonLabel = `Compare ${comparisonHarnessIds
    .map((id) => harnesses.find((harness) => harness.id === id)?.displayName ?? id)
    .join(' with ')}`;
  $: activeModelAccess = modelAccess.find((access) => access.status !== 'ready') ?? modelAccess[0];
  $: runModelAccessReady = modelAccess
    .filter((access) => access.harnessIds.includes(harnessId))
    .every((access) => access.status === 'ready');
  $: comparisonModelAccessReady = modelAccess
    .filter((access) => access.harnessIds.some((id) => comparisonHarnessIds.includes(id)))
    .every((access) => access.status === 'ready');

  async function startTerminal(run?: RunSummary): Promise<void> {
    startupError = '';
    connectionState = 'starting';
    sessionEvents = [];
    session?.dispose();
    try {
      surface ??= await createGhosttySurface(terminalHost);
      session = await connectSession(
        surface,
        {
          onState(next) {
            connectionState = next;
          },
          onEvent(event) {
            sessionEvents = [...sessionEvents, event];
            if (event.type === 'error') {
              connectionState = 'error';
              startupError = event.message;
            }
          },
          onScreen(text) {
            screenText = text;
          }
        },
        run?.id
      );
      terminalRun = run;
      surface.focus();
    } catch (error) {
      connectionState = 'error';
      startupError = message(error);
    }
  }

  async function load(): Promise<void> {
    try {
      [models, scenarios, runs, harnesses, modelProfiles, evaluations] = await Promise.all([
        runClient.models(), runClient.scenarios(), runClient.runs(), runClient.harnesses(),
        runClient.modelProfiles(), runClient.evaluations()
      ]);
      fixtureOnly = scenarios.length === 0;
      scenarioId ||= scenarios[0]?.id ?? '';
      harnessId ||= harnesses[0]?.id ?? '';
      modelProfileId ||= modelProfiles.find((profile) => profile.harnessIds.includes(harnessId))?.id ?? '';
    } catch (error) {
      actionError = message(error);
    }
  }

  async function prepareScenario(): Promise<void> {
    if (!scenarioId || scenarioSwitchBlocked) return;
    const sourceExploreRun = exploreRun;
    const sourceSelectedRun = selectedRun;
    const sourceRunEvents = runEvents;
    const sourceUnavailableRunEvidence = unavailableRunEvidence;
    const sourceWorkspaceId = sourceExploreRun?.summary.id;
    const sourceScenarioId = sourceExploreRun?.summary.scenarioId;
    let detachedSource = false;
    preparing = true;
    actionError = '';
    try {
      const summary = await runClient.prepare(scenarioId, sourceWorkspaceId);
      if (sourceWorkspaceId && summary.id !== sourceWorkspaceId) {
        detachExploreWorkspace();
        detachedSource = true;
      }
      const detail = await runClient.detail(summary.id);
      unavailableRunEvidence = undefined;
      exploreRun = detail;
      selectedRun = detail;
      runEvents = detail.events;
      await loadWorkbench(summary.id);
      activeTab = 'agent';
      agentView = 'review';
      watchExploreRun(summary.id);
      await startTerminal(summary);
    } catch (error) {
      if (sourceScenarioId) scenarioId = sourceScenarioId;
      if (detachedSource && sourceExploreRun) {
        exploreRun = sourceExploreRun;
        selectedRun = sourceSelectedRun;
        runEvents = sourceRunEvents;
        unavailableRunEvidence = sourceUnavailableRunEvidence;
        try {
          await loadWorkbench(sourceExploreRun.summary.id);
          watchExploreRun(sourceExploreRun.summary.id);
          await startTerminal(sourceExploreRun.summary);
        } catch {
          // Preserve the original transition failure as the actionable error.
        }
      }
      actionError = message(error);
    } finally {
      preparing = false;
    }
  }

  async function loadWorkbench(id: string): Promise<void> {
    const workbench = await runClient.workbench(id);
    applyWorkbenchSelection(workbench.selection);
    modelAccess = workbench.modelAccess;
    mergeAgentSessionSnapshot(id, workbench.agentSessions);
    const session = workbench.activeAgentSession?.active
      ? workbench.activeAgentSession
      : workbench.replayAgentSession;
    if (session && !starting && exploreRun?.summary.status === 'exploring') {
      try {
        await openAgentSession(id, session.id, false);
      } catch (error) {
        agentSessionSyncError = agentSessionUnavailableMessage(error);
        requestAgentSessionReconciliation(id, session.id, agentSessionOpenVersion, {
          replaceCurrent: true
        });
      }
    } else {
      clearAgentSessionView();
    }
  }

  async function openAgentSession(
    workspaceId: string,
    sessionId: string,
    reveal = true,
    refresh = true
  ): Promise<void> {
    if (!refresh && activeAgentSession?.summary.id === sessionId) {
      if (reveal) {
        activeTab = 'agent';
        agentInspectionMode = 'session';
      }
      return;
    }
    const requestVersion = ++agentSessionOpenRequestVersion;
    const currentOpenVersion = agentSessionOpenVersion;
    const currentEvidenceGeneration = agentSessionEvidenceGeneration;
    const detail = await runClient.agentSession(workspaceId, sessionId);
    if (
      requestVersion !== agentSessionOpenRequestVersion ||
      currentOpenVersion !== agentSessionOpenVersion ||
      currentEvidenceGeneration !== agentSessionEvidenceGeneration ||
      starting ||
      exploreRun?.summary.status !== 'exploring'
    ) return;
    const current = activeAgentSession;
    const currentSequence = pendingAgentSessionEvents.reduce(
      (latest, event) => Math.max(latest, event.sequence),
      current?.events.at(-1)?.sequence ?? 0
    );
    const detailSequence = detail.events.at(-1)?.sequence ?? 0;
    if (
      current?.summary.id === detail.summary.id &&
      detailSequence < currentSequence
    ) {
      requestAgentSessionReconciliation(workspaceId, sessionId, currentOpenVersion);
      return;
    }
    const openVersion = ++agentSessionOpenVersion;
    clearAgentSessionReconciliation();
    if (activeAgentSession?.summary.id !== detail.summary.id) {
      agentAnswerView = 'rendered';
      agentTurnCancelling = false;
    }
    activeAgentSession = detail;
    rememberAgentSession(detail.summary);
    knownAgentSessionEventSequences = new Set(detail.events.map((event) => event.sequence));
    pendingAgentSessionEvents = [];
    if (agentSessionEventFlushTimer !== undefined) clearTimeout(agentSessionEventFlushTimer);
    agentSessionEventFlushTimer = undefined;
    if (reveal) {
      activeTab = 'agent';
      agentInspectionMode = 'session';
    }
    agentSessionEventStream?.abort();
    agentSessionEventStream = undefined;
    if (!agentSessionIsHistorical(detail.summary)) {
      watchAgentSessionEvents(workspaceId, sessionId, openVersion);
    }
  }

  function watchAgentSessionEvents(
    workspaceId: string,
    sessionId: string,
    openVersion: number
  ): void {
    agentSessionEventStream?.abort();
    let stream: AbortController | undefined;
    stream = runClient.agentSessionEvents(
      workspaceId,
      sessionId,
      (event) => {
        if (
          agentSessionEventStream !== stream ||
          openVersion !== agentSessionOpenVersion ||
          activeAgentSession?.summary.id !== sessionId
        ) return;
        queueAgentSessionEvent(workspaceId, sessionId, openVersion, event);
      },
      (reset) => reconcileAgentSessionEventStreamReset(
        workspaceId,
        sessionId,
        openVersion,
        reset,
        stream
      )
    );
    agentSessionEventStream = stream;
  }

  async function reconcileAgentSessionEventStreamReset(
    workspaceId: string,
    sessionId: string,
    openVersion: number,
    reset: RunEventStreamReset,
    stream: AbortController | undefined
  ): Promise<number> {
    if (
      !stream ||
      agentSessionEventStream !== stream ||
      openVersion !== agentSessionOpenVersion ||
      activeAgentSession?.summary.id !== sessionId
    ) {
      stream?.abort();
      return 0;
    }
    const evidenceGeneration = ++agentSessionEvidenceGeneration;
    if (agentSessionEventFlushTimer !== undefined) clearTimeout(agentSessionEventFlushTimer);
    agentSessionEventFlushTimer = undefined;
    pendingAgentSessionEvents = [];
    knownAgentSessionEventSequences = new Set();
    clearAgentSessionReconciliation();
    if (reset.responseStatus === 404) {
      if (
        openVersion === agentSessionOpenVersion &&
        activeAgentSession?.summary.id === sessionId
      ) {
        agentSessions = agentSessions.filter((session) => session.id !== sessionId);
        clearAgentSessionView();
      }
      return 0;
    }
    let latest: AgentSessionDetail;
    try {
      latest = await runClient.agentSession(workspaceId, sessionId);
    } catch (error) {
      if (
        agentSessionEventStream === stream &&
        openVersion === agentSessionOpenVersion &&
        activeAgentSession?.summary.id === sessionId
      ) {
        agentSessionSyncError = agentSessionUnavailableMessage(error);
      }
      throw error;
    }
    const latestSequence = latest.events.reduce(
      (sequence, event) => Math.max(sequence, event.sequence),
      0
    );
    if (
      evidenceGeneration !== agentSessionEvidenceGeneration ||
      openVersion !== agentSessionOpenVersion ||
      activeAgentSession?.summary.id !== sessionId ||
      agentSessionEventStream !== stream
    ) return latestSequence;
    activeAgentSession = latest;
    rememberAgentSession(latest.summary);
    knownAgentSessionEventSequences = new Set(latest.events.map((event) => event.sequence));
    agentSessionSyncError = '';
    if (agentSessionIsHistorical(latest.summary)) {
      stream.abort();
      if (agentSessionEventStream === stream) agentSessionEventStream = undefined;
    }
    return latestSequence;
  }

  function clearAgentSessionView(): void {
    agentSessionOpenVersion += 1;
    agentSessionEvidenceGeneration += 1;
    agentSessionEventStream?.abort();
    agentSessionEventStream = undefined;
    if (agentSessionEventFlushTimer !== undefined) clearTimeout(agentSessionEventFlushTimer);
    agentSessionEventFlushTimer = undefined;
    pendingAgentSessionEvents = [];
    knownAgentSessionEventSequences = new Set();
    clearAgentSessionReconciliation();
    activeAgentSession = undefined;
    agentAnswerView = 'rendered';
    agentTurnCancelling = false;
  }

  function detachExploreWorkspace(): void {
    exploreEventStream?.abort();
    exploreEventStream = undefined;
    inspectionEventStream?.abort();
    inspectionEventStream = undefined;
    clearAgentSessionView();
    agentSessions = [];
    agentSessionsWorkspaceId = '';
    session?.dispose();
    session = undefined;
    surface?.dispose();
    surface = undefined;
    terminalHost?.replaceChildren();
    terminalRun = undefined;
    screenText = '';
    sessionEvents = [];
    connectionState = 'starting';
    startupError = '';
  }

  function queueAgentSessionEvent(
    workspaceId: string,
    sessionId: string,
    openVersion: number,
    event: RunEvent
  ): void {
    if (
      openVersion !== agentSessionOpenVersion ||
      activeAgentSession?.summary.id !== sessionId ||
      knownAgentSessionEventSequences.has(event.sequence)
    ) return;
    knownAgentSessionEventSequences.add(event.sequence);
    pendingAgentSessionEvents.push(event);
    if (agentSessionEventFlushTimer !== undefined) return;
    agentSessionEventFlushTimer = setTimeout(() => {
      agentSessionEventFlushTimer = undefined;
      if (
        openVersion !== agentSessionOpenVersion ||
        activeAgentSession?.summary.id !== sessionId
      ) {
        pendingAgentSessionEvents = [];
        return;
      }
      const events = pendingAgentSessionEvents.sort((left, right) => left.sequence - right.sequence);
      pendingAgentSessionEvents = [];
      activeAgentSession = applyAgentSessionEvents(activeAgentSession, events);
      rememberAgentSession(activeAgentSession.summary);
      if (events.some(requiresAgentSessionReconciliation)) {
        requestAgentSessionReconciliation(workspaceId, sessionId, openVersion);
      }
    }, 50);
  }

  function applyAgentSessionEvents(
    detail: AgentSessionDetail,
    events: RunEvent[]
  ): AgentSessionDetail {
    const turns = detail.turns.map((turn) => ({
      ...turn,
      presentation: turn.presentation
        ? {
            ...turn.presentation,
            messages: turn.presentation.messages.map((entry) => ({
              ...entry,
              sourceEventSequences: [...entry.sourceEventSequences]
            })),
            sourceEventSequences: [...turn.presentation.sourceEventSequences]
          }
        : undefined
    }));
    const turnIndexes = new Map(turns.map((turn, index) => [turn.id, index]));
    let summary = { ...detail.summary };

    for (const event of events) {
      const payload = eventRecord(event.payload);
      const body = eventRecord(payload.event ?? payload);
      const turnId = typeof payload.turnId === 'string' ? payload.turnId : undefined;
      const turnIndex = turnId === undefined ? undefined : turnIndexes.get(turnId);
      const turn = turnIndex === undefined ? undefined : turns[turnIndex];

      if (event.type === 'agent.session.starting') summary.status = 'starting';
      if (event.type === 'agent.session.ready') {
        summary = { ...summary, active: true, status: 'ready', error: undefined };
      }
      if (event.type === 'agent.session.failed') {
        summary = {
          ...summary,
          active: false,
          status: 'failed',
          error: typeof payload.message === 'string' ? payload.message : summary.error
        };
      }
      if (event.type === 'agent.session.closed' || event.type === 'agent.session.interrupted') {
        summary = {
          ...summary,
          active: false,
          status: event.type === 'agent.session.closed' ? 'closed' : 'interrupted'
        };
      }
      if (event.type === 'agent.turn.started') {
        summary.status = 'running';
        if (turn) turn.status = 'running';
      }
      if (event.type === 'agent.turn.finished') {
        summary.status = 'ready';
        if (turn) {
          const outcome = typeof payload.outcome === 'string' ? payload.outcome : 'failed';
          turn.outcome = outcome;
          turn.status = outcome === 'completed'
            ? 'completed'
            : outcome === 'intervened'
              ? 'intervened'
              : outcome === 'aborted' || outcome === 'cancelled'
                ? 'cancelled'
                : 'failed';
          turn.finishedAtMs = event.atMs;
        }
      }

      if (
        turn?.presentation &&
        (event.type === 'observation.assistant.delta' ||
          event.type === 'observation.assistant.completed')
      ) {
        const messageId = typeof body.messageId === 'string' ? body.messageId : undefined;
        const text = typeof body.text === 'string' ? body.text : undefined;
        if (messageId && text !== undefined) {
          let responseMessage = turn.presentation.messages.find((entry) => entry.id === messageId);
          if (!responseMessage) {
            responseMessage = {
              id: messageId,
              text: '',
              complete: false,
              sourceEventSequences: []
            };
            turn.presentation.messages.push(responseMessage);
          }
          if (event.type === 'observation.assistant.completed') {
            responseMessage.text = text;
            responseMessage.complete = true;
          } else if (!responseMessage.complete) {
            responseMessage.text += text;
          }
          responseMessage.sourceEventSequences.push(event.sequence);
          turn.presentation.sourceEventSequences.push(event.sequence);
          turn.presentation.response = turn.presentation.messages.map((entry) => entry.text).join('\n\n');
          turn.presentation.completeness = {
            ...turn.presentation.completeness,
            assistantOutput: turn.presentation.messages.every((entry) => entry.complete)
              ? 'complete'
              : 'partial'
          };
        }
      }
      summary.updatedAtMs = Math.max(summary.updatedAtMs, event.atMs);
    }

    return {
      ...detail,
      summary,
      turns,
      events: [...detail.events, ...events]
    };
  }

  function eventRecord(value: unknown): Record<string, unknown> {
    return value && typeof value === 'object' && !Array.isArray(value)
      ? value as Record<string, unknown>
      : {};
  }

  function requiresAgentSessionReconciliation(event: RunEvent): boolean {
    return event.type === 'agent.turn.started' ||
      event.type === 'agent.turn.finished' ||
      event.type === 'agent.session.ready' ||
      event.type === 'agent.session.failed' ||
      event.type === 'agent.session.closed' ||
      event.type === 'agent.session.interrupted' ||
      event.type === 'mcp.tool.started' ||
      event.type === 'mcp.tool.completed' ||
      event.type === 'observation.native-action' ||
      event.type === 'observation.usage';
  }

  function requestAgentSessionReconciliation(
    workspaceId: string,
    sessionId: string,
    openVersion: number,
    options: { reveal?: boolean; replaceCurrent?: boolean } = {}
  ): void {
    agentSessionReconcileTarget = {
      workspaceId,
      sessionId,
      openVersion,
      evidenceGeneration: agentSessionEvidenceGeneration,
      reveal: options.reveal ?? false,
      replaceCurrent: options.replaceCurrent ?? false
    };
    if (agentSessionReconcileInFlight || agentSessionReconcileRetryTimer !== undefined) return;
    startAgentSessionReconciliation();
  }

  function clearAgentSessionReconciliation(): void {
    if (agentSessionReconcileRetryTimer !== undefined) {
      clearTimeout(agentSessionReconcileRetryTimer);
      agentSessionReconcileRetryTimer = undefined;
    }
    agentSessionReconcileTarget = undefined;
    agentSessionReconcileRetryDelayMs = AGENT_SESSION_RECONCILE_INITIAL_MS;
    agentSessionSyncError = '';
  }

  function agentSessionReconcileTargetIsCurrent(
    target: AgentSessionReconcileTarget
  ): boolean {
    return target.openVersion === agentSessionOpenVersion &&
      target.evidenceGeneration === agentSessionEvidenceGeneration &&
      (
        target.replaceCurrent ||
        activeAgentSession === undefined ||
        activeAgentSession.summary.id === target.sessionId
      );
  }

  function scheduleAgentSessionReconciliationRetry(
    failedTarget: AgentSessionReconcileTarget
  ): void {
    if (!agentSessionReconcileTargetIsCurrent(failedTarget)) return;
    agentSessionReconcileTarget ??= failedTarget;
    if (agentSessionReconcileRetryTimer !== undefined) return;
    const delayMs = agentSessionReconcileRetryDelayMs;
    agentSessionReconcileRetryDelayMs = Math.min(
      agentSessionReconcileRetryDelayMs * 2,
      AGENT_SESSION_RECONCILE_MAX_MS
    );
    agentSessionReconcileRetryTimer = setTimeout(() => {
      agentSessionReconcileRetryTimer = undefined;
      const target = agentSessionReconcileTarget;
      if (!target || !agentSessionReconcileTargetIsCurrent(target)) {
        agentSessionReconcileTarget = undefined;
        return;
      }
      startAgentSessionReconciliation();
    }, delayMs);
  }

  function startAgentSessionReconciliation(): void {
    if (
      agentSessionReconcileInFlight ||
      agentSessionReconcileRetryTimer !== undefined ||
      !agentSessionReconcileTarget
    ) return;
    const reconciliation = (async () => {
      while (agentSessionReconcileTarget) {
        const target: AgentSessionReconcileTarget = agentSessionReconcileTarget;
        agentSessionReconcileTarget = undefined;
        if (!agentSessionReconcileTargetIsCurrent(target)) continue;
        let latest: AgentSessionDetail;
        try {
          latest = await runClient.agentSession(target.workspaceId, target.sessionId);
        } catch (error) {
          if (agentSessionReconcileTargetIsCurrent(target)) {
            agentSessionReconcileTarget ??= target;
            agentSessionSyncError = agentSessionUnavailableMessage(error);
            scheduleAgentSessionReconciliationRetry(target);
          }
          return;
        }
        const current = activeAgentSession;
        if (!agentSessionReconcileTargetIsCurrent(target)) continue;
        if (
          current !== undefined &&
          current.summary.id !== target.sessionId &&
          !target.replaceCurrent
        ) continue;
        const currentSequence = current?.events.at(-1)?.sequence ?? 0;
        const latestSequence = latest.events.at(-1)?.sequence ?? 0;
        if (latestSequence < currentSequence) {
          agentSessionReconcileTarget ??= target;
          scheduleAgentSessionReconciliationRetry(target);
          return;
        }
        const replacingSession = current?.summary.id !== latest.summary.id;
        if (replacingSession) {
          agentSessionEventStream?.abort();
          agentSessionEventStream = undefined;
          pendingAgentSessionEvents = [];
          knownAgentSessionEventSequences = new Set();
          agentAnswerView = 'rendered';
          agentTurnCancelling = false;
        }
        activeAgentSession = latest;
        rememberAgentSession(latest.summary);
        agentSessionReconcileRetryDelayMs = AGENT_SESSION_RECONCILE_INITIAL_MS;
        agentSessionSyncError = '';
        if (target.reveal) {
          activeTab = 'agent';
          agentInspectionMode = 'session';
        }
        const reconciledSequences = new Set(latest.events.map((event) => event.sequence));
        pendingAgentSessionEvents = pendingAgentSessionEvents.filter(
          (event) => !reconciledSequences.has(event.sequence)
        );
        knownAgentSessionEventSequences = new Set([
          ...knownAgentSessionEventSequences,
          ...reconciledSequences
        ]);
        if (agentSessionIsHistorical(latest.summary)) {
          agentSessionEventStream?.abort();
          agentSessionEventStream = undefined;
        } else if (!agentSessionEventStream) {
          watchAgentSessionEvents(target.workspaceId, target.sessionId, target.openVersion);
        }
      }
    })().finally(() => {
      if (agentSessionReconcileInFlight === reconciliation) {
        agentSessionReconcileInFlight = undefined;
      }
      if (
        agentSessionReconcileTarget &&
        agentSessionReconcileRetryTimer === undefined
      ) {
        startAgentSessionReconciliation();
      }
    });
    agentSessionReconcileInFlight = reconciliation;
  }

  function applyWorkbenchSelection(selection: WorkbenchSelection): void {
    harnessId = selection.harnessId ?? '';
    modelProfileId = selection.modelProfileId ?? '';
    comparisonHarnessIds = selection.comparisonHarnessIds;
  }

  function mergeAgentSessionSnapshot(
    workspaceId: string,
    snapshot: AgentSessionSummary[]
  ): void {
    if (agentSessionsWorkspaceId !== workspaceId) {
      agentSessionsWorkspaceId = workspaceId;
      agentSessions = snapshot;
      return;
    }
    const merged = [...agentSessions];
    for (const summary of snapshot) {
      const existing = merged.findIndex((candidate) => candidate.id === summary.id);
      if (existing === -1) {
        merged.push(summary);
      } else if (summary.updatedAtMs > merged[existing].updatedAtMs) {
        merged[existing] = summary;
      }
    }
    agentSessions = merged;
  }

  function rememberAgentSession(summary: AgentSessionSummary): void {
    if (agentSessionsWorkspaceId && agentSessionsWorkspaceId !== summary.workspaceId) return;
    agentSessionsWorkspaceId ||= summary.workspaceId;
    const existing = agentSessions.findIndex((candidate) => candidate.id === summary.id);
    agentSessions = existing === -1
      ? [summary, ...agentSessions]
      : summary.updatedAtMs >= agentSessions[existing].updatedAtMs
        ? agentSessions.map((candidate, index) => index === existing ? summary : candidate)
        : agentSessions;
  }

  async function changeWorkbenchSelection(input: Partial<WorkbenchSelection>): Promise<void> {
    if (!exploreRun) return;
    actionError = '';
    try {
      const selection = await runClient.updateWorkbenchSelection(exploreRun.summary.id, input);
      applyWorkbenchSelection(selection);
      await loadWorkbench(exploreRun.summary.id);
    } catch (error) {
      actionError = message(error);
      await loadWorkbench(exploreRun.summary.id);
    }
  }

  async function initialize(): Promise<void> {
    await load();
    if (fixtureOnly) {
      await startTerminal();
    } else {
      await prepareScenario();
    }
  }

  async function beginRun(): Promise<void> {
    const hasSelection = harnesses.length ? Boolean(harnessId && modelProfileId) : Boolean(modelId.trim());
    if (
      !exploreRun ||
      exploreRun.summary.status !== 'exploring' ||
      !hasSelection ||
      !runModelAccessReady ||
      starting ||
      scenarioSwitchBlocked
    ) return;
    starting = true;
    actionError = '';
    activeTab = 'agent';
    agentInspectionMode = 'run';
    inspectionEventStream?.abort();
    try {
      const summary = harnesses.length
        ? await runClient.startPreparedHarness(exploreRun.summary.id, harnessId, modelProfileId)
        : await runClient.startPrepared(exploreRun.summary.id, modelId.trim());
      clearAgentSessionView();
      const detail = {
        summary,
        assembly: exploreRun.assembly,
        review: exploreRun.review,
        events: exploreRun.events,
        score: exploreRun.score,
        output: exploreRun.output
      };
      exploreRun = detail;
      selectedRun = detail;
      runEvents = detail.events;
      runs = [summary, ...runs.filter((run) => run.id !== summary.id)];
      watchExploreRun(summary.id);
    } catch (error) {
      actionError = message(error);
    } finally {
      starting = false;
    }
  }

  async function beginEvaluation(): Promise<void> {
    if (!exploreRun || exploreRun.summary.status !== 'exploring' || !modelProfileId || !comparisonModelAccessReady || comparing) return;
    if (
      comparisonHarnessIds.length !== 2 ||
      comparisonHarnessIds.some((id) => !harnesses.some((harness) => harness.id === id))
    ) {
      actionError = 'Choose two available comparison harnesses.';
      return;
    }
    comparing = true;
    actionError = '';
    try {
      const summary = await runClient.compareWorkbench(exploreRun.summary.id);
      evaluations = [summary, ...evaluations];
      const detail = await loadEvaluation(summary.id);
      if (!detail) return;
      activeTab = 'evaluation';
      watchEvaluation(summary.id);
    } catch (error) {
      actionError = message(error);
    } finally {
      comparing = false;
    }
  }

  function watchEvaluation(id: string): void {
    inspectionEventStream?.abort();
    let stream: AbortController | undefined;
    stream = runClient.evaluationEvents(id, (event) => {
      if (inspectionEventStream !== stream) return;
      if (event.type === 'evaluation.unavailable') {
        unavailableEvaluationIds.add(id);
        if (evaluationRefreshTimer) clearTimeout(evaluationRefreshTimer);
        evaluationRefreshTimer = undefined;
        stream?.abort();
        if (inspectionEventStream === stream) inspectionEventStream = undefined;
        evaluations = evaluations.filter((evaluation) => evaluation.id !== id);
        if (selectedEvaluation?.summary.id === id) {
          selectedEvaluation = undefined;
          evaluationRuns = {};
        }
        actionError = EVALUATION_EVIDENCE_UNAVAILABLE;
      } else if (event.type === 'evaluation.finished') {
        if (evaluationRefreshTimer) clearTimeout(evaluationRefreshTimer);
        evaluationRefreshTimer = undefined;
        void refreshEvaluation(id);
      } else if (
        event.type === 'evaluation.arm.started' ||
        event.type === 'evaluation.arm.progress' ||
        event.type === 'evaluation.arm.finished'
      ) {
        if (evaluationRefreshTimer) clearTimeout(evaluationRefreshTimer);
        evaluationRefreshTimer = setTimeout(() => {
          evaluationRefreshTimer = undefined;
          void refreshEvaluation(id);
        }, 75);
      }
    });
    inspectionEventStream = stream;
  }

  async function refreshEvaluation(id: string): Promise<void> {
    try {
      const detail = await loadEvaluation(id);
      if (!detail || unavailableEvaluationIds.has(id)) return;
      const nextEvaluations = await runClient.evaluations();
      if (unavailableEvaluationIds.has(id)) return;
      evaluations = nextEvaluations;
      if (detail.summary.status !== 'running' && detail.summary.status !== 'queued') {
        inspectionEventStream?.abort();
      }
    } catch (error) {
      if (!unavailableEvaluationIds.has(id)) actionError = message(error);
    }
  }

  async function openEvaluation(id: string): Promise<void> {
    inspectionEventStream?.abort();
    try {
      const detail = await loadEvaluation(id);
      if (!detail || unavailableEvaluationIds.has(id)) return;
      activeTab = 'evaluation';
      if (detail.summary.status === 'running' || detail.summary.status === 'queued') {
        watchEvaluation(id);
      }
    } catch (error) {
      if (!unavailableEvaluationIds.has(id)) actionError = message(error);
    }
  }

  async function openEvaluationArm(runId: string | undefined): Promise<void> {
    if (!runId) return;
    await openRun(runId);
  }

  async function loadEvaluation(id: string): Promise<EvaluationDetail | undefined> {
    if (unavailableEvaluationIds.has(id)) return undefined;
    const detail = await runClient.evaluation(id);
    if (unavailableEvaluationIds.has(id)) return undefined;
    const entries = (await Promise.all(
      detail.summary.arms
        .filter((arm): arm is typeof arm & { runId: string } => Boolean(arm.runId))
        .map(async (arm) => {
          try {
            return [arm.harnessId, await runClient.detail(arm.runId)] as const;
          } catch (error) {
            const comparison = detail.comparison?.arms.find(
              (candidate) => candidate.harnessId === arm.harnessId
            );
            if (comparison?.evidenceComplete === false) return undefined;
            throw error;
          }
        })
    )).filter((entry): entry is readonly [string, RunDetail] => entry !== undefined);
    if (unavailableEvaluationIds.has(id)) return undefined;
    selectedEvaluation = detail;
    evaluationRuns = Object.fromEntries(entries);
    return detail;
  }

  function quarantinedRunScore(payload: Record<string, unknown>): UnavailableRunEvidence['score'] | undefined {
    const score = eventRecord(payload.score);
    if (typeof score.evidenceQuarantined !== 'boolean') return undefined;
    return {
      passed: score.passed === true,
      evidenceQuarantined: score.evidenceQuarantined
    };
  }

  function clearTerminalEvidence(id: string, summary: RunSummary): void {
    if (terminalRun?.id !== id) return;
    session?.dispose();
    session = undefined;
    surface?.dispose();
    surface = undefined;
    terminalHost?.replaceChildren();
    screenText = '';
    sessionEvents = [];
    connectionState = 'closed';
    startupError = '';
    terminalRun = summary;
  }

  function applyTerminalRunEvent(id: string, event: RunEvent): boolean {
    const payload = eventRecord(event.payload);
    const status = payload.status;
    if (status !== 'passed' && status !== 'failed' && status !== 'cancelled') return false;
    const terminalStatus: RunSummary['status'] = status;
    const error = typeof payload.error === 'string' ? payload.error : undefined;
    const unavailableScore = quarantinedRunScore(payload);
    const safeError = unavailableScore ? RUN_EVIDENCE_UNAVAILABLE : error;
    const currentSummary = exploreRun?.summary.id === id
      ? exploreRun.summary
      : selectedRun?.summary.id === id
        ? selectedRun.summary
        : terminalRun?.id === id
          ? terminalRun
          : runs.find((run) => run.id === id);
    const safeSummary = currentSummary
      ? {
          ...currentSummary,
          status: terminalStatus,
          finishedAtMs: event.atMs,
          eventCount: Math.max(currentSummary.eventCount, event.sequence),
          error: safeError
        }
      : undefined;
    if (unavailableScore && safeSummary) {
      const selectedWasQuarantined = selectedRun?.summary.id === id;
      const exploreWasQuarantined = exploreRun?.summary.id === id;
      unavailableRunIds.add(id);
      unavailableRunEvidence = { summary: safeSummary, score: unavailableScore };
      if (selectedWasQuarantined) {
        selectedRun = undefined;
        runEvents = [];
      }
      if (exploreWasQuarantined) {
        exploreRun = undefined;
        agentSessions = [];
        agentSessionsWorkspaceId = '';
        clearAgentSessionView();
      }
      const affectedEvaluationIds = new Set(
        evaluations
          .filter((evaluation) =>
            evaluation.sourceWorkspaceId === id ||
            evaluation.arms.some((arm) => arm.runId === id)
          )
          .map((evaluation) => evaluation.id)
      );
      const selectedEvaluationAffected = Boolean(
        selectedEvaluation &&
        (
          selectedEvaluation.summary.sourceWorkspaceId === id ||
          selectedEvaluation.summary.arms.some((arm) => arm.runId === id)
        )
      );
      if (selectedEvaluationAffected && selectedEvaluation) {
        affectedEvaluationIds.add(selectedEvaluation.summary.id);
        selectedEvaluation = undefined;
      }
      for (const evaluationId of affectedEvaluationIds) {
        unavailableEvaluationIds.add(evaluationId);
      }
      evaluations = evaluations.filter((evaluation) => !affectedEvaluationIds.has(evaluation.id));
      evaluationRuns = selectedEvaluationAffected
        ? {}
        : Object.fromEntries(
            Object.entries(evaluationRuns).filter(([, detail]) => detail.summary.id !== id)
          );
      runs = runs.filter((run) => run.id !== id);
      clearTerminalEvidence(id, safeSummary);
      actionError = RUN_EVIDENCE_UNAVAILABLE;
      return true;
    }
    const updateDetail = (detail: RunDetail): RunDetail => ({
      ...detail,
      summary: {
        ...detail.summary,
        status: terminalStatus,
        finishedAtMs: event.atMs,
        eventCount: Math.max(detail.summary.eventCount, event.sequence),
        error
      },
      score: payload.score ?? detail.score
    });
    if (exploreRun?.summary.id === id) exploreRun = updateDetail(exploreRun);
    if (selectedRun?.summary.id === id) selectedRun = updateDetail(selectedRun);
    runs = runs.map((run) => run.id === id
      ? {
          ...run,
          status: terminalStatus,
          finishedAtMs: event.atMs,
          eventCount: Math.max(run.eventCount, event.sequence),
          error
        }
      : run);
    actionError = error ?? '';
    return true;
  }

  function watchExploreRun(id: string): void {
    exploreEventStream?.abort();
    let liveAfterSequence = runEvents.reduce(
      (latest, event) => Math.max(latest, event.sequence),
      0
    );
    let stream: AbortController | undefined;
    stream = runClient.events(id, (event) => {
      if (exploreEventStream !== stream) return;
      if (selectedRun?.summary.id === id && !runEvents.some((known) => known.sequence === event.sequence)) {
        runEvents = [...runEvents, event];
        scheduleReviewRefresh(id);
      }
      if (
        event.type === 'run.status' &&
        event.payload &&
        typeof event.payload === 'object' &&
        typeof (event.payload as { status?: unknown }).status === 'string' &&
        exploreRun?.summary.id === id
      ) {
        const status = (event.payload as { status: RunSummary['status'] }).status;
        exploreRun = {
          ...exploreRun,
          summary: { ...exploreRun.summary, status }
        };
        if (selectedRun?.summary.id === id) {
          selectedRun = { ...selectedRun, summary: { ...selectedRun.summary, status } };
        }
        runs = runs.map((run) => (run.id === id ? { ...run, status } : run));
      }
      if (event.type === 'run.finished') {
        if (reviewRefreshTimer !== undefined) clearTimeout(reviewRefreshTimer);
        reviewRefreshTimer = undefined;
        void refreshRun(id, applyTerminalRunEvent(id, event));
      }
      if (
        event.sequence > liveAfterSequence &&
        event.type === 'workbench.selection.changed' &&
        event.payload &&
        typeof event.payload === 'object'
      ) {
        const selection = (event.payload as { selection?: WorkbenchSelection }).selection;
        if (selection) applyWorkbenchSelection(selection);
      }
      if (
        event.sequence > liveAfterSequence &&
        (
          event.type === 'workbench.agent.session.started' ||
          event.type === 'workbench.agent.session.updated'
        ) &&
        event.payload &&
        typeof event.payload === 'object'
      ) {
        const summary = (event.payload as { session?: AgentSessionSummary }).session;
        if (summary?.workspaceId === id) rememberAgentSession(summary);
      }
      if (
        event.sequence > liveAfterSequence &&
        event.type === 'workbench.evaluation.started' &&
        event.payload &&
        typeof event.payload === 'object' &&
        (event.payload as { origin?: unknown }).origin === 'nushell' &&
        typeof (event.payload as { evaluationId?: unknown }).evaluationId === 'string'
      ) {
        const evaluationId = (event.payload as { evaluationId: string }).evaluationId;
        void openEvaluation(evaluationId);
      }
      if (
        event.sequence > liveAfterSequence &&
        !starting &&
        exploreRun?.summary.status === 'exploring' &&
        (
          event.type === 'workbench.agent.session.started' ||
          event.type === 'workbench.agent.session.activated' ||
          event.type === 'workbench.agent.turn.started'
        ) &&
        event.payload &&
        typeof event.payload === 'object' &&
        typeof (event.payload as { sessionId?: unknown }).sessionId === 'string'
      ) {
        const sessionId = (event.payload as { sessionId: string }).sessionId;
        const reveal = (event.payload as { origin?: unknown }).origin === 'nushell';
        const replaceCurrent = activeAgentSession?.summary.id !== sessionId;
        void openAgentSession(
          id,
          sessionId,
          reveal,
          event.type === 'workbench.agent.session.activated' ||
            event.type === 'workbench.agent.turn.started' ||
            replaceCurrent
        )
          .catch((error) => {
            agentSessionSyncError = agentSessionUnavailableMessage(error);
            requestAgentSessionReconciliation(id, sessionId, agentSessionOpenVersion, {
              reveal,
              replaceCurrent
            });
          });
      }
    }, async (reset) => {
      const reconciledSequence = await reconcileRunEventStreamReset(id, reset, 'explore', stream);
      if (exploreEventStream === stream) liveAfterSequence = reconciledSequence;
      return reconciledSequence;
    });
    exploreEventStream = stream;
  }

  function watchInspectedRun(id: string): void {
    inspectionEventStream?.abort();
    let stream: AbortController | undefined;
    stream = runClient.events(id, (event) => {
      if (inspectionEventStream !== stream) return;
      if (selectedRun?.summary.id === id && !runEvents.some((known) => known.sequence === event.sequence)) {
        runEvents = [...runEvents, event];
        scheduleReviewRefresh(id);
      }
      if (event.type === 'run.finished') {
        if (reviewRefreshTimer !== undefined) clearTimeout(reviewRefreshTimer);
        reviewRefreshTimer = undefined;
        void refreshRun(id, applyTerminalRunEvent(id, event));
      }
    }, (reset) => reconcileRunEventStreamReset(id, reset, 'inspection', stream));
    inspectionEventStream = stream;
  }

  async function reconcileRunEventStreamReset(
    id: string,
    reset: RunEventStreamReset,
    owner: 'explore' | 'inspection',
    stream: AbortController | undefined
  ): Promise<number> {
    const streamIsCurrent = () => Boolean(
      stream &&
      (
        owner === 'explore'
          ? exploreEventStream === stream
          : inspectionEventStream === stream
      )
    );
    if (!streamIsCurrent()) {
      stream?.abort();
      return 0;
    }
    runReviewGeneration += 1;
    if (reviewRefreshTimer !== undefined) clearTimeout(reviewRefreshTimer);
    reviewRefreshTimer = undefined;
    if (selectedRun?.summary.id === id) runEvents = [];
    if (reset.responseStatus === 404) {
      applyTerminalRunEvent(id, {
        sequence: 0,
        atMs: Date.now(),
        type: 'run.finished',
        payload: {
          status: 'failed',
          error: RUN_EVIDENCE_UNAVAILABLE,
          score: { passed: false, evidenceQuarantined: true }
        }
      });
      stream?.abort();
      if (owner === 'explore' && exploreEventStream === stream) {
        exploreEventStream = undefined;
      } else if (owner === 'inspection' && inspectionEventStream === stream) {
        inspectionEventStream = undefined;
      }
      return 0;
    }
    const detail = await runClient.detail(id);
    const latestSequence = detail.events.reduce(
      (latest, event) => Math.max(latest, event.sequence),
      0
    );
    if (!streamIsCurrent()) return latestSequence;
    if (unavailableRunIds.has(id)) return latestSequence;
    if (exploreRun?.summary.id === id) exploreRun = detail;
    if (selectedRun?.summary.id === id) {
      selectedRun = detail;
      runEvents = detail.events;
    }
    runs = runs.map((run) => (run.id === id ? detail.summary : run));
    return latestSequence;
  }

  function scheduleReviewRefresh(id: string): void {
    if (reviewRefreshTimer !== undefined) return;
    const generation = runReviewGeneration;
    reviewRefreshTimer = setTimeout(() => {
      reviewRefreshTimer = undefined;
      void refreshReview(id, generation);
    }, 100);
  }

  async function refreshReview(id: string, generation = runReviewGeneration): Promise<void> {
    try {
      const detail = await runClient.detail(id);
      if (generation !== runReviewGeneration) return;
      if (unavailableRunIds.has(id)) return;
      if (selectedRun?.summary.id !== id) return;
      const currentSequence = runEvents.at(-1)?.sequence ?? 0;
      const detailSequence = detail.events.at(-1)?.sequence ?? 0;
      if (detailSequence < currentSequence) {
        scheduleReviewRefresh(id);
        return;
      }
      selectedRun = {
        ...selectedRun,
        summary: detail.summary,
        assembly: detail.assembly,
        review: detail.review,
        score: detail.score,
        output: detail.output,
        outputError: detail.outputError
      };
      runs = runs.map((run) => (run.id === id ? detail.summary : run));
    } catch (error) {
      actionError = message(error);
    }
  }

  async function refreshRun(
    id: string,
    terminalEventApplied = false,
    requireSuccess = false
  ): Promise<RunDetail | undefined> {
    let detail: RunDetail;
    try {
      detail = await runClient.detail(id);
    } catch (error) {
      if (!terminalEventApplied) actionError = message(error);
      if (requireSuccess) throw error;
      return undefined;
    }
    if (unavailableRunIds.has(id)) return undefined;
    if (exploreRun?.summary.id === id) exploreRun = detail;
    if (selectedRun?.summary.id === id) {
      selectedRun = detail;
      runEvents = detail.events;
    }
    try {
      runs = await runClient.runs();
    } catch (error) {
      if (!terminalEventApplied) actionError = message(error);
    }
    return detail;
  }

  async function openRun(id: string): Promise<void> {
    actionError = '';
    inspectionEventStream?.abort();
    try {
      const detail = await runClient.detail(id);
      if (unavailableRunIds.has(id)) return;
      unavailableRunEvidence = undefined;
      selectedRun = detail;
      runEvents = detail.events;
      activeTab = 'agent';
      agentInspectionMode = 'run';
      agentView = 'review';
      if (
        id !== exploreRun?.summary.id &&
        (detail.summary.status === 'starting' || detail.summary.status === 'running')
      ) {
        watchInspectedRun(id);
      }
    } catch (error) {
      actionError = message(error);
    }
  }

  async function cancelRun(): Promise<void> {
    if (!activeExplore) return;
    try {
      await runClient.cancel(activeExplore.id);
    } catch (error) {
      actionError = message(error);
    }
  }

  async function cancelActiveAgentTurn(): Promise<void> {
    const workspaceId = exploreRun?.summary.id;
    const sessionId = activeAgentSession?.summary.id;
    if (
      !workspaceId ||
      !sessionId ||
      !agentLiveStatus?.cancellable ||
      agentTurnCancelling
    ) return;
    agentTurnCancelling = true;
    actionError = '';
    try {
      await runClient.cancelAgentTurn(workspaceId, sessionId);
    } catch (error) {
      agentTurnCancelling = false;
      actionError = message(error);
    }
  }

  async function cancelEvaluation(): Promise<void> {
    if (!selectedEvaluation || !evaluationRunning) return;
    try {
      await runClient.cancelEvaluation(selectedEvaluation.summary.id);
      await refreshEvaluation(selectedEvaluation.summary.id);
    } catch (error) {
      actionError = message(error);
    }
  }

  function message(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
  }

  function agentSessionUnavailableMessage(error: unknown): string {
    return `Agent session updates are temporarily unavailable. Retrying… (${message(error)})`;
  }

  function pretty(value: unknown): string {
    return value === undefined || value === null ? 'Not available yet.' : JSON.stringify(value, null, 2);
  }

  function turnMessages(turn: AgentTurnSummary): AgentTurnMessagePresentation[] {
    if (turn.presentation?.messages?.length) return turn.presentation.messages ?? [];
    if (!turn.presentation?.response) return [];
    return [{
      id: `legacy-${turn.id}`,
      text: turn.presentation.response,
      complete: turn.status !== 'queued' && turn.status !== 'running',
      sourceEventSequences: turn.presentation.sourceEventSequences ?? []
    }];
  }

  function turnDuration(turn: AgentTurnSummary): number | null {
    return turn.finishedAtMs === undefined ? null : Math.max(0, turn.finishedAtMs - turn.startedAtMs);
  }

  function turnEvents(turn: AgentTurnSummary): RunEvent[] {
    if (!activeAgentSession) return [];
    const sequences = new Set(turn.presentation?.sourceEventSequences ?? []);
    return activeAgentSession.events.filter((event) => {
      if (sequences.has(event.sequence)) return true;
      if (!event.payload || typeof event.payload !== 'object') return false;
      return (event.payload as { turnId?: unknown }).turnId === turn.id;
    });
  }

  function activityLabel(activity: AgentTurnActivityPresentation): string {
    return activity.kind.replaceAll('.', ' ').replaceAll('-', ' ');
  }

  function turnCompleteness(turn: AgentTurnSummary): string {
    const completeness = turn.presentation?.completeness;
    if (!completeness) return 'legacy evidence';
    const values = Object.values(completeness);
    if (values.every((value) => value === 'complete')) return 'complete projection';
    if (values.every((value) => value === 'unavailable')) return 'native evidence only';
    return 'partial projection';
  }

  function shortId(id: string): string {
    return id.split('-').at(-1) ?? id;
  }

  function eventLabel(type: string): string {
    return type.replaceAll('.', ' · ').replaceAll('-', ' ');
  }

  function duration(milliseconds: number | null): string {
    if (milliseconds === null) return '—';
    if (milliseconds < 1_000) return `${milliseconds}ms`;
    return `${(milliseconds / 1_000).toFixed(milliseconds < 10_000 ? 1 : 0)}s`;
  }

  function comparisonArm(harnessId: string): EvaluationComparisonArm | undefined {
    return selectedEvaluation?.comparison?.arms.find((arm) => arm.harnessId === harnessId);
  }

  function usage(value: EvaluationComparisonArm['usage'] | EvaluationComparisonArm['cache']): string {
    if (typeof value === 'string') return value;
    if ('inputTokens' in value) return `${value.inputTokens.toLocaleString()} in · ${value.outputTokens.toLocaleString()} out`;
    return `${value.readTokens.toLocaleString()} read · ${value.writeTokens.toLocaleString()} write`;
  }

  function normalizedPath(path: string | null): string {
    return path?.replace(/^\/workspace\//, '') ?? '';
  }

  function nativeAction(step: RunReviewStep): string {
    const title = step.title.toLowerCase();
    if (title.includes('wrote') || title.includes('write') || title.includes('created')) return 'write';
    if (title.includes('read') || title.includes('verified') || title.includes('verify')) return 'read';
    return title.replaceAll(/[^a-z0-9]+/g, '-');
  }

  function behaviorKey(step: RunReviewStep): string {
    switch (step.kind) {
      case 'harness': return 'harness';
      case 'startup': return `startup:${step.source ?? step.title}`;
      case 'capability': return `capability:${step.source ?? step.title}`;
      case 'native-action': return `native:${nativeAction(step)}:${normalizedPath(step.path)}`;
      case 'workspace-effect': return `workspace:${normalizedPath(step.path)}`;
      case 'outcome': return 'outcome';
      default: return `${step.kind}:${step.title}`;
    }
  }

  function behaviorLabel(step: RunReviewStep): string {
    switch (step.kind) {
      case 'harness': return 'Start';
      case 'startup': return step.title;
      case 'workspace-effect': return 'Workspace';
      case 'outcome': return 'Result';
      default: return step.title;
    }
  }

  function behaviorSegments(run: RunDetail): BehaviorSegment[] {
    const segments: BehaviorSegment[] = [];
    const occurrences = new Map<string, number>();
    let narration: RunReviewStep[] = [];

    for (const step of run.review.steps) {
      if (step.kind === 'model-turn') {
        narration.push(step);
        continue;
      }
      const baseKey = behaviorKey(step);
      const occurrence = occurrences.get(baseKey) ?? 0;
      occurrences.set(baseKey, occurrence + 1);
      const steps = [...narration, step];
      const observedTiming = segmentTiming(run, steps);
      const timing = step.kind === 'harness' && observedTiming.endMs !== null
        ? { startMs: 0, endMs: observedTiming.endMs }
        : observedTiming;
      segments.push({
        key: `${baseKey}:${occurrence}`,
        label: behaviorLabel(step),
        kind: step.kind,
        steps,
        ...timing
      });
      narration = [];
    }

    if (narration.length) {
      const timing = segmentTiming(run, narration);
      segments.push({
        key: 'completion:0',
        label: 'Completion',
        kind: 'model-turn',
        steps: narration,
        ...timing
      });
    }
    return segments;
  }

  function buildBehaviorRows(
    evaluation: EvaluationDetail | undefined,
    details: Record<string, RunDetail>
  ): BehaviorRow[] {
    if (!evaluation) return [];
    const order: string[] = [];
    const rows = new Map<string, BehaviorRow>();
    for (const arm of evaluation.summary.arms) {
      const run = details[arm.harnessId];
      if (!run) continue;
      for (const segment of behaviorSegments(run)) {
        let row = rows.get(segment.key);
        if (!row) {
          row = { key: segment.key, label: segment.label, kind: segment.kind, segments: {} };
          rows.set(segment.key, row);
          order.push(segment.key);
        }
        row.segments[arm.harnessId] = segment;
      }
    }
    return order
      .map((key) => rows.get(key) as BehaviorRow)
      .sort((left, right) => startupRank(left) - startupRank(right));
  }

  function startupRank(row: BehaviorRow): number {
    if (row.kind === 'harness') return 2;
    if (row.kind !== 'startup') return 100;
    const phase = row.key.split(':')[1];
    return ({
      'driver-process': 0,
      'adapter-load': 1,
      'protocol-ready': 2,
      'runtime-build': 3,
      'runtime-process': 4,
      session: 5,
      capabilities: 6,
      workspace: 7
    } as Record<string, number>)[phase] ?? 8;
  }

  function segmentTiming(
    run: RunDetail,
    steps: RunReviewStep[]
  ): Pick<BehaviorSegment, 'startMs' | 'endMs'> {
    const sequences = new Set(steps.flatMap((step) => step.eventSequences));
    const elapsed = run.events
      .filter((event) => sequences.has(event.sequence))
      .map((event) => Math.max(0, event.atMs - run.summary.startedAtMs));
    return elapsed.length
      ? { startMs: Math.min(...elapsed), endMs: Math.max(...elapsed) }
      : { startMs: null, endMs: null };
  }

  function maxComparisonDuration(
    evaluation: EvaluationDetail | undefined,
    details: Record<string, RunDetail>
  ): number {
    if (!evaluation) return 1_000;
    const durations = evaluation.summary.arms.map((arm) => {
      const reported = evaluation.comparison?.arms.find((candidate) => candidate.harnessId === arm.harnessId)?.metrics?.durationMs;
      const run = details[arm.harnessId]?.summary;
      return reported ?? (run?.finishedAtMs ? run.finishedAtMs - run.startedAtMs : 0);
    });
    return Math.max(1_000, ...durations);
  }

  function clockStyle(segment: BehaviorSegment | undefined): string {
    if (!segment || segment.startMs === null || segment.endMs === null) return '';
    const start = Math.min(100, (segment.startMs / comparisonClockMs) * 100);
    const end = Math.min(100, (segment.endMs / comparisonClockMs) * 100);
    return `--clock-start: ${start}%; --clock-span: ${Math.max(1.5, end - start)}%; --clock-end: ${end}%`;
  }

  function clockRange(segment: BehaviorSegment | undefined): string {
    if (!segment || segment.startMs === null || segment.endMs === null) return 'Time not reported';
    const start = segment.startMs === 0 ? '0s' : duration(segment.startMs);
    const end = duration(segment.endMs);
    return start === end ? `+${end}` : `+${start}–${end}`;
  }

  function clockEndLabel(segment: BehaviorSegment | undefined): string {
    return segment?.endMs === null || segment?.endMs === undefined ? 'time unavailable' : `+${duration(segment.endMs)}`;
  }

  function clockEndEdge(segment: BehaviorSegment | undefined): 'start' | 'middle' | 'end' {
    if (!segment || segment.endMs === null) return 'start';
    const end = (segment.endMs / comparisonClockMs) * 100;
    return end < 15 ? 'start' : end > 85 ? 'end' : 'middle';
  }

  onMount(() => {
    void initialize();
    return () => {
      if (reviewRefreshTimer !== undefined) clearTimeout(reviewRefreshTimer);
      if (evaluationRefreshTimer) clearTimeout(evaluationRefreshTimer);
      if (agentSessionEventFlushTimer !== undefined) clearTimeout(agentSessionEventFlushTimer);
      if (agentSessionReconcileRetryTimer !== undefined) {
        clearTimeout(agentSessionReconcileRetryTimer);
      }
      exploreEventStream?.abort();
      inspectionEventStream?.abort();
      agentSessionEventStream?.abort();
      session?.dispose();
      surface?.dispose();
    };
  });
</script>

<svelte:head>
  <title>Agent Lab</title>
  <meta name="description" content="An open workbench for building better agent harnesses" />
</svelte:head>

<main>
  <header>
    <div class="identity">
      <span class="mark">A</span>
      <div>
        <h1>Agent Lab</h1>
        <p>An open workbench for building better agent harnesses.</p>
      </div>
    </div>

    {#if !fixtureOnly}
    <div class="run-controls">
      <label>
        <span>Scenario</span>
        <select bind:value={scenarioId} aria-label="Scenario" disabled={scenarioSwitchBlocked} on:change={() => void prepareScenario()}>
          {#each scenarios as scenario}
            <option value={scenario.id}>{scenario.title}</option>
          {/each}
        </select>
      </label>
      {#if harnesses.length}
        <label>
          <span>Default harness</span>
          <select bind:value={harnessId} aria-label="Default harness" disabled={preparing || running} on:change={() => void changeWorkbenchSelection({ harnessId })}>
            {#each harnesses as harness}
              <option value={harness.id}>{harness.displayName}</option>
            {/each}
          </select>
        </label>
        <label class="model-field">
          <span>Default model</span>
          <select bind:value={modelProfileId} aria-label="Default model" disabled={preparing || running} on:change={() => void changeWorkbenchSelection({ modelProfileId })}>
            <option value="" disabled>Choose a model</option>
            {#each selectableModelProfiles as profile}
              <option value={profile.id}>{profile.displayName}</option>
            {/each}
          </select>
        </label>
      {:else}
        <label class="model-field">
          <span>Model</span>
          <select bind:value={modelId} aria-label="Model" disabled={preparing || running}>
            <option value="" disabled>Choose a model</option>
            {#each models as model}<option value={model}>{model}</option>{/each}
          </select>
        </label>
      {/if}
      {#if activeModelAccess}
        <div class="model-access-pill" data-status={activeModelAccess.status} title={activeModelAccess.message ?? activeModelAccess.setupHint}>
          <span>Model access</span>
          <strong>{activeModelAccess.status === 'ready' ? 'Ready' : 'Connect'}</strong>
        </div>
      {/if}
      {#if finished}
        <button class="primary" disabled={preparing} on:click={() => void prepareScenario()}>
          {preparing ? 'Preparing…' : 'New workspace'}
        </button>
      {:else}
        <button class="primary" disabled={activeExplore?.status !== 'exploring' || !(harnesses.length ? harnessId && modelProfileId : modelId.trim()) || !runModelAccessReady || starting || scenarioSwitchBlocked} on:click={() => void beginRun()}>
          {starting ? 'Starting…' : 'Run harness'}
        </button>
      {/if}
      {#if comparisonHarnessIds.length === 2}
        <button class="quiet compare" disabled={activeExplore?.status !== 'exploring' || !modelProfileId || !comparisonModelAccessReady || preparing || comparing || running || evaluationRunning} on:click={() => void beginEvaluation()}>
          {comparing ? 'Starting…' : comparisonLabel}
        </button>
      {/if}
      {#if running}
        <button class="quiet danger" on:click={() => void cancelRun()}>Cancel</button>
      {:else if evaluationRunning}
        <button class="quiet danger" on:click={() => void cancelEvaluation()}>Cancel evaluation</button>
      {/if}
    </div>
    {/if}

    <div class="connection" data-state={connectionState} aria-live="polite">
      <span class="status-dot"></span>
      <span>{connectionState}</span>
    </div>
  </header>

  {#if actionError || agentSessionSyncError || startupError}
    <div class="banner" role="alert">{actionError || agentSessionSyncError || startupError}</div>
  {/if}

  <section class="bench" aria-label="Agent Lab workbench">
    <article class="terminal-panel">
      <div class="panel-heading">
        <div>
          <span class="label">Explore</span>
          <span class="value">{terminalRun ? `${terminalRun.scenarioTitle} workspace` : fixtureOnly ? 'Fixture shell' : 'Preparing workspace…'}</span>
        </div>
        <span class="transport">PTY · Ghostty</span>
      </div>
      <div class="terminal-frame">
        <div class="terminal-host" bind:this={terminalHost} data-testid="terminal"></div>
        <pre class="screen-reader-output" data-testid="terminal-text" role="region" aria-label="Terminal output">{screenText}</pre>
      </div>
      <footer class="terminal-footer">
        <span>{sessionEvents.find((event) => event.type === 'started')?.provider ?? 'waiting'}</span>
        <span>{terminalRun ? `run ${shortId(terminalRun.id)}` : fixtureOnly ? 'fixture' : 'preparing'}</span>
        <span>loopback only</span>
      </footer>
    </article>

    <aside class="run-panel">
      <nav class="tabs" aria-label="Run views">
        {#if activeAgentSession}
          <button
            class:active={activeTab === 'agent' && agentInspectionMode === 'session'}
            on:click={() => {
              activeTab = 'agent';
              agentInspectionMode = 'session';
            }}
          >Session</button>
        {/if}
        <button
          class:active={activeTab === 'agent' && (agentInspectionMode === 'run' || !activeAgentSession)}
          on:click={() => {
            activeTab = 'agent';
            agentInspectionMode = 'run';
          }}
        >Agent Run</button>
        <button class:active={activeTab === 'workspace'} on:click={() => (activeTab = 'workspace')}>Workspace</button>
        <button class:active={activeTab === 'editor'} on:click={() => (activeTab = 'editor')}>Editor</button>
        <button class:active={activeTab === 'evidence'} on:click={() => (activeTab = 'evidence')}>Evidence</button>
        <button class:active={activeTab === 'evaluation'} on:click={() => (activeTab = 'evaluation')}>Evaluation</button>
      </nav>

      <div class="run-heading">
        {#if activeTab === 'agent' && showingAgentSession && activeAgentSession}
          <div>
            <span class="label">{agentSessionHeading}</span>
            <strong>{activeAgentSession.summary.harnessId} · {activeAgentSession.summary.modelProfileId}</strong>
          </div>
          <span class="run-status" data-status={activeAgentSession.summary.status}>{activeAgentSession.summary.status}</span>
        {:else if unavailableRunEvidence && !selectedRun}
          <div>
            <span class="label">{unavailableRunEvidence.summary.scenarioTitle}</span>
            <strong>Run evidence unavailable</strong>
          </div>
          <span class="run-status" data-status={unavailableRunEvidence.summary.status}>
            {unavailableRunEvidence.summary.status}
          </span>
        {:else if inspectedRun}
          <div>
            <span class="label">{inspectedRun.scenarioTitle}</span>
            <strong>{inspectedRun.status === 'exploring' ? 'Ready for exploration' : inspectedRun.modelId}</strong>
          </div>
          <span class="run-status" data-status={inspectedRun.status}>{inspectedRun.status}</span>
        {:else}
          <div>
            <span class="label">Agent Run</span>
            <strong>Start a scenario to inspect it here.</strong>
          </div>
        {/if}
      </div>

      <div class="tab-content">
        {#if activeTab === 'agent'}
          {#if showingAgentSession && activeAgentSession}
            <section class="session-view" data-testid="interactive-agent-session">
              <div class="session-intro">
                <div>
                  <span class="label">Conversation</span>
                  <p>{agentSessionIntro}</p>
                </div>
                <div class="answer-view-toggle" role="group" aria-label="Agent answer presentation">
                  <button
                    class:active={agentAnswerView === 'rendered'}
                    aria-pressed={agentAnswerView === 'rendered'}
                    on:click={() => (agentAnswerView = 'rendered')}
                  >Rendered</button>
                  <button
                    class:active={agentAnswerView === 'source'}
                    aria-pressed={agentAnswerView === 'source'}
                    on:click={() => (agentAnswerView = 'source')}
                  >Source</button>
                </div>
              </div>
              {#if agentLiveStatus}
                <AgentSessionLiveStatus
                  status={agentLiveStatus}
                  cancelling={agentTurnCancelling}
                  onCancel={() => void cancelActiveAgentTurn()}
                />
              {/if}
              <div class="session-turns">
                {#each activeAgentSession.turns as turn, index}
                  <article class="session-turn" data-testid="session-turn" data-status={turn.status}>
                    <header>
                      <h2>Turn {index + 1}</h2>
                      <div>
                        {#if turnDuration(turn) !== null}<time>{duration(turnDuration(turn))}</time>{/if}
                        <em data-status={turn.status}>{turn.status}</em>
                      </div>
                    </header>
                    <div class="turn-prompt">
                      <span>You</span>
                      <p>{turn.prompt}</p>
                    </div>
                    {#if turn.input !== undefined && turn.input !== null}
                      <details class="provided-context">
                        <summary>Provided context</summary>
                        <pre>{pretty(turn.input)}</pre>
                      </details>
                    {/if}
                    <div class="turn-response" data-testid="agent-response" data-state={turn.status}>
                      <span>Agent</span>
                      {#if turnMessages(turn).length}
                        <div class="assistant-messages">
                          {#each turnMessages(turn) as responseMessage (responseMessage.id)}
                            <article class="assistant-message" data-message-id={responseMessage.id} data-complete={responseMessage.complete}>
                              {#if agentAnswerView === 'rendered'}
                                <AssistantMarkdown source={responseMessage.text} streaming={!responseMessage.complete} />
                              {:else}
                                <!-- svelte-ignore a11y_no_noninteractive_tabindex (keyboard access for overflow content) -->
                                <pre class="response-source" tabindex="0" aria-label="Markdown source for agent response">{responseMessage.text}</pre>
                              {/if}
                            </article>
                          {/each}
                        </div>
                      {:else if turn.status === 'queued' || turn.status === 'running'}
                        <p class="response-pending">Waiting for the first response…</p>
                      {:else}
                        <p class="response-unavailable">This harness did not report an assistant response.</p>
                      {/if}
                    </div>
                    {#if turn.presentation?.activity.length}
                      <section class="turn-activity" aria-label="Turn activity">
                        <span class="label">What the harness did</span>
                        <ol>
                          {#each turn.presentation.activity as activity}
                            {@const detail = agentTurnActivityDetail(activity)}
                            <li data-kind={activity.kind} data-status={activity.status}>
                              <span class="activity-marker"></span>
                              <div>
                                <div class="activity-title">
                                  <strong>{activity.title}</strong>
                                  <em>{activityLabel(activity)}</em>
                                </div>
                                {#if detail}<p>{detail}</p>{/if}
                                {#if activity.source || activity.path}
                                  <small>{activity.source ?? ''}{activity.source && activity.path ? ' · ' : ''}{activity.path ?? ''}</small>
                                {/if}
                              </div>
                            </li>
                          {/each}
                        </ol>
                      </section>
                    {/if}
                    <footer class="turn-summary">
                      <span>{turnCompleteness(turn)}</span>
                      {#if turn.presentation?.usage}<span>Usage reported</span>{/if}
                      {#if turn.humanInterventionAtMs}
                        <span>Human input observed; effects are not agent-only</span>
                      {/if}
                      {#if turn.error}<span class="turn-error">{turn.error}</span>{/if}
                    </footer>
                    <details class="turn-evidence">
                      <summary>Evidence</summary>
                      <dl>
                        <div><dt>Workspace revision</dt><dd>{turn.sourceRevision}</dd></div>
                        <div><dt>Projection</dt><dd>{turn.presentation ? `v${turn.presentation.schemaVersion}` : 'legacy'}</dd></div>
                        {#if turn.presentation?.sourceDigest}
                          <div><dt>Source digest</dt><dd>{turn.presentation.sourceDigest}</dd></div>
                        {/if}
                      </dl>
                      <div class="turn-capability-revisions">
                        <span class="label">Capability revisions</span>
                        <ul>
                          {#each Object.entries(turn.capabilityRevisions) as [source, revision]}
                            <li><strong>{source}</strong><span>{revision}</span></li>
                          {/each}
                        </ul>
                      </div>
                      {#if turn.presentation?.usage}
                        <div class="turn-usage">
                          <span class="label">Usage</span>
                          <pre>{pretty(turn.presentation.usage)}</pre>
                        </div>
                      {/if}
                      <details class="turn-raw-events">
                        <summary>Raw events</summary>
                        <ol class="run-events" aria-label={`Raw events for turn ${index + 1}`}>
                          {#each turnEvents(turn) as event}
                            <li>
                              <span class="sequence">{String(event.sequence).padStart(2, '0')}</span>
                              <div>
                                <strong>{eventLabel(event.type)}</strong>
                                {#if event.payload !== null}<pre>{pretty(event.payload)}</pre>{/if}
                              </div>
                            </li>
                          {:else}
                            <li class="empty">No raw events were retained for this turn.</li>
                          {/each}
                        </ol>
                      </details>
                    </details>
                  </article>
                {:else}
                  <div class="session-empty">
                    <strong>Ask the harness about this workspace</strong>
                    <p>The harness can discover the same catalog and analysis capabilities you can use in Explore.</p>
                    <code>agent "Find the active catalog items and explain what matters"</code>
                  </div>
                {/each}
              </div>
              {#if sessionAssembly}
                <details class="session-environment">
                  <summary>
                    <span>Session environment</span>
                    <small>{shortId(sessionAssembly.workspace.id.replace('/workspace', ''))} · {sessionAssembly.capabilitySources.length} capabilities</small>
                  </summary>
                  <dl>
                    <div><dt>Seed revision</dt><dd>{sessionAssembly.workspace.seedRevision}</dd></div>
                    <div><dt>Model</dt><dd>{activeAgentSession.summary.modelId}</dd></div>
                  </dl>
                  <ul>
                    {#each sessionAssembly.capabilitySources as source}
                      <li><strong>{source.id}</strong><span>{source.revision}</span><em>{source.projections.join(' + ')}</em></li>
                    {/each}
                  </ul>
                </details>
              {/if}
            </section>
          {:else if unavailableRunEvidence && !selectedRun}
            <section class="empty-state" data-testid="run-evidence-unavailable">
              <strong>Run evidence unavailable</strong>
              <p>Protected evidence was removed from this workbench. Start a new workspace to continue.</p>
            </section>
          {:else}
            {#if selectedRun?.assembly}
              <section class="assembly" data-testid="assembly">
                <div class="question">
                  <span class="label">Question</span>
                  <p>{selectedRun.assembly.question}</p>
                </div>
                <dl class="assembly-grid">
                  <div>
                    <dt>Harness</dt>
                    <dd>{selectedRun.assembly.harness.driver?.name ?? 'External driver'}</dd>
                    <small>{selectedRun.assembly.harness.driver ? `v${selectedRun.assembly.harness.driver.version}` : 'waiting for run'}</small>
                  </div>
                  <div>
                    <dt>Model</dt>
                    <dd>{selectedRun.assembly.harness.modelId ?? modelProfiles.find((profile) => profile.id === modelProfileId)?.displayName ?? (modelId.trim() || 'Choose a model')}</dd>
                    <small>{selectedRun.assembly.harness.adapter}</small>
                  </div>
                  {#if activeModelAccess}
                    <div class="model-access-cell" data-status={activeModelAccess.status}>
                      <dt>Model access</dt>
                      <dd>{activeModelAccess.status === 'ready' ? 'Ready' : 'Connect to run'}</dd>
                      <small>{activeModelAccess.source ?? activeModelAccess.displayName}</small>
                    </div>
                  {/if}
                  <div>
                    <dt>Workspace</dt>
                    <dd>{shortId(selectedRun.assembly.workspace.id.replace('/workspace', ''))}</dd>
                    <small>{selectedRun.assembly.workspace.attachment.replaceAll('-', ' ')}</small>
                  </div>
                  <div>
                    <dt>Seed revision</dt>
                    <dd>{selectedRun.assembly.workspace.seedRevision}</dd>
                    <small>{selectedRun.assembly.workspace.changeTracking.replaceAll('-', ' ')}</small>
                  </div>
                </dl>
                {#if activeModelAccess?.status === 'needs-setup'}
                  <div class="model-access-setup" role="status">
                    <strong>Connect {activeModelAccess.displayName}</strong>
                    {#if activeModelAccess.message}<p>{activeModelAccess.message}</p>{/if}
                    <p>{activeModelAccess.setupHint}</p>
                  </div>
                {/if}
                <div class="capabilities">
                  <span class="label">Capability sources</span>
                  <ul>
                    {#each selectedRun.assembly.capabilitySources as source}
                      <li>
                        <span><strong>{source.id}</strong><small>{source.revision}</small></span>
                        <em>{source.projections.join(' + ')}</em>
                      </li>
                    {:else}
                      <li class="waiting">Preparing capability sources…</li>
                    {/each}
                  </ul>
                </div>
              </section>
              <div class="activity-heading">
                <span class="label">{agentView === 'review' ? 'Run review' : 'Raw trace'}</span>
                <div class="agent-view-toggle" aria-label="Agent run detail">
                  <button class:active={agentView === 'review'} on:click={() => (agentView = 'review')}>Review</button>
                  <button class:active={agentView === 'raw'} on:click={() => (agentView = 'raw')}>Raw trace</button>
                </div>
              </div>
            {/if}
            {#if agentView === 'review'}
              <section class="review" data-testid="run-review">
                {#if selectedRun?.review.steps.length}
                  <dl class="review-metrics">
                    <div><dt>Turns</dt><dd>{selectedRun.review.metrics.modelTurns}</dd></div>
                    <div><dt>Capabilities</dt><dd>{selectedRun.review.metrics.capabilityCalls}</dd></div>
                    <div><dt>Native actions</dt><dd>{selectedRun.review.metrics.nativeActions}</dd></div>
                    <div><dt>Effects</dt><dd>{selectedRun.review.metrics.workspaceChanges}</dd></div>
                    <div><dt>Duration</dt><dd>{duration(selectedRun.review.metrics.durationMs)}</dd></div>
                  </dl>
                  <ol class="review-steps" aria-label="Causal run review">
                    {#each selectedRun.review.steps as step}
                      <li data-kind={step.kind} data-status={step.status}>
                        <span class="review-marker">{String(step.ordinal).padStart(2, '0')}</span>
                        <div>
                          <div class="review-step-heading">
                            <strong>{step.title}</strong>
                            <span>{step.kind.replaceAll('-', ' ')}</span>
                          </div>
                          {#if step.detail}<p>{step.detail}</p>{/if}
                          <small>{step.source ? `source ${step.source} · ` : ''}{step.path ? `${step.path} · ` : ''}events {step.eventSequences.join(', ')}</small>
                        </div>
                      </li>
                    {/each}
                  </ol>
                {:else}
                  <div class="review-empty">
                    <strong>Start with the environment</strong>
                    <p>Explore the workspace yourself, then ask the selected harness to investigate it.</p>
                    <ol class="starting-points">
                      <li><code>catalog list | where active</code></li>
                      <li><code>agent "Find the active catalog items and explain what matters"</code></li>
                      <li><code>lab assembly</code></li>
                      <li><code>lab compare</code></li>
                    </ol>
                    <button on:click={() => (agentView = 'raw')}>Inspect preparation events</button>
                  </div>
                {/if}
              </section>
            {:else}
              <ol class="run-events" aria-label="Agent run events">
                {#each runEvents as event}
                  <li>
                    <span class="sequence">{String(event.sequence).padStart(2, '0')}</span>
                    <div>
                      <strong>{eventLabel(event.type)}</strong>
                      {#if event.payload !== null}<pre>{pretty(event.payload)}</pre>{/if}
                    </div>
                  </li>
                {:else}
                  <li class="empty">Model, tool, and workspace activity will stream here.</li>
                {/each}
              </ol>
            {/if}
          {/if}
        {:else if activeTab === 'workspace'}
          {#if unavailableRunEvidence && !selectedRun}
            <section class="empty-state" data-testid="run-evidence-unavailable">
              <strong>Workspace evidence unavailable</strong>
              <p>Protected workspace evidence was removed from this workbench.</p>
            </section>
          {:else}
            <section class="artifact">
              <span class="label">result.json</span>
              {#if selectedRun?.outputError}
                <p class="artifact-error">Output could not be parsed: {selectedRun.outputError}</p>
              {:else}
                <pre>{pretty(selectedRun?.output)}</pre>
              {/if}
            </section>
          {/if}
        {:else if activeTab === 'editor'}
          <section class="empty-state">
            <strong>No editor for this scenario</strong>
            <p>The catalog run uses the shared filesystem directly. Editor diagnostics belong to scenarios that opt into an editor.</p>
          </section>
        {:else if activeTab === 'evidence'}
          <section class="artifact">
            <span class="label">Score</span>
            <pre>{pretty(selectedRun?.score ?? unavailableRunEvidence?.score)}</pre>
          </section>
        {:else}
          <section class="evaluation" data-testid="evaluation-view">
            {#if selectedEvaluation}
              <div class="evaluation-heading">
                <div>
                  <span class="label">Behavioral comparison</span>
                  <strong>{selectedEvaluation.summary.sourceRevision}</strong>
                </div>
                <span class="run-status" data-status={selectedEvaluation.summary.status}>{selectedEvaluation.summary.status}</span>
              </div>
              <div class="comparison-context">
                <span>Same revision</span>
                <span>{selectedEvaluation.summary.modelProfileId}</span>
                <span>Same prompt, capabilities, and limits</span>
              </div>
              <div class="behavior-scroll">
                <div class="behavior-grid" data-testid="behavioral-diff">
                  <div class="behavior-row behavior-header">
                    <div class="phase-heading">
                      <span class="label">Behavior</span>
                      <small>elapsed wall clock</small>
                    </div>
                    {#each selectedEvaluation.summary.arms as arm}
                      <div class="arm-summary">
                        <div class="arm-heading">
                          <strong>{arm.harnessId}</strong>
                          <span data-status={arm.status}>{arm.status}</span>
                        </div>
                        {#if comparisonArm(arm.harnessId)?.metrics}
                          <dl>
                            <div><dt>Steps</dt><dd>{comparisonArm(arm.harnessId)?.metrics?.modelTurns}</dd></div>
                            <div><dt>Calls</dt><dd>{comparisonArm(arm.harnessId)?.metrics?.capabilityCalls}</dd></div>
                            <div><dt>Native</dt><dd>{comparisonArm(arm.harnessId)?.metrics?.nativeActions}</dd></div>
                            <div><dt>Duration</dt><dd>{duration(comparisonArm(arm.harnessId)?.metrics?.durationMs ?? null)}</dd></div>
                          </dl>
                          <div class="arm-reporting">
                            <span>Usage <strong>{usage(comparisonArm(arm.harnessId)?.usage ?? 'not reported')}</strong></span>
                            <span>Cache <strong>{usage(comparisonArm(arm.harnessId)?.cache ?? 'not reported')}</strong></span>
                          </div>
                        {:else}
                          <small>{!arm.runId
                            ? 'Waiting to start'
                            : comparisonArm(arm.harnessId)?.evidenceComplete === false
                              ? 'Run evidence unavailable'
                              : 'Loading run evidence'}</small>
                        {/if}
                        <div class="clock-axis" aria-label={`Shared elapsed wall clock from zero to ${duration(comparisonClockMs)}`}>
                          <span>0s</span>
                          <span>{duration(comparisonClockMs / 2)}</span>
                          <span>{duration(comparisonClockMs)}</span>
                        </div>
                      </div>
                    {/each}
                  </div>

                  {#each behaviorRows as row}
                    <div class="behavior-row" data-phase={row.key}>
                      <div class="phase-heading">
                        <span>{row.label}</span>
                        <small>{row.kind.replaceAll('-', ' ')}</small>
                      </div>
                      {#each selectedEvaluation.summary.arms as arm}
                        <div class="behavior-cell">
                          {#if row.segments[arm.harnessId]}
                            <div class:startup={row.kind === 'harness' || row.kind === 'startup'} class="phase-clock" aria-label={clockRange(row.segments[arm.harnessId])}>
                              <div class="clock-track" style={clockStyle(row.segments[arm.harnessId])}>
                                <span class="clock-range"></span>
                                <span class="clock-end"></span>
                                {#if row.kind === 'harness' || row.kind === 'startup'}
                                  <strong class="clock-end-label" data-edge={clockEndEdge(row.segments[arm.harnessId])}>{clockEndLabel(row.segments[arm.harnessId])}</strong>
                                {/if}
                              </div>
                              <small>{clockRange(row.segments[arm.harnessId])}</small>
                            </div>
                            {#each row.segments[arm.harnessId]?.steps ?? [] as step}
                              {#if step.kind === 'model-turn'}
                                <div class="model-observation">
                                  <span>{step.title}</span>
                                  {#if step.detail}<p>{step.detail}</p>{/if}
                                </div>
                              {:else}
                                <div class="behavior-action" data-kind={step.kind}>
                                  <div>
                                    <span>{step.title}</span>
                                    <em>{step.status}</em>
                                  </div>
                                  {#if step.detail}<p>{step.detail}</p>{/if}
                                  {#if step.path}<small>{normalizedPath(step.path)}</small>{/if}
                                </div>
                              {/if}
                            {/each}
                          {:else}
                            <span class="not-observed">Not observed</span>
                          {/if}
                        </div>
                      {/each}
                    </div>
                  {/each}

                  <div class="behavior-row result-row">
                    <div class="phase-heading">
                      <span>Artifact</span>
                      <small>result.json</small>
                    </div>
                    {#each selectedEvaluation.summary.arms as arm}
                      <div class="result-cell">
                        <pre>{comparisonArm(arm.harnessId)?.evidenceComplete === false
                          ? 'Run evidence unavailable.'
                          : pretty(evaluationRuns[arm.harnessId]?.output)}</pre>
                      </div>
                    {/each}
                  </div>
                </div>
              </div>

              <div class="paired-result" data-match={selectedEvaluation.comparison?.outputsMatch ?? false}>
                <span class="label">Result</span>
                {#if selectedEvaluation.comparison}
                  <strong>{selectedEvaluation.comparison.artifactComparison === 'same' ||
                  (!selectedEvaluation.comparison.artifactComparison && selectedEvaluation.comparison.outputsMatch)
                    ? 'Same evaluated artifact'
                    : selectedEvaluation.comparison.artifactComparison === 'different'
                      ? 'Artifacts differ'
                      : 'Artifact unavailable'}</strong>
                  <p>Both scores remain scenario-specific. Timing, turns, and usage are supporting evidence rather than a universal ranking.</p>
                {:else}
                  <strong>Comparison in progress</strong>
                  <p>The result comparison will appear after both native runs finalize.</p>
                {/if}
              </div>

              <details class="native-replays">
                <summary>Native replays and raw evidence</summary>
                <p>Open either arm to inspect its full normalized review, native event stream, workspace, and score.</p>
                <div>
                  {#each selectedEvaluation.summary.arms as arm}
                    <button
                      disabled={!arm.runId || comparisonArm(arm.harnessId)?.evidenceComplete === false}
                      on:click={() => void openEvaluationArm(arm.runId)}
                    >
                      {comparisonArm(arm.harnessId)?.evidenceComplete === false
                        ? `${arm.harnessId} replay unavailable`
                        : `Open ${arm.harnessId} replay`}
                    </button>
                  {/each}
                </div>
              </details>
            {:else}
              <div class="empty-state">
                <strong>No evaluation selected</strong>
                <p>Snapshot the Explore workspace and compare v0 with Eve to inspect both native runs here.</p>
              </div>
            {/if}
          </section>
        {/if}
      </div>

      <div class="histories">
        <div class="history">
          <div class="history-title">
            <span class="label">Run history</span>
            <span>{runs.length}</span>
          </div>
          <div class="history-list">
            {#each runs as run}
              <button class:selected={inspectedRun?.id === run.id} on:click={() => void openRun(run.id)}>
                <span class="history-status" data-status={run.status}></span>
                <span>
                  <strong>{run.scenarioTitle}</strong>
                  <small>{shortId(run.id)} · {run.modelId}</small>
                </span>
                <em>{run.status}</em>
              </button>
            {:else}
              <p>No completed runs yet.</p>
            {/each}
          </div>
        </div>
        <div class="history evaluation-history">
          <div class="history-title">
            <span class="label">Evaluation history</span>
            <span>{evaluations.length}</span>
          </div>
          <div class="history-list">
            {#each evaluations as evaluation}
              <button class:selected={selectedEvaluation?.summary.id === evaluation.id} on:click={() => void openEvaluation(evaluation.id)}>
                <span class="history-status" data-status={evaluation.status}></span>
                <span>
                  <strong>{evaluation.harnessIds.join(' / ')}</strong>
                  <small>{shortId(evaluation.id)} · {evaluation.modelProfileId}</small>
                </span>
                <em>{evaluation.status}</em>
              </button>
            {:else}<p>No evaluations yet.</p>{/each}
          </div>
        </div>
      </div>
    </aside>
  </section>
</main>

<style>
  :global(*) { box-sizing: border-box; }
  :global(html) {
    --font-sans: "Geist Variable", ui-sans-serif, system-ui, sans-serif;
    --font-mono: "Geist Mono Variable", ui-monospace, monospace;
    color-scheme: dark;
    font-family: var(--font-sans);
    background: #0a0e0d;
  }
  :global(body) {
    margin: 0;
    min-width: 320px;
    min-height: 100vh;
    color: #d9e0dc;
    background: radial-gradient(circle at 12% -10%, rgba(62, 111, 90, 0.16), transparent 30rem), #0a0e0d;
  }
  button, select { font: inherit; }
  main { width: min(1600px, calc(100% - 40px)); margin: 0 auto; padding: 22px 0 30px; }
  header { display: grid; grid-template-columns: auto minmax(680px, 1fr) auto; align-items: end; gap: 24px; margin-bottom: 16px; }
  .identity { display: flex; align-items: center; gap: 11px; }
  .mark { display: grid; place-items: center; width: 30px; height: 30px; border: 1px solid #345348; border-radius: 7px; color: #9bc47c; font-weight: 650; }
  h1 { margin: 0; color: #f1f4f2; font-size: 1rem; font-weight: 610; letter-spacing: -0.02em; }
  .identity p { margin: 3px 0 0; color: #718078; font-size: 0.73rem; }
  .run-controls { display: flex; flex-wrap: nowrap; justify-content: flex-end; align-items: end; gap: 6px; min-width: 0; }
  label { display: grid; gap: 4px; }
  .run-controls label { flex: 1 1 130px; min-width: 0; max-width: 180px; }
  label > span, .label, .transport { color: #73847b; font-size: 0.62rem; font-weight: 680; letter-spacing: 0.12em; text-transform: uppercase; }
  select { width: 100%; min-width: 0; min-height: 30px; border: 1px solid #293730; border-radius: 6px; padding: 0 8px; color: #cbd5cf; background: #111715; font-size: 0.7rem; line-height: 1; outline: none; }
  select:focus { border-color: #4b6d5e; }
  .run-controls .model-field { flex-basis: 190px; max-width: 230px; }
  .run-controls button { flex: none; height: 30px; margin: 0; padding: 0 11px; font-size: 0.7rem; line-height: 1; white-space: nowrap; }
  .model-access-pill { display: grid; flex: none; align-content: center; gap: 1px; min-height: 30px; padding: 0 8px; border: 1px solid #34443c; border-radius: 6px; background: #111715; }
  .model-access-pill span { color: #6d7c74; font-size: 0.5rem; font-weight: 650; letter-spacing: 0.08em; text-transform: uppercase; }
  .model-access-pill strong { color: #a8c994; font-size: 0.61rem; font-weight: 540; white-space: nowrap; }
  .model-access-pill[data-status="needs-setup"] { border-color: #695532; background: #211c12; }
  .model-access-pill[data-status="needs-setup"] strong { color: #d9b46d; }
  button { border: 0; color: inherit; background: transparent; cursor: pointer; }
  button:disabled { cursor: not-allowed; opacity: 0.45; }
  .primary { border-radius: 6px; color: #101710; background: #9bc47c; font-weight: 620; }
  .quiet { border: 1px solid #34423b; border-radius: 6px; }
  .compare { white-space: nowrap; }
  .danger { color: #df8c8c; }
  .connection { display: flex; align-items: center; gap: 7px; padding-bottom: 9px; color: #89968f; font-family: var(--font-mono); font-size: 0.68rem; }
  .status-dot, .history-status { width: 6px; height: 6px; border-radius: 50%; background: #d1a85e; }
  .connection[data-state="connected"] .status-dot, .history-status[data-status="passed"] { background: #91b976; }
  .connection[data-state="error"] .status-dot, .connection[data-state="closed"] .status-dot, .history-status[data-status="failed"] { background: #d26d73; }
  .history-status[data-status="cancelled"] { background: #8d9691; }
  .banner { margin-bottom: 12px; padding: 9px 12px; border: 1px solid #653d40; border-radius: 6px; color: #e4a2a5; background: #251719; font-size: 0.75rem; }
  .bench { display: grid; grid-template-columns: minmax(0, 1.18fr) minmax(430px, 0.82fr); height: max(600px, calc(100dvh - 120px)); min-height: 0; overflow: hidden; border: 1px solid #27342f; border-radius: 12px; background: #101614; box-shadow: 0 28px 80px rgba(0, 0, 0, 0.26); }
  .terminal-panel { display: grid; grid-template-rows: 58px minmax(0, 1fr) 34px; min-width: 0; min-height: 0; }
  .panel-heading, .run-heading { display: flex; align-items: center; justify-content: space-between; gap: 14px; padding: 12px 17px; border-bottom: 1px solid #27342f; }
  .panel-heading > div, .run-heading > div { display: grid; gap: 4px; min-width: 0; }
  .value, .run-heading strong { overflow: hidden; color: #cbd4cf; font-size: 0.78rem; font-weight: 480; text-overflow: ellipsis; white-space: nowrap; }
  .transport { color: #526159; font-family: var(--font-mono); }
  .terminal-frame { position: relative; min-width: 0; min-height: 0; overflow: hidden; contain: layout paint; }
  .terminal-host { position: absolute; inset: 14px; overflow: hidden; border-radius: 4px; outline: none; background: #101614; }
  :global(.terminal-host canvas) { display: block; }
  .screen-reader-output { position: absolute; width: 1px; height: 1px; overflow: hidden; contain: strict; clip: rect(0 0 0 0); clip-path: inset(50%); white-space: pre; }
  .terminal-footer { display: flex; align-items: center; gap: 18px; padding: 0 17px; border-top: 1px solid #202c27; color: #58665f; font-family: var(--font-mono); font-size: 0.63rem; }
  .terminal-footer span:last-child { margin-left: auto; }
  .run-panel { display: grid; grid-template-rows: 44px 58px minmax(0, 1fr) auto; min-width: 0; min-height: 0; border-left: 1px solid #27342f; background: #0d1311; }
  .tabs {
    display: flex;
    min-inline-size: 0;
    gap: 2px;
    overflow-x: auto;
    overscroll-behavior-inline: contain;
    padding: 5px 7px 0;
    border-bottom: 1px solid #27342f;
    scrollbar-width: none;
    scroll-padding-inline: 7px;
  }
  .tabs::-webkit-scrollbar { display: none; }
  .tabs button { position: relative; flex: 0 0 auto; padding: 0 10px; color: #6f7d76; font-size: 0.7rem; white-space: nowrap; }
  .tabs button.active { color: #d2dad6; }
  .tabs button.active::after { position: absolute; right: 8px; bottom: -1px; left: 8px; height: 2px; background: #91b976; content: ''; }
  .run-status { padding: 4px 8px; border: 1px solid #34443c; border-radius: 999px; color: #a9b6af; font-family: var(--font-mono); font-size: 0.63rem; }
  .run-status[data-status="passed"] { border-color: #46604f; color: #b8d5a8; background: #16211a; }
  .run-status[data-status="failed"] { border-color: #684146; color: #e09ba0; background: #251719; }
  .run-status[data-status="cancelled"] { border-color: #4a5550; color: #b2bbb6; background: #171d1a; }
  .tab-content { min-block-size: 0; overflow: auto; overscroll-behavior-block: contain; scrollbar-color: #405048 transparent; scrollbar-gutter: stable; contain: layout paint; }
  .assembly { padding: 16px 17px 14px; border-bottom: 1px solid #27342f; }
  .question { padding: 12px 13px; border: 1px solid #293832; border-radius: 7px; background: #111916; }
  .question p { margin: 6px 0 0; color: #c5d0ca; font-size: 0.76rem; line-height: 1.48; }
  .assembly-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1px; margin: 13px 0 0; overflow: hidden; border: 1px solid #24312c; border-radius: 7px; background: #24312c; }
  .assembly-grid > div { display: grid; gap: 3px; min-width: 0; padding: 10px 11px; background: #0e1512; }
  .assembly-grid dt { color: #68776f; font-size: 0.57rem; font-weight: 680; letter-spacing: 0.1em; text-transform: uppercase; }
  .assembly-grid dd { overflow: hidden; margin: 0; color: #b9c5bf; font-family: var(--font-mono); font-size: 0.67rem; text-overflow: ellipsis; white-space: nowrap; }
  .assembly-grid small { overflow: hidden; color: #56655d; font-size: 0.59rem; text-overflow: ellipsis; white-space: nowrap; }
  .model-access-cell[data-status="ready"] dd { color: #a8c994; }
  .model-access-cell[data-status="needs-setup"] dd { color: #d9b46d; }
  .model-access-setup { margin-top: 10px; padding: 10px 11px; border: 1px solid #5d4b2f; border-radius: 7px; background: #1c1810; }
  .model-access-setup strong { color: #d3b276; font-size: 0.67rem; font-weight: 560; }
  .model-access-setup p { margin: 4px 0 0; color: #927e59; font-family: var(--font-mono); font-size: 0.57rem; line-height: 1.45; }
  .capabilities { margin-top: 13px; }
  .capabilities ul { display: flex; flex-wrap: wrap; gap: 6px; margin: 8px 0 0; padding: 0; list-style: none; }
  .capabilities li { display: flex; align-items: center; gap: 12px; min-width: 180px; padding: 7px 9px; border: 1px solid #26352f; border-radius: 6px; background: #0d1411; }
  .capabilities li > span { display: grid; gap: 1px; }
  .capabilities strong { color: #aebbb4; font-family: var(--font-mono); font-size: 0.66rem; font-weight: 540; }
  .capabilities small, .capabilities em { color: #5f6f66; font-family: var(--font-mono); font-size: 0.56rem; font-style: normal; }
  .capabilities em { margin-left: auto; color: #78966c; }
  .capabilities .waiting { color: #617068; font-size: 0.65rem; }
  .session-view { background: #0b1210; }
  .session-intro { display: flex; align-items: start; justify-content: space-between; gap: 14px; padding: 13px 17px; border-bottom: 1px solid #1d2924; }
  .session-intro > div:first-child { min-width: 0; }
  .session-intro p { margin: 5px 0 0; color: #8c9a92; font-size: 0.7rem; line-height: 1.45; }
  .answer-view-toggle { display: flex; flex: none; gap: 2px; padding: 2px; border: 1px solid #293730; border-radius: 5px; background: #0a100e; }
  .answer-view-toggle button { border-radius: 3px; padding: 4px 7px; color: #718078; font-size: 0.56rem; }
  .answer-view-toggle button.active { color: #cbd5cf; background: #1a2520; }
  .answer-view-toggle button:focus-visible { outline: 2px solid #789d6b; outline-offset: 2px; }
  .session-turns { display: grid; }
  .session-turn { min-width: 0; padding: 15px 17px 13px; border-bottom: 1px solid #27342f; }
  .session-turn > header { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 12px; color: #718078; font-family: var(--font-mono); font-size: 0.59rem; }
  .session-turn > header h2 { margin: 0; color: inherit; font: inherit; }
  .session-turn > header > div { display: flex; align-items: center; gap: 9px; }
  .session-turn > header time { color: #65746c; }
  .session-turn > header em { color: #7d8c84; font-style: normal; }
  .session-turn > header em[data-status="completed"] { color: #a8c994; }
  .session-turn > header em[data-status="failed"], .session-turn > header em[data-status="cancelled"] { color: #d98d92; }
  .turn-prompt, .turn-response { display: grid; grid-template-columns: 46px minmax(0, 1fr); gap: 10px; }
  .turn-prompt > span, .turn-response > span { padding-top: 2px; color: #6e7d75; font-size: 0.6rem; font-weight: 680; letter-spacing: 0.08em; text-transform: uppercase; }
  .turn-prompt p { margin: 0; color: #c9d2cd; font-size: 0.76rem; line-height: 1.48; }
  .turn-response { margin-top: 12px; }
  .turn-response > p { margin: 0; color: #d9e0dc; font-size: 0.78rem; line-height: 1.56; white-space: pre-wrap; }
  .assistant-messages { display: grid; min-width: 0; gap: 10px; }
  .assistant-message { min-width: 0; }
  .assistant-message + .assistant-message { padding-top: 10px; border-top: 1px solid #202d27; }
  .response-source { max-width: 100%; max-height: 280px; margin: 0; overflow: auto; padding: 9px 10px; border: 1px solid #28372f; border-radius: 6px; color: #aab8b1; background: #09100d; scrollbar-color: #405048 transparent; }
  .response-source:focus-visible { outline: 2px solid #789d6b; outline-offset: 2px; }
  .turn-response .response-pending { color: #8fa099; }
  .turn-response .response-unavailable { color: #78867f; font-style: italic; }
  .provided-context { margin: 9px 0 0 56px; border: 1px solid #26342e; border-radius: 6px; background: #0a100e; }
  .provided-context summary, .turn-evidence > summary, .turn-raw-events > summary, .session-environment > summary { cursor: pointer; }
  .provided-context summary { padding: 7px 9px; color: #84938b; font-size: 0.62rem; }
  .provided-context pre { max-height: 150px; margin: 0; padding: 0 9px 9px; }
  .turn-activity { margin: 14px 0 0 56px; }
  .turn-activity > ol { display: grid; gap: 1px; margin: 7px 0 0; padding: 0; overflow: hidden; border: 1px solid #24312c; border-radius: 6px; background: #24312c; list-style: none; }
  .turn-activity li { display: grid; grid-template-columns: 8px minmax(0, 1fr); gap: 9px; padding: 8px 9px; background: #0d1411; }
  .activity-marker { align-self: start; width: 6px; height: 6px; margin-top: 5px; border: 1px solid #688275; border-radius: 50%; background: #18231e; }
  .turn-activity li[data-kind*="workspace"] .activity-marker { border-color: #89aa74; background: #31462a; }
  .turn-activity li[data-status="failed"] .activity-marker { border-color: #b16067; background: #3b2023; }
  .activity-title { display: flex; align-items: baseline; justify-content: space-between; gap: 10px; }
  .activity-title strong { color: #bfc9c4; font-size: 0.69rem; font-weight: 540; }
  .activity-title em { color: #718078; font-size: 0.52rem; font-style: normal; text-transform: capitalize; }
  .turn-activity p { margin: 2px 0 0; color: #899990; font-size: 0.63rem; line-height: 1.4; }
  .turn-activity small { display: block; margin-top: 3px; color: #687870; font-family: var(--font-mono); font-size: 0.54rem; }
  .turn-summary { display: flex; flex-wrap: wrap; gap: 6px 12px; margin: 12px 0 0 56px; color: #6f7e76; font-family: var(--font-mono); font-size: 0.54rem; }
  .turn-summary .turn-error { color: #d98d92; }
  .turn-evidence { margin: 10px 0 0 56px; border-top: 1px solid #1f2b26; }
  .turn-evidence > summary { padding: 8px 0 0; color: #718078; font-size: 0.59rem; }
  .turn-evidence > dl { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 7px 12px; margin: 10px 0; }
  .turn-evidence > dl > div { min-width: 0; }
  .turn-evidence dt { color: #65746c; font-size: 0.52rem; letter-spacing: 0.06em; text-transform: uppercase; }
  .turn-evidence dd { overflow-wrap: anywhere; margin: 2px 0 0; color: #92a099; font-family: var(--font-mono); font-size: 0.55rem; }
  .turn-capability-revisions ul, .session-environment ul { display: grid; gap: 4px; margin: 7px 0 0; padding: 0; list-style: none; }
  .turn-capability-revisions li, .session-environment li { display: flex; align-items: baseline; gap: 8px; color: #718078; font-family: var(--font-mono); font-size: 0.55rem; }
  .turn-capability-revisions strong, .session-environment strong { color: #95a39b; font-weight: 540; }
  .turn-usage { margin-top: 10px; }
  .turn-usage pre { margin-top: 5px; }
  .turn-raw-events { margin-top: 10px; }
  .turn-raw-events > summary { color: #718078; font-size: 0.59rem; }
  .turn-raw-events .run-events { padding: 4px 0 0; }
  .turn-raw-events .run-events li { padding: 8px 0; }
  .session-empty { margin: 16px 17px; padding: 18px; border: 1px dashed #304039; border-radius: 7px; }
  .session-empty strong { color: #c3cdc8; font-size: 0.76rem; font-weight: 560; }
  .session-empty p { margin: 5px 0 11px; color: #87958e; font-size: 0.68rem; line-height: 1.45; }
  .session-empty code { display: block; overflow-wrap: anywhere; padding: 8px 9px; border: 1px solid #293832; border-radius: 5px; color: #a8c994; background: #0a100e; font-family: var(--font-mono); font-size: 0.59rem; }
  .session-environment { margin: 0; padding: 0 17px 14px; border-bottom: 1px solid #27342f; }
  .session-environment > summary { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 11px 0; color: #89978f; font-size: 0.64rem; }
  .session-environment > summary small { color: #617068; font-family: var(--font-mono); font-size: 0.54rem; }
  .session-environment > dl { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 9px; margin: 0 0 10px; }
  .session-environment dt { color: #65746c; font-size: 0.52rem; text-transform: uppercase; }
  .session-environment dd { overflow-wrap: anywhere; margin: 2px 0 0; color: #98a69f; font-family: var(--font-mono); font-size: 0.57rem; }
  .session-environment li em { margin-left: auto; color: #78966c; font-style: normal; }
  .activity-heading { display: flex; align-items: center; justify-content: space-between; padding: 10px 17px 8px; color: #596760; font-size: 0.6rem; }
  .agent-view-toggle { display: flex; gap: 2px; padding: 2px; border: 1px solid #26342e; border-radius: 6px; background: #0a100e; }
  .agent-view-toggle button { padding: 4px 8px; border-radius: 4px; color: #65746c; font-size: 0.59rem; }
  .agent-view-toggle button.active { color: #c3cec8; background: #18221e; }
  .review { padding: 0 17px 20px; }
  .review-metrics { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); margin: 0 0 10px; overflow: hidden; border: 1px solid #24312c; border-radius: 7px; }
  .review-metrics > div { display: grid; gap: 3px; min-width: 0; padding: 8px 9px; border-left: 1px solid #24312c; background: #0c1210; }
  .review-metrics > div:first-child { border-left: 0; }
  .review-metrics dt { overflow: hidden; color: #65736c; font-size: 0.53rem; font-weight: 650; letter-spacing: 0.07em; text-overflow: ellipsis; text-transform: uppercase; white-space: nowrap; }
  .review-metrics dd { margin: 0; color: #b7c2bc; font-family: var(--font-mono); font-size: 0.7rem; }
  .review-steps { margin: 0; padding: 0; list-style: none; }
  .review-steps li { position: relative; display: grid; grid-template-columns: 30px minmax(0, 1fr); gap: 8px; padding: 8px 0; }
  .review-steps li:not(:last-child)::after { position: absolute; top: 27px; bottom: -4px; left: 12px; width: 1px; background: #293730; content: ''; }
  .review-marker { z-index: 1; display: grid; place-items: center; align-self: start; width: 25px; height: 20px; border: 1px solid #405048; border-radius: 999px; color: #87968e; background: #0d1311; font-family: var(--font-mono); font-size: 0.56rem; }
  .review-steps li[data-status="passed"] .review-marker, .review-steps li[data-status="completed"] .review-marker { border-color: #405b4b; color: #96b783; }
  .review-steps li[data-status="failed"] .review-marker { border-color: #6b3e42; color: #d5868b; }
  .review-step-heading { display: flex; align-items: baseline; justify-content: space-between; gap: 10px; }
  .review-step-heading strong { color: #c2ccc7; font-size: 0.72rem; font-weight: 540; }
  .review-step-heading span { color: #829189; font-size: 0.54rem; letter-spacing: 0.06em; text-transform: uppercase; }
  .review-steps p { margin: 2px 0; color: #94a39b; font-size: 0.67rem; line-height: 1.42; }
  .review-steps small { color: #718078; font-family: var(--font-mono); font-size: 0.54rem; }
  .review-empty { margin-top: 2px; padding: 22px 15px; border: 1px dashed #2a3832; border-radius: 7px; color: #738179; }
  .review-empty strong { color: #b9c4be; font-size: 0.76rem; font-weight: 540; }
  .review-empty p { margin: 6px 0 12px; font-size: 0.67rem; line-height: 1.5; }
  .starting-points { display: grid; gap: 5px; margin: 0 0 14px; padding: 0; list-style: none; }
  .starting-points li { min-width: 0; }
  .starting-points code { display: block; overflow: hidden; padding: 6px 8px; border: 1px solid #26342e; border-radius: 5px; color: #91aa9d; background: #0b110f; font-family: var(--font-mono); font-size: 0.59rem; text-overflow: ellipsis; white-space: nowrap; }
  .review-empty button { padding: 6px 9px; border: 1px solid #324139; border-radius: 5px; color: #91aa9d; font-size: 0.62rem; }
  .run-events { margin: 0; padding: 8px 17px 20px; list-style: none; }
  .run-events li { display: grid; grid-template-columns: 27px minmax(0, 1fr); gap: 7px; padding: 11px 0; border-bottom: 1px solid #1d2924; content-visibility: auto; contain-intrinsic-block-size: 72px; }
  .run-events .sequence { color: #536159; font-family: var(--font-mono); font-size: 0.65rem; }
  .run-events strong { color: #b9c5bf; font-family: var(--font-mono); font-size: 0.69rem; font-weight: 510; }
  pre { margin: 7px 0 0; overflow: auto; color: #82928a; font-family: var(--font-mono); font-size: 0.64rem; line-height: 1.55; white-space: pre-wrap; word-break: break-word; }
  .run-events .empty { display: block; padding: 28px 0; color: #5f6b65; font-size: 0.75rem; }
  .artifact { padding: 18px; }
  .artifact > pre { min-height: 280px; margin-top: 12px; padding: 14px; border: 1px solid #202d27; border-radius: 6px; color: #aebbb4; background: #0a0f0d; }
  .artifact-error { margin-top: 12px; padding: 14px; border: 1px solid #56373a; border-radius: 6px; color: #cf8b90; background: #160f10; font-size: 0.7rem; line-height: 1.5; }
  .empty-state { max-width: 370px; padding: 34px 20px; color: #7c8a83; }
  .empty-state strong { color: #bbc5c0; font-size: 0.82rem; }
  .empty-state p { font-size: 0.73rem; line-height: 1.55; }
  .evaluation { padding: 18px; }
  .evaluation-heading { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 14px; }
  .evaluation-heading > div { display: grid; gap: 4px; min-width: 0; }
  .evaluation-heading strong { overflow: hidden; color: #b9c5bf; font-family: var(--font-mono); font-size: 0.67rem; text-overflow: ellipsis; white-space: nowrap; }
  .comparison-context { display: flex; flex-wrap: wrap; gap: 5px; margin-bottom: 12px; }
  .comparison-context span { padding: 4px 7px; border: 1px solid #293832; border-radius: 999px; color: #7d8d84; font-family: var(--font-mono); font-size: 0.53rem; }
  .behavior-scroll { margin: 0 -4px 12px; overflow-x: auto; overscroll-behavior-inline: contain; scrollbar-gutter: stable; }
  .behavior-grid { display: grid; grid-template-columns: minmax(86px, 0.42fr) repeat(2, minmax(225px, 1fr)); min-width: 590px; margin: 0 4px; overflow: hidden; border: 1px solid #293832; border-radius: 7px; background: #293832; gap: 1px; }
  .behavior-row { display: grid; grid-template-columns: subgrid; grid-column: 1 / -1; }
  .behavior-row > * { min-width: 0; background: #0c1310; }
  .behavior-header > * { background: #111916; }
  .phase-heading { display: grid; align-content: start; gap: 3px; padding: 11px 9px; }
  .phase-heading > span:not(.label) { overflow-wrap: anywhere; color: #91a098; font-size: 0.59rem; font-weight: 560; }
  .phase-heading small { color: #56655d; font-size: 0.49rem; letter-spacing: 0.05em; text-transform: uppercase; }
  .arm-summary { display: grid; gap: 9px; padding: 11px; }
  .arm-heading { display: flex; align-items: baseline; justify-content: space-between; gap: 10px; }
  .arm-heading strong { color: #d0d8d4; font-size: 0.76rem; font-weight: 570; text-transform: none; }
  .arm-heading span { color: #77867e; font-size: 0.54rem; text-transform: capitalize; }
  .arm-heading span[data-status="passed"] { color: #a8c994; }
  .arm-heading span[data-status="failed"] { color: #d98d92; }
  .arm-summary dl { display: flex; flex-wrap: wrap; gap: 5px 10px; margin: 0; }
  .arm-summary dl > div { display: flex; gap: 4px; }
  .arm-summary dt { color: #5f6f67; font-size: 0.5rem; text-transform: uppercase; }
  .arm-summary dd { margin: 0; color: #aab6b0; font-family: var(--font-mono); font-size: 0.55rem; }
  .arm-summary small { color: #66766d; font-family: var(--font-mono); font-size: 0.55rem; }
  .arm-reporting { display: grid; gap: 3px; color: #607068; font-size: 0.52rem; }
  .arm-reporting span { display: flex; justify-content: space-between; gap: 8px; }
  .arm-reporting strong { overflow: hidden; color: #8b9a92; font-family: var(--font-mono); font-size: inherit; font-weight: 450; text-overflow: ellipsis; white-space: nowrap; }
  .clock-axis { position: relative; display: flex; justify-content: space-between; padding-top: 5px; border-top: 1px solid #2a3932; color: #596860; font-family: var(--font-mono); font-size: 0.46rem; }
  .clock-axis::before, .clock-axis::after { position: absolute; top: 0; width: 1px; height: 4px; background: #405248; content: ''; }
  .clock-axis::before { left: 0; }
  .clock-axis::after { right: 0; }
  .behavior-cell { display: grid; align-content: start; gap: 8px; padding: 10px 11px; }
  .phase-clock { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: center; gap: 7px; min-height: 10px; }
  .phase-clock.startup { padding-top: 9px; }
  .clock-track { position: relative; height: 1px; background: #26352e; }
  .clock-range { position: absolute; top: -1px; left: var(--clock-start, 0%); width: var(--clock-span, 1.5%); min-width: 3px; height: 3px; border-radius: 999px; background: #78966f; }
  .clock-end { position: absolute; top: -4px; left: var(--clock-end, 0%); width: 1px; height: 9px; background: #a2be91; transform: translateX(-1px); }
  .clock-end::after { position: absolute; top: 2px; left: 50%; width: 3px; height: 3px; border-radius: 50%; background: #b3cea2; content: ''; transform: translateX(-50%); }
  .clock-end-label { position: absolute; bottom: 5px; left: var(--clock-end, 0%); color: #81947e; font-family: var(--font-mono); font-size: 0.44rem; font-weight: 500; white-space: nowrap; transform: translateX(-50%); }
  .clock-end-label[data-edge="start"] { transform: none; }
  .clock-end-label[data-edge="end"] { transform: translateX(-100%); }
  .phase-clock small { color: #66766d; font-family: var(--font-mono); font-size: 0.48rem; white-space: nowrap; }
  .model-observation { display: grid; gap: 3px; padding: 8px; border-left: 2px solid #384e43; background: #111a16; }
  .model-observation > span { color: #91a49a; font-family: var(--font-mono); font-size: 0.54rem; }
  .model-observation p, .behavior-action p { margin: 0; color: #9aa9a1; font-size: 0.63rem; line-height: 1.45; }
  .behavior-action { display: grid; gap: 4px; }
  .behavior-action > div { display: flex; align-items: baseline; justify-content: space-between; gap: 8px; }
  .behavior-action span { color: #c0cac5; font-size: 0.66rem; font-weight: 560; }
  .behavior-action em { color: #728279; font-size: 0.49rem; font-style: normal; text-transform: uppercase; }
  .behavior-action small { color: #718078; font-family: var(--font-mono); font-size: 0.53rem; }
  .behavior-action[data-kind="outcome"] span { color: #b3ce9f; }
  .not-observed { align-self: center; color: #56635c; font-size: 0.59rem; font-style: italic; }
  .result-cell { min-height: 190px; padding: 10px 11px; background: #09100d; }
  .result-cell pre { max-height: 260px; margin: 0; color: #aebbb4; }
  .paired-result { display: grid; gap: 6px; padding: 13px; border: 1px solid #293832; border-radius: 7px; background: #0a100e; }
  .paired-result > strong { color: #c4cec9; font-size: 0.75rem; font-weight: 550; }
  .paired-result[data-match="true"] > strong { color: #b4d2a2; }
  .paired-result p { margin: 0; color: #75847c; font-size: 0.64rem; line-height: 1.5; }
  .native-replays { margin-top: 10px; padding: 11px 12px; border: 1px solid #25332d; border-radius: 7px; color: #829189; background: #0c1310; }
  .native-replays summary { color: #aab6b0; font-size: 0.67rem; cursor: pointer; }
  .native-replays p { margin: 8px 0; color: #74837b; font-size: 0.61rem; line-height: 1.45; }
  .native-replays > div { display: flex; flex-wrap: wrap; gap: 7px; }
  .native-replays button { padding: 6px 9px; border: 1px solid #34463d; border-radius: 5px; color: #9bb0a5; font-size: 0.6rem; }
  .native-replays button:not(:disabled):hover { border-color: #567261; color: #c3d1ca; }
  .histories { min-block-size: 0; max-block-size: min(190px, 25dvb); overflow: auto; overscroll-behavior-block: contain; border-top: 1px solid #27342f; scrollbar-color: #405048 transparent; scrollbar-gutter: stable; }
  .history { display: grid; grid-template-rows: auto auto; }
  .evaluation-history { border-top: 1px solid #202c27; }
  .history-title { display: flex; justify-content: space-between; padding: 10px 16px 6px; color: #596760; font-size: 0.65rem; }
  .history-list { min-block-size: 0; padding: 0 7px 7px; }
  .history-list button { display: grid; grid-template-columns: 7px minmax(0, 1fr) auto; align-items: center; gap: 9px; width: 100%; padding: 8px 9px; border-radius: 5px; text-align: left; }
  .history-list button:hover, .history-list button.selected { background: #141d19; }
  .history-list button > span:nth-child(2) { display: grid; gap: 2px; min-width: 0; }
  .history-list strong { overflow: hidden; color: #aeb9b3; font-size: 0.68rem; font-weight: 520; text-overflow: ellipsis; white-space: nowrap; }
  .history-list small, .history-list em { overflow: hidden; color: #5e6c65; font-family: var(--font-mono); font-size: 0.58rem; font-style: normal; text-overflow: ellipsis; white-space: nowrap; }
  .history-list p { margin: 8px 9px; color: #56635c; font-size: 0.68rem; }
  @media (max-width: 1280px) {
    header { grid-template-columns: 1fr auto; }
    .run-controls { grid-row: 2; grid-column: 1 / -1; justify-content: flex-start; }
  }
  @media (max-width: 1050px) {
    .bench { grid-template-columns: 1fr; height: auto; min-height: 720px; }
    .terminal-panel { height: clamp(430px, calc(100dvh - 175px), 682px); }
    .run-panel { block-size: 100dvb; min-block-size: 0; border-top: 1px solid #27342f; border-left: 0; }
  }
  @media (max-width: 620px) {
    main { width: calc(100% - 20px); padding-top: 14px; }
    .run-controls { display: grid; grid-template-columns: 1fr 1fr; }
    .run-controls > *, .run-controls label, .run-controls .model-field { width: 100%; min-width: 0; max-width: none; }
    .bench { min-height: 600px; }
    .terminal-panel { height: clamp(400px, calc(100dvh - 167px), 620px); }
    .review-metrics { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    .review-metrics > div:nth-child(4) { border-left: 0; }
  }
</style>
