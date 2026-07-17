<script lang="ts">
  import '@fontsource-variable/geist';
  import '@fontsource-variable/geist-mono';
  import { onMount } from 'svelte';
  import { createRunClient, type RunDetail, type RunEvent, type RunSummary, type ScenarioManifest } from '$lib/runs';
  import { createGhosttySurface } from '$lib/terminal/ghostty';
  import { connectSession } from '$lib/terminal/session';
  import type { BrowserSession, ConnectionState, SessionEvent } from '$lib/terminal/session';
  import type { TerminalSurface } from '$lib/terminal/surface';

  type Tab = 'agent' | 'workspace' | 'editor' | 'evidence';

  const runClient = createRunClient();
  let terminalHost: HTMLDivElement;
  let surface: TerminalSurface | undefined;
  let session: BrowserSession | undefined;
  let eventStream: AbortController | undefined;
  let connectionState: ConnectionState = 'starting';
  let sessionEvents: SessionEvent[] = [];
  let screenText = '';
  let startupError = '';
  let scenarios: ScenarioManifest[] = [];
  let scenarioId = '';
  let modelId = '';
  let runs: RunSummary[] = [];
  let selectedRun: RunDetail | undefined;
  let runEvents: RunEvent[] = [];
  let activeTab: Tab = 'agent';
  let actionError = '';
  let preparing = false;
  let starting = false;

  $: activeRun = selectedRun?.summary;
  $: running = activeRun?.status === 'starting' || activeRun?.status === 'running';

  async function startTerminal(runId: string): Promise<void> {
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
          },
          onScreen(text) {
            screenText = text;
          }
        },
        runId
      );
      surface.focus();
    } catch (error) {
      connectionState = 'error';
      startupError = message(error);
    }
  }

  async function load(): Promise<void> {
    try {
      [scenarios, runs] = await Promise.all([runClient.scenarios(), runClient.runs()]);
      scenarioId ||= scenarios[0]?.id ?? '';
    } catch (error) {
      actionError = message(error);
    }
  }

  async function prepareScenario(): Promise<void> {
    if (!scenarioId || preparing || running) return;
    preparing = true;
    actionError = '';
    eventStream?.abort();
    try {
      const summary = await runClient.prepare(scenarioId);
      const detail = await runClient.detail(summary.id);
      selectedRun = detail;
      runEvents = detail.events;
      activeTab = 'agent';
      watchRun(summary.id);
      await startTerminal(summary.id);
    } catch (error) {
      actionError = message(error);
    } finally {
      preparing = false;
    }
  }

  async function initialize(): Promise<void> {
    await load();
    await prepareScenario();
  }

  async function beginRun(): Promise<void> {
    if (!selectedRun || selectedRun.summary.status !== 'exploring' || !modelId.trim() || starting) return;
    starting = true;
    actionError = '';
    activeTab = 'agent';
    eventStream?.abort();
    try {
      const summary = await runClient.startPrepared(selectedRun.summary.id, modelId.trim());
      selectedRun = {
        summary,
        assembly: selectedRun.assembly,
        events: runEvents,
        score: selectedRun?.score,
        output: selectedRun?.output
      };
      runs = [summary, ...runs.filter((run) => run.id !== summary.id)];
      watchRun(summary.id);
    } catch (error) {
      actionError = message(error);
    } finally {
      starting = false;
    }
  }

  function watchRun(id: string): void {
    eventStream?.abort();
    eventStream = runClient.events(id, (event) => {
      if (!runEvents.some((known) => known.sequence === event.sequence)) {
        runEvents = [...runEvents, event];
      }
      if (
        event.type === 'run.status' &&
        event.payload &&
        typeof event.payload === 'object' &&
        typeof (event.payload as { status?: unknown }).status === 'string' &&
        selectedRun?.summary.id === id
      ) {
        const status = (event.payload as { status: RunSummary['status'] }).status;
        selectedRun = {
          ...selectedRun,
          summary: { ...selectedRun.summary, status }
        };
        runs = runs.map((run) => (run.id === id ? { ...run, status } : run));
      }
      if (event.type === 'run.finished') void refreshRun(id);
    });
  }

  async function refreshRun(id: string): Promise<void> {
    try {
      const detail = await runClient.detail(id);
      selectedRun = detail;
      runEvents = detail.events;
      runs = await runClient.runs();
    } catch (error) {
      actionError = message(error);
    }
  }

  async function openRun(id: string): Promise<void> {
    actionError = '';
    eventStream?.abort();
    try {
      const detail = await runClient.detail(id);
      selectedRun = detail;
      runEvents = detail.events;
      activeTab = 'agent';
      if (detail.summary.status === 'starting' || detail.summary.status === 'running') watchRun(id);
      await startTerminal(id);
    } catch (error) {
      actionError = message(error);
    }
  }

  async function cancelRun(): Promise<void> {
    if (!activeRun) return;
    try {
      await runClient.cancel(activeRun.id);
    } catch (error) {
      actionError = message(error);
    }
  }

  function message(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
  }

  function pretty(value: unknown): string {
    return value === undefined || value === null ? 'Not available yet.' : JSON.stringify(value, null, 2);
  }

  function shortId(id: string): string {
    return id.split('-').at(-1) ?? id;
  }

  function eventLabel(type: string): string {
    return type.replaceAll('.', ' · ').replaceAll('-', ' ');
  }

  onMount(() => {
    void initialize();
    return () => {
      eventStream?.abort();
      session?.dispose();
      surface?.dispose();
    };
  });
</script>

<svelte:head>
  <title>Agent Lab</title>
  <meta name="description" content="Explore capabilities and inspect agent runs in one local workspace" />
</svelte:head>

<main>
  <header>
    <div class="identity">
      <span class="mark">A</span>
      <div>
        <h1>Agent Lab</h1>
        <p>Explore capabilities and inspect model behavior.</p>
      </div>
    </div>

    <div class="run-controls">
      <label>
        <span>Scenario</span>
        <select bind:value={scenarioId} aria-label="Scenario" disabled={preparing || running} on:change={() => void prepareScenario()}>
          {#each scenarios as scenario}
            <option value={scenario.id}>{scenario.title}</option>
          {/each}
        </select>
      </label>
      <label class="model-field">
        <span>Model ID</span>
        <input bind:value={modelId} placeholder="provider/model" aria-label="Model ID" />
      </label>
      <button class="primary" disabled={activeRun?.status !== 'exploring' || !modelId.trim() || preparing || starting || running} on:click={() => void beginRun()}>
        {starting ? 'Starting…' : 'Run'}
      </button>
      {#if running}
        <button class="quiet danger" on:click={() => void cancelRun()}>Cancel</button>
      {/if}
    </div>

    <div class="connection" data-state={connectionState} aria-live="polite">
      <span class="status-dot"></span>
      <span>{connectionState}</span>
    </div>
  </header>

  {#if actionError || startupError}
    <div class="banner" role="alert">{actionError || startupError}</div>
  {/if}

  <section class="bench" aria-label="Agent Lab workbench">
    <article class="terminal-panel">
      <div class="panel-heading">
        <div>
          <span class="label">Explore</span>
          <span class="value">{activeRun ? `${activeRun.scenarioTitle} workspace` : 'Preparing workspace…'}</span>
        </div>
        <span class="transport">PTY · Ghostty</span>
      </div>
      <div class="terminal-frame">
        <div class="terminal-host" bind:this={terminalHost} data-testid="terminal"></div>
        <pre class="screen-reader-output" data-testid="terminal-text" role="region" aria-label="Terminal output">{screenText}</pre>
      </div>
      <footer class="terminal-footer">
        <span>{sessionEvents.find((event) => event.type === 'started')?.provider ?? 'waiting'}</span>
        <span>{activeRun ? `run ${shortId(activeRun.id)}` : 'preparing'}</span>
        <span>loopback only</span>
      </footer>
    </article>

    <aside class="run-panel">
      <nav class="tabs" aria-label="Run views">
        <button class:active={activeTab === 'agent'} on:click={() => (activeTab = 'agent')}>Agent Run</button>
        <button class:active={activeTab === 'workspace'} on:click={() => (activeTab = 'workspace')}>Workspace</button>
        <button class:active={activeTab === 'editor'} on:click={() => (activeTab = 'editor')}>Editor</button>
        <button class:active={activeTab === 'evidence'} on:click={() => (activeTab = 'evidence')}>Evidence</button>
      </nav>

      <div class="run-heading">
        {#if activeRun}
          <div>
            <span class="label">{activeRun.scenarioTitle}</span>
            <strong>{activeRun.status === 'exploring' ? 'Ready for exploration' : activeRun.modelId}</strong>
          </div>
          <span class="run-status" data-status={activeRun.status}>{activeRun.status}</span>
        {:else}
          <div>
            <span class="label">Agent Run</span>
            <strong>Start a scenario to inspect it here.</strong>
          </div>
        {/if}
      </div>

      <div class="tab-content">
        {#if activeTab === 'agent'}
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
                  <dd>{selectedRun.assembly.harness.modelId ?? (modelId.trim() || 'Choose a model')}</dd>
                  <small>{selectedRun.assembly.harness.adapter}</small>
                </div>
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
              <span class="label">Activity</span>
              <span>{runEvents.length} events</span>
            </div>
          {/if}
          <ol class="run-events" aria-label="Agent run events">
            {#each runEvents as event}
              <li>
                <span class="sequence">{String(event.sequence).padStart(2, '0')}</span>
                <div>
                  <strong>{eventLabel(event.type)}</strong>
                  {#if event.payload !== null}
                    <pre>{pretty(event.payload)}</pre>
                  {/if}
                </div>
              </li>
            {:else}
              <li class="empty">Model, tool, and workspace activity will stream here.</li>
            {/each}
          </ol>
        {:else if activeTab === 'workspace'}
          <section class="artifact">
            <span class="label">result.json</span>
            <pre>{pretty(selectedRun?.output)}</pre>
          </section>
        {:else if activeTab === 'editor'}
          <section class="empty-state">
            <strong>No editor for this scenario</strong>
            <p>The catalog run uses the shared filesystem directly. Editor diagnostics belong to scenarios that opt into an editor.</p>
          </section>
        {:else}
          <section class="artifact">
            <span class="label">Score</span>
            <pre>{pretty(selectedRun?.score)}</pre>
          </section>
        {/if}
      </div>

      <div class="history">
        <div class="history-title">
          <span class="label">Run history</span>
          <span>{runs.length}</span>
        </div>
        <div class="history-list">
          {#each runs as run}
            <button class:selected={activeRun?.id === run.id} on:click={() => void openRun(run.id)}>
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
  button, input, select { font: inherit; }
  main { width: min(1600px, calc(100% - 40px)); margin: 0 auto; padding: 22px 0 30px; }
  header { display: grid; grid-template-columns: auto minmax(460px, 1fr) auto; align-items: end; gap: 24px; margin-bottom: 16px; }
  .identity { display: flex; align-items: center; gap: 11px; }
  .mark { display: grid; place-items: center; width: 30px; height: 30px; border: 1px solid #345348; border-radius: 7px; color: #9bc47c; font-weight: 650; }
  h1 { margin: 0; color: #f1f4f2; font-size: 1rem; font-weight: 610; letter-spacing: -0.02em; }
  .identity p { margin: 3px 0 0; color: #718078; font-size: 0.73rem; }
  .run-controls { display: flex; justify-content: flex-end; align-items: end; gap: 8px; }
  label { display: grid; gap: 5px; }
  label > span, .label, .transport { color: #73847b; font-size: 0.62rem; font-weight: 680; letter-spacing: 0.12em; text-transform: uppercase; }
  input, select { min-height: 34px; border: 1px solid #293730; border-radius: 6px; padding: 0 10px; color: #cbd5cf; background: #111715; outline: none; }
  input:focus, select:focus { border-color: #4b6d5e; }
  select { min-width: 165px; }
  .model-field { min-width: 220px; }
  .run-controls button { height: 34px; margin: 0; padding: 0 15px; }
  button { border: 0; color: inherit; background: transparent; cursor: pointer; }
  button:disabled { cursor: not-allowed; opacity: 0.45; }
  .primary { border-radius: 6px; color: #101710; background: #9bc47c; font-weight: 620; }
  .quiet { border: 1px solid #34423b; border-radius: 6px; }
  .danger { color: #df8c8c; }
  .connection { display: flex; align-items: center; gap: 7px; padding-bottom: 9px; color: #89968f; font-family: var(--font-mono); font-size: 0.68rem; }
  .status-dot, .history-status { width: 6px; height: 6px; border-radius: 50%; background: #d1a85e; }
  .connection[data-state="connected"] .status-dot, [data-status="passed"] { background: #91b976; }
  .connection[data-state="error"] .status-dot, .connection[data-state="closed"] .status-dot, [data-status="failed"] { background: #d26d73; }
  [data-status="cancelled"] { background: #8d9691; }
  .banner { margin-bottom: 12px; padding: 9px 12px; border: 1px solid #653d40; border-radius: 6px; color: #e4a2a5; background: #251719; font-size: 0.75rem; }
  .bench { display: grid; grid-template-columns: minmax(0, 1.18fr) minmax(430px, 0.82fr); height: max(600px, calc(100dvh - 120px)); min-height: 0; overflow: hidden; border: 1px solid #27342f; border-radius: 12px; background: #101614; box-shadow: 0 28px 80px rgba(0, 0, 0, 0.26); }
  .terminal-panel { display: grid; grid-template-rows: 58px minmax(0, 1fr) 34px; min-width: 0; min-height: 0; }
  .panel-heading, .run-heading { display: flex; align-items: center; justify-content: space-between; gap: 14px; padding: 12px 17px; border-bottom: 1px solid #27342f; }
  .panel-heading > div, .run-heading > div { display: grid; gap: 4px; min-width: 0; }
  .value, .run-heading strong { overflow: hidden; color: #cbd4cf; font-size: 0.78rem; font-weight: 480; text-overflow: ellipsis; white-space: nowrap; }
  .transport { color: #526159; font-family: var(--font-mono); }
  .terminal-frame { position: relative; min-width: 0; min-height: 590px; overflow: hidden; contain: layout paint; }
  .terminal-host { position: absolute; inset: 14px; overflow: hidden; border-radius: 4px; outline: none; background: #101614; }
  :global(.terminal-host canvas) { display: block; }
  .screen-reader-output { position: absolute; width: 1px; height: 1px; overflow: hidden; contain: strict; clip: rect(0 0 0 0); clip-path: inset(50%); white-space: pre; }
  .terminal-footer { display: flex; align-items: center; gap: 18px; padding: 0 17px; border-top: 1px solid #202c27; color: #58665f; font-family: var(--font-mono); font-size: 0.63rem; }
  .terminal-footer span:last-child { margin-left: auto; }
  .run-panel { display: grid; grid-template-rows: 44px 58px minmax(0, 1fr) auto; min-width: 0; min-height: 0; border-left: 1px solid #27342f; background: #0d1311; }
  .tabs { display: flex; gap: 2px; padding: 5px 7px 0; border-bottom: 1px solid #27342f; }
  .tabs button { position: relative; padding: 0 10px; color: #6f7d76; font-size: 0.7rem; }
  .tabs button.active { color: #d2dad6; }
  .tabs button.active::after { position: absolute; right: 8px; bottom: -1px; left: 8px; height: 2px; background: #91b976; content: ''; }
  .run-status { padding: 4px 8px; border: 1px solid #34443c; border-radius: 999px; color: #a9b6af; font-family: var(--font-mono); font-size: 0.63rem; }
  .tab-content { min-height: 0; overflow: auto; contain: layout paint; }
  .assembly { padding: 16px 17px 14px; border-bottom: 1px solid #27342f; }
  .question { padding: 12px 13px; border: 1px solid #293832; border-radius: 7px; background: #111916; }
  .question p { margin: 6px 0 0; color: #c5d0ca; font-size: 0.76rem; line-height: 1.48; }
  .assembly-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1px; margin: 13px 0 0; overflow: hidden; border: 1px solid #24312c; border-radius: 7px; background: #24312c; }
  .assembly-grid > div { display: grid; gap: 3px; min-width: 0; padding: 10px 11px; background: #0e1512; }
  .assembly-grid dt { color: #68776f; font-size: 0.57rem; font-weight: 680; letter-spacing: 0.1em; text-transform: uppercase; }
  .assembly-grid dd { overflow: hidden; margin: 0; color: #b9c5bf; font-family: var(--font-mono); font-size: 0.67rem; text-overflow: ellipsis; white-space: nowrap; }
  .assembly-grid small { overflow: hidden; color: #56655d; font-size: 0.59rem; text-overflow: ellipsis; white-space: nowrap; }
  .capabilities { margin-top: 13px; }
  .capabilities ul { display: flex; flex-wrap: wrap; gap: 6px; margin: 8px 0 0; padding: 0; list-style: none; }
  .capabilities li { display: flex; align-items: center; gap: 12px; min-width: 180px; padding: 7px 9px; border: 1px solid #26352f; border-radius: 6px; background: #0d1411; }
  .capabilities li > span { display: grid; gap: 1px; }
  .capabilities strong { color: #aebbb4; font-family: var(--font-mono); font-size: 0.66rem; font-weight: 540; }
  .capabilities small, .capabilities em { color: #5f6f66; font-family: var(--font-mono); font-size: 0.56rem; font-style: normal; }
  .capabilities em { margin-left: auto; color: #78966c; }
  .capabilities .waiting { color: #617068; font-size: 0.65rem; }
  .activity-heading { display: flex; justify-content: space-between; padding: 11px 17px 0; color: #596760; font-size: 0.6rem; }
  .run-events { margin: 0; padding: 8px 17px 20px; list-style: none; }
  .run-events li { display: grid; grid-template-columns: 27px minmax(0, 1fr); gap: 7px; padding: 11px 0; border-bottom: 1px solid #1d2924; content-visibility: auto; contain-intrinsic-block-size: 72px; }
  .run-events .sequence { color: #536159; font-family: var(--font-mono); font-size: 0.65rem; }
  .run-events strong { color: #b9c5bf; font-family: var(--font-mono); font-size: 0.69rem; font-weight: 510; }
  pre { margin: 7px 0 0; overflow: auto; color: #82928a; font-family: var(--font-mono); font-size: 0.64rem; line-height: 1.55; white-space: pre-wrap; word-break: break-word; }
  .run-events .empty { display: block; padding: 28px 0; color: #5f6b65; font-size: 0.75rem; }
  .artifact { padding: 18px; }
  .artifact > pre { min-height: 280px; margin-top: 12px; padding: 14px; border: 1px solid #202d27; border-radius: 6px; color: #aebbb4; background: #0a0f0d; }
  .empty-state { max-width: 370px; padding: 34px 20px; color: #7c8a83; }
  .empty-state strong { color: #bbc5c0; font-size: 0.82rem; }
  .empty-state p { font-size: 0.73rem; line-height: 1.55; }
  .history { max-height: 190px; border-top: 1px solid #27342f; }
  .history-title { display: flex; justify-content: space-between; padding: 10px 16px 6px; color: #596760; font-size: 0.65rem; }
  .history-list { max-height: 150px; overflow: auto; padding: 0 7px 7px; }
  .history-list button { display: grid; grid-template-columns: 7px minmax(0, 1fr) auto; align-items: center; gap: 9px; width: 100%; padding: 8px 9px; border-radius: 5px; text-align: left; }
  .history-list button:hover, .history-list button.selected { background: #141d19; }
  .history-list button > span:nth-child(2) { display: grid; gap: 2px; min-width: 0; }
  .history-list strong { overflow: hidden; color: #aeb9b3; font-size: 0.68rem; font-weight: 520; text-overflow: ellipsis; white-space: nowrap; }
  .history-list small, .history-list em { overflow: hidden; color: #5e6c65; font-family: var(--font-mono); font-size: 0.58rem; font-style: normal; text-overflow: ellipsis; white-space: nowrap; }
  .history-list p { margin: 8px 9px; color: #56635c; font-size: 0.68rem; }
  @media (max-width: 1050px) {
    header { grid-template-columns: 1fr auto; }
    .run-controls { grid-row: 2; grid-column: 1 / -1; justify-content: flex-start; }
    .bench { grid-template-columns: 1fr; height: auto; min-height: 720px; }
    .run-panel { min-height: 620px; border-top: 1px solid #27342f; border-left: 0; }
  }
  @media (max-width: 620px) {
    main { width: calc(100% - 20px); padding-top: 14px; }
    .run-controls { display: grid; grid-template-columns: 1fr 1fr; }
    .run-controls label { min-width: 0; }
    input, select { width: 100%; min-width: 0; }
    .bench { min-height: 600px; }
    .terminal-frame { min-height: 460px; }
  }
</style>
