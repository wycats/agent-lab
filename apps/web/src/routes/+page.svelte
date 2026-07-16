<script lang="ts">
  import { onMount } from 'svelte';
  import { createGhosttySurface } from '$lib/terminal/ghostty';
  import { connectSession } from '$lib/terminal/session';
  import type { BrowserSession, ConnectionState, SessionEvent } from '$lib/terminal/session';
  import type { TerminalSurface } from '$lib/terminal/surface';

  let terminalHost: HTMLDivElement;
  let surface: TerminalSurface | undefined;
  let session: BrowserSession | undefined;
  let state: ConnectionState = 'starting';
  let events: SessionEvent[] = [];
  let screenText = '';
  let startupError = '';

  async function start(): Promise<void> {
    startupError = '';
    state = 'starting';
    events = [];
    session?.dispose();
    try {
      surface ??= await createGhosttySurface(terminalHost);
      session = await connectSession(surface, {
        onState(next) {
          state = next;
        },
        onEvent(event) {
          events = [...events, event];
        },
        onScreen(text) {
          screenText = text;
        }
      });
      surface.focus();
    } catch (error) {
      state = 'error';
      startupError = error instanceof Error ? error.message : String(error);
    }
  }

  onMount(() => {
    void start();
    return () => {
      session?.dispose();
      surface?.dispose();
    };
  });
</script>

<svelte:head>
  <title>Agent Lab — terminal workbench</title>
  <meta
    name="description"
    content="An interactive browser workbench for Agent Lab's Nushell and MCP session"
  />
</svelte:head>

<main>
  <header>
    <div>
      <p class="eyebrow">Agent Lab</p>
      <h1>Terminal workbench</h1>
      <p class="lede">Explore a live Nushell session with MCP tools.</p>
    </div>
    <div class="connection" data-state={state} aria-live="polite">
      <span class="status-dot"></span>
      <span>{state}</span>
    </div>
  </header>

  <section class="bench" aria-label="Agent Lab browser bench">
    <article class="terminal-panel">
      <div class="panel-heading">
        <div>
          <span class="label">Interactive session</span>
          <span class="value">Nushell + MCP fixture</span>
        </div>
        <span class="transport">PTY · WebSocket · Ghostty</span>
      </div>
      <div class="terminal-frame">
        <div class="terminal-host" bind:this={terminalHost} data-testid="terminal"></div>
        <pre class="screen-reader-output" data-testid="terminal-text" aria-live="polite">{screenText}</pre>
      </div>
    </article>

    <aside class="evidence-panel">
      <div class="panel-heading">
        <div>
          <span class="label">Session details</span>
          <span class="value">Live connection</span>
        </div>
      </div>

      <dl>
        <div>
          <dt>Provider</dt>
          <dd>{events.find((event) => event.type === 'started')?.provider ?? 'waiting'}</dd>
        </div>
        <div>
          <dt>Boundary</dt>
          <dd>real child PTY</dd>
        </div>
        <div>
          <dt>Exposure</dt>
          <dd>loopback only</dd>
        </div>
      </dl>

      <ol class="events" aria-label="Session events">
        {#each events as event, index}
          <li>
            <span>{String(index + 1).padStart(2, '0')}</span>
            <strong>{event.type}</strong>
            {#if event.type === 'started'}
              <small>{event.provider} · {event.cols}×{event.rows}</small>
            {:else if event.type === 'resized'}
              <small>{event.cols}×{event.rows}</small>
            {:else if event.type === 'error'}
              <small>{event.message}</small>
            {/if}
          </li>
        {:else}
          <li class="empty">Waiting for the fixture session…</li>
        {/each}
      </ol>

      {#if startupError}
        <p class="error" role="alert">{startupError}</p>
      {/if}

      {#if state === 'closed' || state === 'error'}
        <button type="button" on:click={() => void start()}>Start a new fixture session</button>
      {/if}

      <p class="note">
        Connection and terminal events appear here as the session changes.
      </p>
    </aside>
  </section>
</main>

<style>
  :global(*) {
    box-sizing: border-box;
  }

  :global(html) {
    color-scheme: dark;
    font-family:
      Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    background: #0b100e;
  }

  :global(body) {
    margin: 0;
    min-width: 320px;
    min-height: 100vh;
    color: #d8e0db;
    background:
      radial-gradient(circle at 18% 10%, rgba(54, 102, 84, 0.2), transparent 32rem),
      #0b100e;
  }

  main {
    width: min(1500px, calc(100% - 48px));
    margin: 0 auto;
    padding: 42px 0 48px;
  }

  header {
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    gap: 24px;
    margin-bottom: 28px;
  }

  .eyebrow,
  .label,
  .transport,
  dt {
    color: #7f9188;
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.14em;
    text-transform: uppercase;
  }

  .eyebrow {
    margin: 0 0 8px;
  }

  h1 {
    margin: 0;
    color: #f1f5f2;
    font-family: Georgia, "Times New Roman", serif;
    font-size: clamp(2.4rem, 6vw, 4.8rem);
    font-weight: 400;
    letter-spacing: -0.055em;
    line-height: 0.95;
  }

  .lede {
    margin: 14px 0 0;
    color: #9caaa3;
    font-size: 0.95rem;
  }

  .connection {
    display: flex;
    align-items: center;
    gap: 9px;
    min-width: 108px;
    padding: 9px 13px;
    border: 1px solid #27342f;
    border-radius: 999px;
    color: #aebbb4;
    font-family: monospace;
    font-size: 0.78rem;
  }

  .status-dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: #e6b450;
    box-shadow: 0 0 10px currentColor;
  }

  .connection[data-state="connected"] .status-dot {
    background: #8fb573;
  }

  .connection[data-state="error"] .status-dot,
  .connection[data-state="closed"] .status-dot {
    background: #e06c75;
  }

  .bench {
    display: grid;
    grid-template-columns: minmax(0, 1fr) 280px;
    overflow: hidden;
    min-height: 660px;
    border: 1px solid #27342f;
    border-radius: 14px;
    background: #111715;
    box-shadow: 0 30px 80px rgba(0, 0, 0, 0.28);
  }

  .terminal-panel,
  .evidence-panel {
    min-width: 0;
  }

  .evidence-panel {
    display: flex;
    flex-direction: column;
    padding-bottom: 20px;
    border-left: 1px solid #27342f;
    background: #0e1412;
  }

  .panel-heading {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    min-height: 68px;
    padding: 14px 20px;
    border-bottom: 1px solid #27342f;
  }

  .panel-heading div {
    display: grid;
    gap: 5px;
  }

  .value {
    color: #d8e0db;
    font-size: 0.86rem;
  }

  .transport {
    color: #596a62;
    font-family: monospace;
    letter-spacing: 0.08em;
  }

  .terminal-frame {
    position: relative;
    height: 590px;
    padding: 18px;
  }

  .terminal-host {
    width: 100%;
    height: 100%;
    overflow: hidden;
    border-radius: 5px;
    outline: none;
    background: #111715;
  }

  :global(.terminal-host canvas) {
    display: block;
  }

  .screen-reader-output {
    position: absolute;
    width: 1px;
    height: 1px;
    overflow: hidden;
    clip: rect(0 0 0 0);
    clip-path: inset(50%);
    white-space: pre;
  }

  dl {
    display: grid;
    gap: 16px;
    margin: 0;
    padding: 20px;
    border-bottom: 1px solid #27342f;
  }

  dl div {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 12px;
  }

  dt {
    color: #607069;
  }

  dd {
    margin: 0;
    color: #aebbb4;
    font-family: monospace;
    font-size: 0.76rem;
  }

  .events {
    display: grid;
    gap: 1px;
    margin: 0;
    padding: 16px 20px;
    list-style: none;
  }

  .events li {
    display: grid;
    grid-template-columns: 24px 1fr auto;
    gap: 8px;
    padding: 9px 0;
    color: #617168;
    font-family: monospace;
    font-size: 0.72rem;
  }

  .events strong {
    color: #b9c4be;
    font-weight: 500;
  }

  .events small {
    color: #8fb573;
  }

  .events .empty {
    display: block;
    color: #59645e;
  }

  .note,
  .error {
    margin: auto 20px 0;
    color: #66756e;
    font-size: 0.75rem;
    line-height: 1.55;
  }

  .error {
    margin-bottom: 14px;
    color: #e06c75;
  }

  button {
    margin: 0 20px 16px;
    padding: 9px 12px;
    border: 1px solid #345e52;
    border-radius: 5px;
    color: #cbd6d0;
    background: #183027;
    cursor: pointer;
  }

  @media (max-width: 900px) {
    main {
      width: min(100% - 24px, 720px);
      padding-top: 24px;
    }

    header {
      align-items: flex-start;
      flex-direction: column;
    }

    .bench {
      grid-template-columns: 1fr;
    }

    .evidence-panel {
      min-height: 360px;
      border-top: 1px solid #27342f;
      border-left: 0;
    }

    .transport {
      display: none;
    }
  }
</style>
