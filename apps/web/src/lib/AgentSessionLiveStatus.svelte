<script lang="ts">
  import { onMount } from 'svelte';
  import type { AgentSessionLiveStatusModel } from './agent-live-status';

  export let status: AgentSessionLiveStatusModel;
  export let cancelling = false;
  export let onCancel: () => void;

  let nowMs = Date.now();

  const phaseLabels: Record<AgentSessionLiveStatusModel['phase'], string> = {
    starting: 'Starting',
    preparing: 'Preparing',
    reasoning: 'Reasoning',
    responding: 'Responding',
    acting: 'Acting',
    waiting: 'Waiting',
    finalizing: 'Finalizing',
    running: 'Running',
    cancelling: 'Cancelling'
  };

  $: elapsedMs = Math.max(0, nowMs - status.startedAtMs);
  $: elapsed = formatElapsed(elapsedMs);
  $: elapsedLabel = `Elapsed ${elapsed}`;

  onMount(() => {
    nowMs = Date.now();
    const timer = setInterval(() => {
      nowMs = Date.now();
    }, 1_000);
    return () => clearInterval(timer);
  });

  function formatElapsed(milliseconds: number): string {
    if (milliseconds < 10_000) return `${(milliseconds / 1_000).toFixed(1)}s`;
    if (milliseconds < 60_000) return `${Math.floor(milliseconds / 1_000)}s`;
    const minutes = Math.floor(milliseconds / 60_000);
    const seconds = Math.floor((milliseconds % 60_000) / 1_000);
    return `${minutes}m ${String(seconds).padStart(2, '0')}s`;
  }
</script>

<section
  class="agent-live-status"
  data-testid="agent-live-status"
  data-phase={status.phase}
  data-source-event-sequence={status.sourceEventSequence ?? undefined}
  data-source-event-type={status.sourceEventType ?? undefined}
  aria-label="Live agent status"
>
  <div class="live-state">
    <span class="live-dot" aria-hidden="true"></span>
    <div class="live-copy" aria-live="polite" aria-atomic="true">
      <strong>{phaseLabels[status.phase]}</strong>
      {#if status.detail}<p>{status.detail}</p>{/if}
    </div>
  </div>
  {#if status.source}
    <div class="live-source">
      <span>Source</span>
      <strong>{status.source}</strong>
    </div>
  {/if}
  <time aria-label={elapsedLabel}>{elapsed}</time>
  {#if status.cancellable}
    <button type="button" disabled={cancelling} on:click={onCancel}>
      {cancelling ? 'Cancelling…' : 'Cancel turn'}
    </button>
  {/if}
</section>

<style>
  .agent-live-status {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto auto auto;
    align-items: center;
    gap: 10px;
    min-width: 0;
    padding: 9px 17px;
    border-bottom: 1px solid #27342f;
    background: #0d1512;
  }
  .live-state {
    display: grid;
    grid-template-columns: 8px minmax(0, 1fr);
    align-items: center;
    gap: 8px;
    min-width: 0;
  }
  .live-dot {
    width: 7px;
    height: 7px;
    border: 1px solid #81a873;
    border-radius: 50%;
    background: #31472b;
    box-shadow: 0 0 0 3px rgb(82 117 70 / 12%);
  }
  .live-copy {
    min-width: 0;
  }
  .live-copy strong {
    display: block;
    color: #c7d1cc;
    font-size: 0.68rem;
    font-weight: 570;
  }
  .live-copy p {
    overflow: hidden;
    margin: 2px 0 0;
    color: #829189;
    font-size: 0.59rem;
    line-height: 1.35;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .live-source {
    display: grid;
    gap: 1px;
    min-width: 0;
    padding-left: 10px;
    border-left: 1px solid #27342f;
  }
  .live-source span {
    color: #5f6e66;
    font-size: 0.48rem;
    font-weight: 680;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }
  .live-source strong, time {
    overflow: hidden;
    color: #91a29a;
    font-family: var(--font-mono);
    font-size: 0.56rem;
    font-weight: 500;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  time {
    color: #708078;
  }
  button {
    height: 27px;
    border: 1px solid #50373a;
    border-radius: 5px;
    padding: 0 8px;
    color: #d78b90;
    background: #171211;
    font: inherit;
    font-size: 0.59rem;
    white-space: nowrap;
    cursor: pointer;
  }
  button:disabled {
    cursor: wait;
    opacity: 0.62;
  }
  button:focus-visible {
    outline: 2px solid #789d6b;
    outline-offset: 2px;
  }
  @media (max-width: 520px) {
    .agent-live-status {
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px 10px;
      padding-inline: 12px;
    }
    .live-source {
      grid-column: 1;
      grid-row: 2;
      padding-left: 15px;
      border-left: 0;
    }
    time {
      grid-column: 2;
      grid-row: 1;
    }
    button {
      grid-column: 2;
      grid-row: 2;
      justify-self: end;
    }
  }
</style>
