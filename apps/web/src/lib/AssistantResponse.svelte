<script lang="ts">
  import AssistantMarkdown from '$lib/AssistantMarkdown.svelte';
  import { splitAssistantText } from '$lib/assistant-text';

  export let source: string;
  export let streaming = false;

  $: parts = splitAssistantText(source);
</script>

<div class="assistant-response" data-testid="assistant-response" data-streaming={streaming}>
  {#each parts as part, index (`${index}:${part.kind}`)}
    {#if part.kind === 'thinking'}
      <details
        class="thinking-block"
        data-complete={part.complete}
        open={streaming || !part.complete}
      >
        <summary>
          <span>Thinking</span>
          {#if streaming || !part.complete}<em>in progress</em>{/if}
        </summary>
        <div class="thinking-content">
          <AssistantMarkdown source={part.text} streaming={streaming || !part.complete} />
        </div>
      </details>
    {:else if part.text}
      <AssistantMarkdown source={part.text} {streaming} />
    {/if}
  {/each}
</div>

<style>
  .assistant-response {
    display: grid;
    min-width: 0;
    gap: 8px;
  }

  .thinking-block {
    min-width: 0;
    border: 1px solid #2b3b33;
    border-radius: 6px;
    background: #0c1410;
  }

  .thinking-block summary {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 7px 9px;
    color: #91a098;
    cursor: pointer;
    font-size: 0.64rem;
    font-weight: 570;
    list-style-position: inside;
  }

  .thinking-block summary::marker {
    color: #65776d;
  }

  .thinking-block summary em {
    color: #72837a;
    font-family: var(--font-mono);
    font-size: 0.56rem;
    font-style: normal;
    font-weight: 450;
  }

  .thinking-block[open] summary {
    border-bottom: 1px solid #24322b;
  }

  .thinking-content {
    min-width: 0;
    padding: 8px 10px 9px;
    color: #a7b2ac;
    opacity: 0.9;
  }

  .thinking-content :global(.assistant-markdown) {
    color: inherit;
    font-size: 0.72rem;
  }

  .thinking-block:focus-within {
    border-color: #486052;
  }

  .thinking-block summary:focus-visible {
    outline: 2px solid #789d6b;
    outline-offset: 2px;
  }
</style>
