<script lang="ts">
  import AssistantMarkdown from '$lib/AssistantMarkdown.svelte';

  export let source: string;
  export let streaming = false;
</script>

<div class="assistant-response" data-testid="assistant-response" data-streaming={streaming}>
  <AssistantMarkdown {source} {streaming} />
</div>

<style>
  .assistant-response {
    display: grid;
    min-width: 0;
    gap: 8px;
  }
</style>
