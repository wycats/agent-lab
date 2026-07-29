<script lang="ts">
  import SvelteMarkdown, {
    buildUnsupportedHTML,
    defaultRenderers,
    defaultSanitizeUrl,
    type Renderers,
    type SanitizeUrlFn,
    type SvelteMarkdownOptions,
    type Token
  } from '@humanspeak/svelte-markdown';
  import { createAssistantThinkingProjector } from '$lib/assistant-thinking';
  import type { Snippet } from 'svelte';

  export let source: string;
  export let streaming = false;

  type ThinkingSnippetProps = {
    complete: boolean;
    children?: Snippet;
  };

  const thinkingProjector = createAssistantThinkingProjector();
  const extensions = [thinkingProjector.extension];
  $: projectedSource = thinkingProjector.project(source);
  const renderers: Partial<Renderers> = {
    ...defaultRenderers,
    html: buildUnsupportedHTML()
  };
  const options: Partial<SvelteMarkdownOptions> = {
    breaks: false,
    gfm: true,
    headerIds: false,
    walkTokens: disableRawHtml
  };
  const sanitizeAgentUrl: SanitizeUrlFn = (url, context) => {
    if (context.type === 'image') return '';
    const sanitized = defaultSanitizeUrl(url, context);
    return /^tel:/i.test(sanitized) ? '' : sanitized;
  };

  function opensNewWindow(href: string | undefined): boolean {
    return /^(?:https?:)?\/\//i.test(href ?? '');
  }

  function disableRawHtml(token: Token): void {
    visitTokenValue(token);
  }

  function visitTokenValue(value: unknown): void {
    if (Array.isArray(value)) {
      for (const item of value) visitTokenValue(item);
      return;
    }
    if (!value || typeof value !== 'object') return;
    const record = value as Record<string, unknown>;
    if (record.type === 'html') {
      record.type = 'space';
      record.raw = '';
      delete record.attributes;
      delete record.tag;
      delete record.text;
      delete record.tokens;
      return;
    }
    for (const child of Object.values(record)) visitTokenValue(child);
  }
</script>

<div class="assistant-markdown" data-testid="assistant-markdown" data-streaming={streaming}>
  <SvelteMarkdown
    source={projectedSource}
    {streaming}
    {extensions}
    {renderers}
    {options}
    sanitizeUrl={sanitizeAgentUrl}
  >
    {#snippet thinking({ complete, children }: ThinkingSnippetProps)}
      <details
        class="thinking-block"
        data-complete={complete}
        open={streaming || !complete}
      >
        <summary>
          <span>Thinking</span>
          {#if !complete}<em>in progress</em>{/if}
        </summary>
        <div class="thinking-content">
          {@render children?.()}
        </div>
      </details>
    {/snippet}

    {#snippet heading({ depth, children })}
      {#if depth === 1}
        <h3 class="markdown-heading">{@render children?.()}</h3>
      {:else if depth === 2}
        <h4 class="markdown-heading">{@render children?.()}</h4>
      {:else if depth === 3}
        <h5 class="markdown-heading">{@render children?.()}</h5>
      {:else}
        <h6 class="markdown-heading">{@render children?.()}</h6>
      {/if}
    {/snippet}

    {#snippet link({ href, title, children })}
      {#if href}
        <a
          class="markdown-link"
          {href}
          {title}
          target={opensNewWindow(href) ? '_blank' : undefined}
          rel={opensNewWindow(href) ? 'noopener noreferrer' : undefined}
        >{@render children?.()}</a>
      {:else}
        <span class="blocked-link">{@render children?.()}</span>
      {/if}
    {/snippet}

    {#snippet image({ text, title })}
      <span class="image-placeholder" role="note" aria-label={`Image omitted: ${text || title || 'unlabelled image'}`}>
        Image omitted: {text || title || 'unlabelled image'}
      </span>
    {/snippet}

    {#snippet code({ lang, text })}
      <!-- svelte-ignore a11y_no_noninteractive_tabindex (keyboard access for overflow content) -->
      <div
        class="code-scroll"
        role="region"
        aria-label={lang ? `${lang} code from agent response` : 'Code from agent response'}
        tabindex="0"
      ><pre><code data-language={lang || undefined}>{text}</code></pre></div>
    {/snippet}

    {#snippet table({ children })}
      <!-- svelte-ignore a11y_no_noninteractive_tabindex (keyboard access for overflow content) -->
      <div class="table-scroll" role="region" aria-label="Table in agent response" tabindex="0">
        <table>{@render children?.()}</table>
      </div>
    {/snippet}
  </SvelteMarkdown>
</div>

<style>
  .assistant-markdown {
    min-width: 0;
    color: #d9e0dc;
    font-size: 0.78rem;
    line-height: 1.56;
    overflow-wrap: anywhere;
  }

  .assistant-markdown :global(:first-child) { margin-top: 0; }
  .assistant-markdown :global(:last-child) { margin-bottom: 0; }
  .assistant-markdown :global(p) { margin: 0.58em 0; }
  .assistant-markdown :global(.markdown-heading) {
    margin: 1.05em 0 0.48em;
    color: #eef2ef;
    font-size: 0.93rem;
    font-weight: 620;
    line-height: 1.28;
  }
  .assistant-markdown :global(ul),
  .assistant-markdown :global(ol) { margin: 0.58em 0; padding-inline-start: 1.5rem; }
  .assistant-markdown :global(li) { margin: 0.22em 0; padding-inline-start: 0.16rem; }
  .assistant-markdown :global(blockquote) {
    margin: 0.72em 0;
    padding: 0.18em 0 0.18em 0.8rem;
    border-inline-start: 2px solid #52685d;
    color: #aab6b0;
  }
  .assistant-markdown :global(code) {
    border-radius: 3px;
    padding: 0.12em 0.3em;
    color: #bad8aa;
    background: #151f1a;
    font-family: var(--font-mono);
    font-size: 0.9em;
  }
  .assistant-markdown :global(.code-scroll),
  .assistant-markdown :global(.table-scroll) {
    max-width: 100%;
    margin: 0.72em 0;
    overflow: auto;
    border: 1px solid #28372f;
    border-radius: 6px;
    background: #09100d;
    scrollbar-color: #405048 transparent;
  }
  .assistant-markdown :global(.code-scroll:focus-visible),
  .assistant-markdown :global(.table-scroll:focus-visible) { outline: 2px solid #789d6b; outline-offset: 2px; }
  .assistant-markdown :global(.code-scroll pre) { min-width: max-content; margin: 0; padding: 0.7rem 0.8rem; }
  .assistant-markdown :global(.code-scroll code) { padding: 0; color: #b9c7c0; background: transparent; }
  .assistant-markdown :global(.table-scroll table) { min-width: 100%; border-collapse: collapse; }
  .assistant-markdown :global(.table-scroll th),
  .assistant-markdown :global(.table-scroll td) {
    padding: 0.42rem 0.55rem;
    border-bottom: 1px solid #26342e;
    text-align: start;
    white-space: nowrap;
  }
  .assistant-markdown :global(.table-scroll th) { color: #c8d2cd; background: #111916; }
  .assistant-markdown :global(.markdown-link) {
    color: #aad18f;
    text-decoration: underline;
    text-decoration-thickness: 1px;
    text-underline-offset: 0.16em;
  }
  .assistant-markdown :global(.markdown-link:focus-visible) { outline: 2px solid #789d6b; outline-offset: 2px; }
  .assistant-markdown :global(.blocked-link) { color: #aab6b0; text-decoration: line-through; }
  .assistant-markdown :global(.image-placeholder) {
    display: inline-block;
    border: 1px dashed #526159;
    border-radius: 4px;
    padding: 0.14em 0.42em;
    color: #899990;
    font-family: var(--font-mono);
    font-size: 0.86em;
  }
  .assistant-markdown :global(hr) { margin: 0.9em 0; border: 0; border-top: 1px solid #2d3b35; }

  .thinking-block {
    min-width: 0;
    margin: 0.58em 0;
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
  .thinking-block summary::marker { color: #65776d; }
  .thinking-block summary em {
    color: #72837a;
    font-family: var(--font-mono);
    font-size: 0.56rem;
    font-style: normal;
    font-weight: 450;
  }
  .thinking-block[open] summary { border-bottom: 1px solid #24322b; }
  .thinking-content {
    min-width: 0;
    padding: 8px 10px 9px;
    color: #a7b2ac;
    font-size: 0.72rem;
    opacity: 0.9;
  }
  .thinking-block:focus-within { border-color: #486052; }
  .thinking-block summary:focus-visible {
    outline: 2px solid #789d6b;
    outline-offset: 2px;
  }
</style>
