<script lang="ts">
  import type { Snippet } from 'svelte';

  /**
   * A crafted empty state: an illustrated badge with a soft animated ring,
   * a title, supporting copy, and an optional action.
   */
  let {
    kind = 'inbox',
    title,
    description,
    action,
  } = $props<{
    kind?: 'search' | 'clients' | 'server' | 'database' | 'clock' | 'inbox';
    title: string;
    description?: string;
    action?: Snippet;
  }>();
</script>

<div class="empty-state-rich">
  <div class="empty-badge">
    <span class="empty-ring" aria-hidden="true"></span>
    <span class="empty-ring empty-ring-2" aria-hidden="true"></span>
    {#if kind === 'search'}
      <svg class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
        <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-4.35-4.35M11 18a7 7 0 100-14 7 7 0 000 14z" />
      </svg>
    {:else if kind === 'clients'}
      <svg class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
        <path stroke-linecap="round" stroke-linejoin="round" d="M17 20h5v-2a4 4 0 00-3-3.87M9 20H4v-2a4 4 0 013-3.87m6-1.13a4 4 0 100-8 4 4 0 000 8z" />
      </svg>
    {:else if kind === 'server'}
      <svg class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
        <path stroke-linecap="round" stroke-linejoin="round" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-9-4h.01M8 16h.01" />
      </svg>
    {:else if kind === 'database'}
      <svg class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
        <path stroke-linecap="round" stroke-linejoin="round" d="M4 7c0-1.1 3.58-2 8-2s8 .9 8 2-3.58 2-8 2-8-.9-8-2zm0 0v10c0 1.1 3.58 2 8 2s8-.9 8-2V7M4 12c0 1.1 3.58 2 8 2s8-.9 8-2" />
      </svg>
    {:else if kind === 'clock'}
      <svg class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
        <path stroke-linecap="round" stroke-linejoin="round" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
    {:else}
      <svg class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
        <path stroke-linecap="round" stroke-linejoin="round" d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-5.586a1 1 0 00-.707.293l-1.414 1.414a1 1 0 01-.707.293h-1.172a1 1 0 01-.707-.293l-1.414-1.414A1 1 0 008.586 13H4" />
      </svg>
    {/if}
  </div>

  <div class="text-sm font-semibold text-ink">{title}</div>
  {#if description}
    <p class="max-w-xs text-center text-xs leading-relaxed text-muted">{description}</p>
  {/if}
  {#if action}
    <div class="mt-1">{@render action()}</div>
  {/if}
</div>
