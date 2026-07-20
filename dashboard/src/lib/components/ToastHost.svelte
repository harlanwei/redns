<script lang="ts">
  import { fly, fade } from 'svelte/transition';
  import { toasts, dismissToast } from '../utils/toast.svelte';

  /** Renders the global toast stack in the bottom-right corner. */
</script>

<div
  class="pointer-events-none fixed bottom-4 right-4 z-[60] flex w-[calc(100vw-2rem)] max-w-sm flex-col gap-2"
  aria-live="polite"
  aria-atomic="false"
>
  {#each toasts as toast (toast.id)}
    <div
      class="toast toast-{toast.kind} pointer-events-auto"
      role="status"
      in:fly={{ x: 48, duration: 220 }}
      out:fade={{ duration: 140 }}
    >
      <span class="toast-icon" aria-hidden="true">
        {#if toast.kind === 'success'}
          <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
        {:else if toast.kind === 'danger'}
          <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M12 9v2m0 4h.01M5.07 19h13.86c1.54 0 2.5-1.67 1.73-3L13.73 4c-.77-1.33-2.69-1.33-3.46 0L3.34 16c-.77 1.33.19 3 1.73 3z" /></svg>
        {:else}
          <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
        {/if}
      </span>
      <p class="min-w-0 flex-1 text-sm font-medium">{toast.message}</p>
      {#if toast.actionLabel && toast.onAction}
        <button
          class="toast-action"
          onclick={() => {
            toast.onAction?.();
            dismissToast(toast.id);
          }}
        >
          {toast.actionLabel}
        </button>
      {/if}
      <button class="toast-close" aria-label="Dismiss" onclick={() => dismissToast(toast.id)}>
        <svg class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12" /></svg>
      </button>
    </div>
  {/each}
</div>
