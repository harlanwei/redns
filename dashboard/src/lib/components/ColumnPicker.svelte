<script lang="ts">
  import { fly } from 'svelte/transition';
  import type { LogColumnId } from '../types/dashboard';

  export type ColumnDef = { id: LogColumnId; label: string; locked?: boolean };

  /**
   * Dropdown for toggling log-table column visibility. The `query` column is
   * locked so the table always has a primary column.
   */
  let { columns, visible, onToggle } = $props<{
    columns: ColumnDef[];
    visible: LogColumnId[];
    onToggle: (id: LogColumnId) => void;
  }>();

  let open = $state(false);
  let rootEl = $state<HTMLDivElement | null>(null);

  function isVisible(id: LogColumnId): boolean {
    return visible.includes(id);
  }

  function handleOutside(e: MouseEvent) {
    if (rootEl && !rootEl.contains(e.target as Node)) open = false;
  }

  $effect(() => {
    if (open) {
      document.addEventListener('click', handleOutside, true);
      return () => document.removeEventListener('click', handleOutside, true);
    }
  });
</script>

<div class="relative" bind:this={rootEl}>
  <button
    type="button"
    class="btn btn-secondary"
    onclick={() => (open = !open)}
    aria-expanded={open}
    aria-haspopup="true"
  >
    <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.75" aria-hidden="true">
      <path stroke-linecap="round" stroke-linejoin="round" d="M9 4.5v15m6-15v15M4.5 9h15M4.5 15h15" />
    </svg>
    Columns
    <svg class="h-3.5 w-3.5 text-faint transition-transform {open ? 'rotate-180' : ''}" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" aria-hidden="true">
      <path stroke-linecap="round" stroke-linejoin="round" d="M19 9l-7 7-7-7" />
    </svg>
  </button>

  {#if open}
    <div
      class="absolute right-0 top-full z-20 mt-1.5 w-44 overflow-hidden rounded-lg border border-line bg-surface py-1 shadow-lift"
      transition:fly={{ y: -4, duration: 150 }}
      role="menu"
      aria-label="Toggle columns"
    >
      {#each columns as col (col.id)}
        <button
          type="button"
          role="menuitemcheckbox"
          aria-checked={isVisible(col.id)}
          disabled={col.locked}
          class="flex w-full items-center gap-2.5 px-3 py-2 text-left text-sm text-ink hover:bg-panel disabled:cursor-not-allowed disabled:opacity-50"
          onclick={() => onToggle(col.id)}
        >
          <span
            class="flex h-4 w-4 shrink-0 items-center justify-center rounded border {isVisible(col.id)
              ? 'border-accent bg-accent-fill text-on-accent'
              : 'border-line-2 bg-surface text-transparent'}"
            aria-hidden="true"
          >
            <svg class="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="3"><path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7" /></svg>
          </span>
          {col.label}
          {#if col.locked}<span class="ml-auto text-[10px] text-faint">fixed</span>{/if}
        </button>
      {/each}
    </div>
  {/if}
</div>
