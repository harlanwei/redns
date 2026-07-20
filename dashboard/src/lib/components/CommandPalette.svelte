<script lang="ts">
  import { tick } from 'svelte';
  import { fade, fly } from 'svelte/transition';
  import { portal } from '../utils/portal';

  export type PaletteCommand = {
    id: string;
    label: string;
    section?: string;
    hint?: string;
    run: () => void;
  };

  let { open, onClose, commands } = $props<{
    open: boolean;
    onClose: () => void;
    commands: PaletteCommand[];
  }>();

  let query = $state('');
  let activeIndex = $state(0);
  let inputEl = $state<HTMLInputElement | null>(null);
  let listEl = $state<HTMLDivElement | null>(null);

  let filtered = $derived.by(() => {
    const q = query.trim().toLowerCase();
    if (!q) return commands;
    return commands
      .map((cmd) => {
        const label = cmd.label.toLowerCase();
        const section = (cmd.section ?? '').toLowerCase();
        const idx = label.indexOf(q);
        const sectionHit = section.includes(q);
        return { cmd, score: idx === 0 ? 3 : idx > 0 ? 2 : sectionHit ? 1 : 0 };
      })
      .filter((entry) => entry.score > 0)
      .sort((a, b) => b.score - a.score)
      .map((entry) => entry.cmd);
  });

  $effect(() => {
    if (open) {
      query = '';
      activeIndex = 0;
      tick().then(() => inputEl?.focus());
    }
  });

  $effect(() => {
    activeIndex;
    const el = listEl?.children[activeIndex] as HTMLElement | undefined;
    el?.scrollIntoView({ block: 'nearest' });
  });

  function runCommand(cmd: PaletteCommand | undefined) {
    if (!cmd) return;
    onClose();
    cmd.run();
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      activeIndex = filtered.length ? (activeIndex + 1) % filtered.length : 0;
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      activeIndex = filtered.length ? (activeIndex - 1 + filtered.length) % filtered.length : 0;
    } else if (e.key === 'Enter') {
      e.preventDefault();
      runCommand(filtered[activeIndex]);
    } else if (e.key === 'Escape') {
      e.preventDefault();
      onClose();
    }
  }
</script>

{#if open}
  <div class="fixed inset-0 z-50 flex items-start justify-center px-4 pt-[14vh]" use:portal>
    <div
      class="absolute inset-0 bg-black/45 dark:bg-black/70 backdrop-blur-[2px]"
      aria-hidden="true"
      onclick={onClose}
      transition:fade={{ duration: 140 }}
    ></div>

    <div
      class="palette-panel relative z-10 w-full max-w-lg"
      role="dialog"
      aria-modal="true"
      aria-label="Command palette"
      transition:fly={{ y: -10, duration: 190 }}
    >
      <div class="flex items-center gap-2.5 border-b border-line px-4">
        <svg class="h-4 w-4 shrink-0 text-faint" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.75" aria-hidden="true">
          <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-4.35-4.35M11 18a7 7 0 100-14 7 7 0 000 14z" />
        </svg>
        <input
          bind:this={inputEl}
          bind:value={query}
          onkeydown={handleKeydown}
          placeholder="Jump to a view or run a command…"
          class="h-12 min-w-0 flex-1 bg-transparent text-sm text-ink placeholder:text-faint focus:outline-none"
          aria-label="Search commands"
        />
        <kbd class="kbd">esc</kbd>
      </div>

      <div bind:this={listEl} class="max-h-72 overflow-y-auto p-2" role="listbox" aria-label="Commands">
        {#each filtered as cmd, i (cmd.id)}
          <button
            type="button"
            role="option"
            aria-selected={i === activeIndex}
            class="palette-item"
            class:palette-item-active={i === activeIndex}
            onclick={() => runCommand(cmd)}
            onmouseenter={() => (activeIndex = i)}
          >
            <span class="truncate text-sm font-medium">{cmd.label}</span>
            <span class="ml-auto flex shrink-0 items-center gap-2">
              {#if cmd.section}
                <span class="text-[11px] text-faint">{cmd.section}</span>
              {/if}
              {#if cmd.hint}
                <kbd class="kbd">{cmd.hint}</kbd>
              {/if}
            </span>
          </button>
        {:else}
          <div class="px-3 py-8 text-center text-sm text-faint">No matching commands</div>
        {/each}
      </div>
    </div>
  </div>
{/if}
