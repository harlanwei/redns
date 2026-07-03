<script lang="ts">
  import { slide } from 'svelte/transition';
  import type { TabId } from '../types/dashboard';
  import { theme, toggleTheme } from '../utils/theme.svelte';

  const tabs: { id: TabId; label: string }[] = [
    { id: 'logs', label: 'Query stream' },
    { id: 'clients', label: 'Clients' },
    { id: 'cache', label: 'Cache' },
    { id: 'upstreams', label: 'Upstreams' },
  ];

  let { activeTab, onTabChange } = $props<{
    activeTab: TabId;
    onTabChange: (tab: TabId) => void;
  }>();

  let mobileMenuOpen = $state(false);

  function selectTab(tab: TabId) {
    onTabChange(tab);
    mobileMenuOpen = false;
  }
</script>

<aside class="hidden lg:sticky lg:top-0 lg:flex lg:h-dvh lg:flex-col lg:border-r lg:border-white/10 lg:bg-header lg:text-header-text">
  <div class="flex h-full flex-col p-4">
    <div class="flex items-center gap-3 px-2 py-2">
      <div class="relative flex h-10 w-10 items-center justify-center rounded-md bg-accent-fill font-extrabold text-white ring-1 ring-white/20">
        R
        <span class="absolute -bottom-0.5 -right-0.5 h-2.5 w-2.5 rounded-full bg-success-2 ring-2 ring-header" aria-hidden="true"></span>
      </div>
      <div class="leading-tight">
        <h1 class="text-xl font-bold tracking-tight">ReDNS</h1>
        <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-header-muted -mt-0.5">Resolver Console</p>
      </div>
    </div>

    <nav class="mt-8 space-y-1" aria-label="Dashboard sections">
      {#each tabs as tab}
        <button
          onclick={() => selectTab(tab.id)}
          aria-current={activeTab === tab.id ? 'page' : undefined}
          class="group block w-full rounded-md px-3 py-2.5 text-left transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-white/50 {activeTab === tab.id ? 'bg-accent-fill text-white' : 'text-header-muted hover:bg-white/10 hover:text-white'}"
        >
          <span class="block text-sm font-semibold">{tab.label}</span>
        </button>
      {/each}
    </nav>

    <div class="mt-auto space-y-3">
      <div class="border-t border-white/10 pt-4">
        <div class="flex items-center justify-between gap-3">
          <span class="text-xs font-semibold uppercase tracking-[0.08em] text-header-muted">Resolver</span>
          <span class="inline-flex items-center gap-1.5 rounded-md bg-success-bg px-2 py-1 text-xs font-semibold text-success-text">
            <span class="h-1.5 w-1.5 rounded-full bg-success-2" aria-hidden="true"></span>
            Ready
          </span>
        </div>
        <div class="mt-3 grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
          <div class="text-header-muted">Retention</div>
          <div class="text-right font-semibold text-white">24h</div>
          <div class="text-header-muted">Theme</div>
          <div class="text-right font-semibold capitalize text-white">{theme.value}</div>
        </div>
      </div>

      <button
        onclick={toggleTheme}
        class="flex w-full items-center justify-between rounded-md border border-white/10 bg-white/5 px-4 py-3 text-sm font-semibold text-header-muted transition-colors hover:bg-white/10 hover:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-white/50"
      >
        <span>Appearance</span>
        <span class="capitalize text-white">{theme.value}</span>
      </button>
    </div>
  </div>
</aside>

<header class="sticky top-0 z-20 border-b border-white/10 bg-header text-header-text shadow-soft lg:hidden">
  <div class="px-4 sm:px-6">
    <div class="flex min-h-16 items-center justify-between gap-4 py-2">
      <div class="flex items-center gap-3">
        <div class="relative flex h-9 w-9 items-center justify-center rounded-md bg-accent-fill font-extrabold text-white ring-1 ring-white/20">
          R
          <span class="absolute -bottom-0.5 -right-0.5 h-2.5 w-2.5 rounded-full bg-success-2 ring-2 ring-header" aria-hidden="true"></span>
        </div>
        <div class="leading-tight">
          <h1 class="text-xl font-bold tracking-tight">ReDNS</h1>
          <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-header-muted -mt-0.5">Resolver Console</p>
        </div>
      </div>

      <div class="flex items-center gap-1.5">
        <button
          onclick={toggleTheme}
          aria-label="Toggle theme"
          title={theme.value === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
          class="rounded-md p-2 text-header-muted transition-colors hover:bg-white/10 hover:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-white/50"
        >
          {#if theme.value === 'dark'}
            <svg class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
              <path stroke-linecap="round" stroke-linejoin="round" d="M12 3v1.5m0 15V21m9-9h-1.5M6 12H4.5m15.364-6.864l-1.06 1.06M5.696 18.304l-1.06 1.06m12.728 0l-1.06-1.06M6.757 6.757L5.696 5.696M16 12a4 4 0 11-8 0 4 4 0 018 0z" />
            </svg>
          {:else}
            <svg class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
              <path stroke-linecap="round" stroke-linejoin="round" d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
            </svg>
          {/if}
        </button>

        <button
          onclick={() => (mobileMenuOpen = !mobileMenuOpen)}
          class="rounded-md p-2 text-header-muted transition-colors hover:bg-white/10 hover:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-white/50"
          aria-label="Menu"
          aria-expanded={mobileMenuOpen}
        >
          <svg class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            {#if mobileMenuOpen}
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
            {:else}
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16" />
            {/if}
          </svg>
        </button>
      </div>
    </div>
  </div>

  {#if mobileMenuOpen}
    <div class="border-t border-white/10 bg-header-2" transition:slide>
      <nav class="space-y-1 px-3 py-4" aria-label="Dashboard sections">
        {#each tabs as tab}
          <button
            onclick={() => selectTab(tab.id)}
            aria-current={activeTab === tab.id ? 'page' : undefined}
            class="block w-full rounded-md px-3 py-2.5 text-left transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-white/50 {activeTab === tab.id ? 'bg-accent-fill text-white' : 'text-header-muted hover:bg-white/10 hover:text-white'}"
          >
            <span class="block text-sm font-semibold">{tab.label}</span>
          </button>
        {/each}
      </nav>
    </div>
  {/if}
</header>
