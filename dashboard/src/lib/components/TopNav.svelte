<script lang="ts">
  import { onMount } from 'svelte';
  import { slide } from 'svelte/transition';
  import type { TabId } from '../types/dashboard';
  import { theme, toggleTheme } from '../utils/theme.svelte';

  type Tab = { id: TabId; label: string; hint: string; icon: string };

  // Icon paths kept inline to match the project's existing SVG convention.
  const tabs: Tab[] = [
    {
      id: 'logs',
      label: 'Query stream',
      hint: 'Live resolver activity',
      icon: 'M4 6h16M4 12h10M4 18h6',
    },
    {
      id: 'clients',
      label: 'Clients',
      hint: 'Traffic by source',
      icon: 'M17 20h5v-2a4 4 0 00-3-3.87M9 20H4v-2a4 4 0 013-3.87m6-1.13a4 4 0 100-8 4 4 0 000 8z',
    },
    {
      id: 'cache',
      label: 'Cache',
      hint: 'Occupancy & hit rate',
      icon: 'M4 7c0-1.1 3.58-2 8-2s8 .9 8 2-3.58 2-8 2-8-.9-8-2zm0 0v10c0 1.1 3.58 2 8 2s8-.9 8-2V7',
    },
    {
      id: 'upstreams',
      label: 'Upstreams',
      hint: 'Egress & latency',
      icon: 'M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-3 3h.01M17 15h.01',
    },
  ];

  let { activeTab, onTabChange } = $props<{
    activeTab: TabId;
    onTabChange: (tab: TabId) => void;
  }>();

  let mobileMenuOpen = $state(false);
  // e.g. "0.1.0 (3b043ff2)" — package version plus short commit hash. Empty
  // until `/api/version` resolves; the UI degrades gracefully if it fails.
  let buildVersion = $state('');

  onMount(async () => {
    try {
      const res = await fetch('/api/version');
      if (res.ok) {
        const data = await res.json();
        if (typeof data?.version === 'string') buildVersion = data.version.trim();
      }
    } catch {
      // Non-critical metadata; leave buildVersion empty.
    }
  });

  function selectTab(tab: TabId) {
    onTabChange(tab);
    mobileMenuOpen = false;
  }
</script>

<aside class="relative hidden overflow-hidden lg:sticky lg:top-0 lg:flex lg:h-dvh lg:flex-col lg:border-r lg:border-white/10 lg:bg-header lg:text-header-text">
  <!-- Ambient accent glow anchored to the top of the rail. -->
  <div class="pointer-events-none absolute inset-x-0 top-0 h-40 bg-[radial-gradient(24rem_12rem_at_50%_-30%,color-mix(in_srgb,var(--ui-accent)_45%,transparent),transparent)]" aria-hidden="true"></div>

  <div class="relative flex h-full flex-col p-4">
    <div class="flex items-center gap-3 px-2 py-2">
      <div class="relative flex h-11 w-11 items-center justify-center rounded-xl bg-accent-fill text-lg font-extrabold text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.25)] ring-1 ring-white/20">
        R
        <span class="absolute -bottom-0.5 -right-0.5 h-3 w-3 rounded-full bg-success-2 ring-2 ring-header">
          <span class="absolute inset-0 rounded-full bg-success-2 animate-soft-pulse"></span>
        </span>
      </div>
      <div class="leading-tight">
        <h1 class="text-xl font-bold tracking-tight">ReDNS</h1>
        <p class="-mt-0.5 text-[11px] font-semibold uppercase tracking-[0.14em] text-header-muted">Resolver console</p>
      </div>
    </div>

    <nav class="mt-8 space-y-1.5" aria-label="Dashboard sections">
      {#each tabs as tab}
        {@const active = activeTab === tab.id}
        <button
          onclick={() => selectTab(tab.id)}
          aria-current={active ? 'page' : undefined}
          class="group relative flex w-full items-center gap-3 rounded-lg px-3 py-2.5 text-left focus:outline-none focus-visible:ring-2 focus-visible:ring-white/50 {active ? 'bg-white/10 text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.08)]' : 'text-header-muted hover:bg-white/5 hover:text-white'}"
        >
          <span class="absolute inset-y-2 left-0 w-0.5 rounded-full bg-accent-3 transition-all duration-200 {active ? 'opacity-100' : 'opacity-0 group-hover:opacity-40'}" aria-hidden="true"></span>
          <svg class="h-5 w-5 shrink-0 {active ? 'text-accent-3' : 'text-header-muted group-hover:text-white'}" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.75" aria-hidden="true">
            <path stroke-linecap="round" stroke-linejoin="round" d={tab.icon} />
          </svg>
          <span class="min-w-0">
            <span class="block text-sm font-semibold">{tab.label}</span>
            <span class="block truncate text-[11px] font-medium {active ? 'text-header-muted' : 'text-header-muted/70'}">{tab.hint}</span>
          </span>
        </button>
      {/each}
    </nav>

    <div class="mt-auto space-y-3">
      <div class="rounded-xl border border-white/10 bg-white/5 p-3.5 shadow-[inset_0_1px_0_rgba(255,255,255,0.06)]">
        <div class="flex items-center justify-between gap-3">
          <span class="text-[11px] font-semibold uppercase tracking-[0.1em] text-header-muted">Resolver</span>
          <span class="inline-flex items-center gap-1.5 rounded-md bg-success-bg px-2 py-1 text-xs font-semibold text-success-text">
            <span class="h-1.5 w-1.5 rounded-full bg-success-2 animate-soft-pulse" aria-hidden="true"></span>
            Ready
          </span>
        </div>
        <dl class="mt-3 grid grid-cols-2 gap-x-4 gap-y-1.5 text-xs">
          <dt class="text-header-muted">Retention</dt>
          <dd class="text-right font-semibold text-white tabular-nums">24h</dd>
          <dt class="text-header-muted">Appearance</dt>
          <dd class="text-right font-semibold capitalize text-white">{theme.value}</dd>
          {#if buildVersion}
            <dt class="text-header-muted">Build</dt>
            <dd class="text-right font-mono text-[11px] font-semibold text-white tabular-nums">{buildVersion}</dd>
          {/if}
        </dl>
      </div>

      <button
        onclick={toggleTheme}
        class="group flex w-full items-center justify-between rounded-xl border border-white/10 bg-white/5 px-4 py-3 text-sm font-semibold text-header-muted hover:bg-white/10 hover:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-white/50 active:scale-[0.99]"
      >
        <span>Switch appearance</span>
        <span class="flex h-7 w-7 items-center justify-center rounded-lg bg-white/10 text-white group-hover:bg-white/15">
          {#if theme.value === 'dark'}
            <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" aria-hidden="true">
              <path stroke-linecap="round" stroke-linejoin="round" d="M12 3v1.5m0 15V21m9-9h-1.5M6 12H4.5m15.364-6.864l-1.06 1.06M5.696 18.304l-1.06 1.06m12.728 0l-1.06-1.06M6.757 6.757L5.696 5.696M16 12a4 4 0 11-8 0 4 4 0 018 0z" />
            </svg>
          {:else}
            <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" aria-hidden="true">
              <path stroke-linecap="round" stroke-linejoin="round" d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
            </svg>
          {/if}
        </span>
      </button>
    </div>
  </div>
</aside>

<header class="sticky top-0 z-20 border-b border-white/10 bg-header text-header-text shadow-soft lg:hidden">
  <div class="px-4 sm:px-6">
    <div class="flex min-h-16 items-center justify-between gap-4 py-2">
      <div class="flex items-center gap-3">
        <div class="relative flex h-9 w-9 items-center justify-center rounded-lg bg-accent-fill font-extrabold text-white ring-1 ring-white/20">
          R
          <span class="absolute -bottom-0.5 -right-0.5 h-2.5 w-2.5 rounded-full bg-success-2 ring-2 ring-header" aria-hidden="true"></span>
        </div>
        <div class="leading-tight">
          <h1 class="text-xl font-bold tracking-tight">ReDNS</h1>
          <p class="-mt-0.5 text-[11px] font-semibold uppercase tracking-[0.14em] text-header-muted">
            Resolver console{#if buildVersion}<span class="ml-1 normal-case tracking-normal text-header-muted/70">· {buildVersion}</span>{/if}
          </p>
        </div>
      </div>

      <div class="flex items-center gap-1.5">
        <button
          onclick={toggleTheme}
          aria-label="Toggle theme"
          title={theme.value === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
          class="rounded-lg p-2 text-header-muted hover:bg-white/10 hover:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-white/50"
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
          class="rounded-lg p-2 text-header-muted hover:bg-white/10 hover:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-white/50"
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
          {@const active = activeTab === tab.id}
          <button
            onclick={() => selectTab(tab.id)}
            aria-current={active ? 'page' : undefined}
            class="flex w-full items-center gap-3 rounded-lg px-3 py-2.5 text-left focus:outline-none focus-visible:ring-2 focus-visible:ring-white/50 {active ? 'bg-white/10 text-white' : 'text-header-muted hover:bg-white/5 hover:text-white'}"
          >
            <svg class="h-5 w-5 shrink-0 {active ? 'text-accent-3' : ''}" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.75" aria-hidden="true">
              <path stroke-linecap="round" stroke-linejoin="round" d={tab.icon} />
            </svg>
            <span class="min-w-0">
              <span class="block text-sm font-semibold">{tab.label}</span>
              <span class="block truncate text-[11px] font-medium text-header-muted/70">{tab.hint}</span>
            </span>
          </button>
        {/each}
      </nav>
    </div>
  {/if}
</header>
