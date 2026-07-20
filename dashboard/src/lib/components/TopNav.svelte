<script lang="ts">
  import { onMount } from 'svelte';
  import { slide } from 'svelte/transition';
  import type { TabId } from '../types/dashboard';
  import { theme, toggleTheme } from '../utils/theme.svelte';
  import Logo from './Logo.svelte';

  type Tab = { id: TabId; label: string };

  const tabs: Tab[] = [
    { id: 'logs', label: 'Queries' },
    { id: 'clients', label: 'Clients' },
    { id: 'cache', label: 'Cache' },
    { id: 'upstreams', label: 'Upstreams' },
  ];

  let { activeTab, onTabChange } = $props<{
    activeTab: TabId;
    onTabChange: (tab: TabId) => void;
  }>();

  let mobileMenuOpen = $state(false);
  let buildVersion = $state('');

  onMount(async () => {
    try {
      const res = await fetch('/api/version');
      if (res.ok) {
        const data = await res.json();
        if (typeof data?.version === 'string') buildVersion = data.version.trim();
      }
    } catch {
      // Non-critical metadata.
    }
  });

  function selectTab(tab: TabId) {
    onTabChange(tab);
    mobileMenuOpen = false;
  }
</script>

<header class="sticky top-0 z-30 border-b border-line bg-surface/90 backdrop-blur-md">
  <div class="mx-auto flex h-14 max-w-[1400px] items-center gap-3 px-4 sm:h-16 sm:gap-6 sm:px-6 lg:px-8">
    <div class="flex min-w-0 items-center gap-2.5">
      <div
        class="logo-tile group relative flex h-8 w-8 items-center justify-center rounded-lg text-on-accent transition duration-200 hover:-translate-y-0.5 bg-[radial-gradient(130%_130%_at_28%_18%,rgba(101,112,126,0.5),transparent_60%),linear-gradient(135deg,#2a3038_0%,#12161c_55%,#05070a_100%)] dark:bg-[radial-gradient(130%_130%_at_28%_18%,rgba(255,255,255,0.95),transparent_55%),linear-gradient(135deg,#ffffff_0%,#eef1f5_55%,#d2dae3_100%)] shadow-[inset_0_1px_0_rgba(255,255,255,0.16),inset_0_-1px_0_rgba(0,0,0,0.75),0_2px_8px_-2px_rgba(8,10,14,0.55)] hover:shadow-[inset_0_1px_0_rgba(255,255,255,0.22),inset_0_-1px_0_rgba(0,0,0,0.75),0_8px_20px_-8px_rgba(8,10,14,0.65)] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.95),inset_0_-1px_0_rgba(139,149,163,0.55),0_2px_8px_-2px_rgba(0,0,0,0.7)] dark:hover:shadow-[inset_0_1px_0_rgba(255,255,255,0.95),inset_0_-1px_0_rgba(139,149,163,0.55),0_8px_20px_-8px_rgba(0,0,0,0.8)]"
      >
        <Logo size="h-7 w-7" />
        <span
          class="pointer-events-none absolute inset-0 rounded-lg bg-linear-to-br from-white/15 via-white/5 to-transparent opacity-70 transition-opacity duration-300 group-hover:opacity-100"
          aria-hidden="true"
        ></span>
      </div>
      <div class="min-w-0 leading-tight">
        <div class="text-sm font-semibold tracking-tight text-ink">ReDNS</div>
        {#if buildVersion}
          <div class="truncate font-mono text-[10px] text-faint">{buildVersion}</div>
        {/if}
      </div>
    </div>

    <nav class="hidden min-w-0 flex-1 items-center gap-1 md:flex" aria-label="Dashboard sections">
      {#each tabs as tab}
        <button
          class="topnav-tab"
          aria-current={activeTab === tab.id ? 'page' : undefined}
          onclick={() => selectTab(tab.id)}
        >
          {tab.label}
        </button>
      {/each}
    </nav>

    <div class="ml-auto flex items-center gap-1.5 sm:gap-2">
      <button
        onclick={toggleTheme}
        class="btn btn-ghost !px-2.5"
        aria-label="Toggle theme"
        title={theme.value === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
      >
        {#if theme.value === 'dark'}
          <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.75" aria-hidden="true">
            <path stroke-linecap="round" stroke-linejoin="round" d="M12 3v1.5m0 15V21m9-9h-1.5M6 12H4.5m15.364-6.864l-1.06 1.06M5.696 18.304l-1.06 1.06m12.728 0l-1.06-1.06M6.757 6.757L5.696 5.696M16 12a4 4 0 11-8 0 4 4 0 018 0z" />
          </svg>
        {:else}
          <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.75" aria-hidden="true">
            <path stroke-linecap="round" stroke-linejoin="round" d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
          </svg>
        {/if}
      </button>

      <button
        class="btn btn-ghost !px-2.5 md:hidden"
        onclick={() => (mobileMenuOpen = !mobileMenuOpen)}
        aria-label="Menu"
        aria-expanded={mobileMenuOpen}
      >
        <svg class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.75">
          {#if mobileMenuOpen}
            <path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12" />
          {:else}
            <path stroke-linecap="round" stroke-linejoin="round" d="M4 6h16M4 12h16M4 18h16" />
          {/if}
        </svg>
      </button>
    </div>
  </div>

  {#if mobileMenuOpen}
    <div class="border-t border-line bg-surface md:hidden" transition:slide>
      <nav class="mx-auto flex max-w-[1400px] flex-col gap-1 px-3 py-3" aria-label="Dashboard sections">
        {#each tabs as tab}
          <button
            class="topnav-tab w-full justify-start"
            aria-current={activeTab === tab.id ? 'page' : undefined}
            onclick={() => selectTab(tab.id)}
          >
            {tab.label}
          </button>
        {/each}
      </nav>
    </div>
  {/if}
</header>
