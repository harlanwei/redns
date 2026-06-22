<script lang="ts">
  import { slide } from 'svelte/transition';
  import type { TabId } from '../types/dashboard';
  import { theme, toggleTheme } from '../utils/theme.svelte';

  const tabs: TabId[] = ['logs', 'clients', 'cache', 'upstreams'];

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

<header class="bg-grad-header text-header-text z-20 sticky top-0 border-b border-white/10 shadow-lg">
  <div class="absolute inset-0 pointer-events-none opacity-60" style="background: radial-gradient(600px 200px at 90% -40%, rgba(124,143,252,0.35), transparent 70%);"></div>
  <div class="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
    <div class="flex justify-between h-16 items-center">
      <div class="flex items-center gap-3">
        <div class="relative w-10 h-10 rounded-xl bg-grad-accent animate-drift flex items-center justify-center font-extrabold shadow-lg ring-1 ring-white/20">
          R
          <span class="absolute -bottom-0.5 -right-0.5 w-3 h-3 rounded-full bg-success-2 ring-2 ring-header animate-soft-pulse"></span>
        </div>
        <div class="leading-tight">
          <h1 class="text-xl font-bold tracking-tight">ReDNS</h1>
          <p class="text-[11px] uppercase tracking-[0.18em] text-header-muted -mt-0.5">DNS Dashboard</p>
        </div>
      </div>

      <nav class="hidden sm:flex items-center space-x-1 p-1 rounded-xl bg-white/5 ring-1 ring-white/10">
        {#each tabs as tab}
          <button
            onclick={() => selectTab(tab)}
            class="relative px-4 py-1.5 rounded-lg text-sm font-medium capitalize transition-all duration-200 {activeTab === tab ? 'text-white' : 'text-header-muted hover:text-white'}"
          >
            {#if activeTab === tab}
              <span class="absolute inset-0 rounded-lg bg-grad-accent shadow-glow opacity-95"></span>
            {/if}
            <span class="relative">{tab}</span>
          </button>
        {/each}
      </nav>

      <div class="flex items-center gap-1.5">
        <button
          onclick={toggleTheme}
          aria-label="Toggle theme"
          title={theme.value === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
          class="p-2 rounded-lg text-header-muted hover:bg-white/10 hover:text-white transition-colors duration-200 focus:outline-none focus-visible:ring-2 focus-visible:ring-white/40"
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
          class="sm:hidden p-2 rounded-lg text-header-muted hover:bg-white/10 hover:text-white transition-colors"
          aria-label="Menu"
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
    <div class="sm:hidden bg-header-2/95 backdrop-blur border-t border-white/10" transition:slide>
      <div class="px-3 pt-3 pb-4 space-y-1">
        {#each tabs as tab}
          <button
            onclick={() => selectTab(tab)}
            class="block w-full text-left px-4 py-2.5 rounded-lg text-base font-medium capitalize transition-colors {activeTab === tab ? 'bg-grad-accent text-white shadow-glow' : 'text-header-muted hover:bg-white/10 hover:text-white'}"
          >
            {tab}
          </button>
        {/each}
      </div>
    </div>
  {/if}
</header>
