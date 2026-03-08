<script lang="ts">
  import { slide } from 'svelte/transition';
  import type { TabId } from '../types/dashboard';

  const tabs: TabId[] = ['logs', 'clients', 'upstreams'];

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

<header class="bg-navy-900 text-white shadow-md z-20 sticky top-0">
  <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
    <div class="flex justify-between h-16 items-center">
      <div class="flex items-center gap-3">
        <div class="w-8 h-8 rounded bg-navy-600 flex items-center justify-center font-bold text-white shadow-inner">
          R
        </div>
        <h1 class="text-xl font-bold tracking-tight">ReDNS <span class="text-navy-300 font-normal">Dashboard</span></h1>
      </div>

      <nav class="hidden sm:flex space-x-2">
        {#each tabs as tab}
          <button
            onclick={() => selectTab(tab)}
            class="px-4 py-2 rounded-md text-sm font-medium transition-colors duration-200 {activeTab === tab ? 'bg-navy-700 text-white shadow-sm ring-1 ring-navy-600' : 'text-navy-200 hover:bg-navy-800 hover:text-white capitalize'}"
          >
            <span class="capitalize">{tab}</span>
          </button>
        {/each}
      </nav>

      <div class="sm:hidden flex items-center">
        <button
          onclick={() => (mobileMenuOpen = !mobileMenuOpen)}
          class="text-navy-200 hover:text-white focus:outline-none focus:text-white"
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
    <div class="sm:hidden bg-navy-800 border-t border-navy-700" transition:slide>
      <div class="px-2 pt-2 pb-3 space-y-1">
        {#each tabs as tab}
          <button
            onclick={() => selectTab(tab)}
            class="block w-full text-left px-3 py-2 rounded-md text-base font-medium {activeTab === tab ? 'bg-navy-900 text-white' : 'text-navy-200 hover:bg-navy-700 hover:text-white capitalize'}"
          >
            <span class="capitalize">{tab}</span>
          </button>
        {/each}
      </div>
    </div>
  {/if}
</header>
