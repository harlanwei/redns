<script lang="ts">
  import TopNav from './lib/components/TopNav.svelte';
  import LogsView from './lib/components/LogsView.svelte';
  import ClientsView from './lib/components/ClientsView.svelte';
  import UpstreamsView from './lib/components/UpstreamsView.svelte';
  import CacheView from './lib/components/CacheView.svelte';
  import type { TabId } from './lib/types/dashboard';

  let activeTab = $state<TabId>('logs');

  const pageMeta: Record<TabId, { title: string; description: string }> = {
    logs: {
      title: 'Queries',
      description: 'Search, inspect, and monitor resolver traffic.',
    },
    clients: {
      title: 'Clients',
      description: 'Sources ranked by query volume.',
    },
    cache: {
      title: 'Cache',
      description: 'Occupancy, hit rate, and shard balance.',
    },
    upstreams: {
      title: 'Upstreams',
      description: 'Selection, errors, and latency by server.',
    },
  };
</script>

<div class="min-h-dvh font-sans">
  <a
    href="#dashboard-content"
    class="sr-only focus:not-sr-only focus:fixed focus:left-4 focus:top-4 focus:z-50 focus:rounded-lg focus:border focus:border-line focus:bg-surface focus:px-4 focus:py-2 focus:text-sm focus:font-semibold focus:text-ink focus:shadow-card focus:outline-none focus:ring-2 focus:ring-accent"
  >
    Skip to dashboard content
  </a>

  <TopNav {activeTab} onTabChange={(tab) => (activeTab = tab)} />

  <main id="dashboard-content" class="mx-auto w-full max-w-[1400px] px-4 py-5 sm:px-6 sm:py-6 lg:px-8">
    {#key activeTab}
      <header class="animate-rise mb-5 flex flex-col gap-1 sm:mb-6 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 class="text-xl font-semibold tracking-tight text-ink sm:text-2xl">{pageMeta[activeTab].title}</h1>
          <p class="mt-1 text-sm text-muted">{pageMeta[activeTab].description}</p>
        </div>
      </header>

      <div class="animate-rise" style="animation-delay: 40ms">
        {#if activeTab === 'logs'}
          <LogsView />
        {:else if activeTab === 'clients'}
          <ClientsView />
        {:else if activeTab === 'cache'}
          <CacheView />
        {:else if activeTab === 'upstreams'}
          <UpstreamsView />
        {/if}
      </div>
    {/key}
  </main>
</div>
