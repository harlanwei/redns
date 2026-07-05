<script lang="ts">
  import TopNav from './lib/components/TopNav.svelte';
  import LogsView from './lib/components/LogsView.svelte';
  import ClientsView from './lib/components/ClientsView.svelte';
  import UpstreamsView from './lib/components/UpstreamsView.svelte';
  import CacheView from './lib/components/CacheView.svelte';
  import type { TabId } from './lib/types/dashboard';

  let activeTab = $state<TabId>('logs');

  const pageMeta: Record<TabId, { title: string; kicker: string; description: string; detail: string }> = {
    logs: {
      title: 'Query stream',
      kicker: 'Resolver activity',
      description: 'Live DNS requests, response codes, answer records, and latency.',
      detail: '24h retention',
    },
    clients: {
      title: 'Client activity',
      kicker: 'Traffic sources',
      description: 'Hosts and devices generating resolver load.',
      detail: 'Ranked by volume',
    },
    cache: {
      title: 'Cache profile',
      kicker: 'Resolver memory',
      description: 'Occupancy, shard balance, hit rate, and capacity pressure.',
      detail: 'Per cache instance',
    },
    upstreams: {
      title: 'Upstream health',
      kicker: 'Resolver egress',
      description: 'Upstream selection, completion, rejection, error, and latency metrics.',
      detail: 'Sortable metrics',
    },
  };
</script>

<div class="min-h-screen font-sans lg:grid lg:grid-cols-[17.5rem_minmax(0,1fr)]">
  <a
    href="#dashboard-content"
    class="sr-only focus:not-sr-only focus:fixed focus:left-4 focus:top-4 focus:z-50 focus:rounded-md focus:bg-surface focus:px-4 focus:py-2 focus:text-sm focus:font-semibold focus:text-ink focus:shadow-card focus:outline-none focus:ring-2 focus:ring-accent"
  >
    Skip to dashboard content
  </a>
  <TopNav {activeTab} onTabChange={(tab) => (activeTab = tab)} />

  <div class="min-w-0">
    <main id="dashboard-content" class="max-w-[1480px] w-full mx-auto px-4 sm:px-6 lg:px-8 py-5 sm:py-7 lg:py-8">
      {#key activeTab}
        <section class="animate-rise mb-6 border-b border-line pb-5">
          <div class="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
            <div class="min-w-0 max-w-3xl">
              <div class="mb-2.5 inline-flex items-center gap-1.5 text-[11px] font-semibold uppercase tracking-[0.12em] text-accent-2">
                <span class="h-1 w-6 rounded-full bg-accent" aria-hidden="true"></span>
                {pageMeta[activeTab].kicker}
              </div>
              <h1 class="text-[1.6rem] font-bold leading-tight tracking-tight text-ink sm:text-[2rem]">{pageMeta[activeTab].title}</h1>
              <p class="mt-2 max-w-[62ch] text-sm leading-6 text-muted">{pageMeta[activeTab].description}</p>
            </div>
            <div class="flex flex-wrap items-center gap-2 text-xs font-semibold">
              <span class="rounded-lg border border-line bg-surface px-3 py-1.5 text-muted shadow-soft">{pageMeta[activeTab].detail}</span>
              <span class="inline-flex items-center gap-1.5 rounded-lg border border-success/20 bg-success-bg px-3 py-1.5 text-success-text">
                <span class="h-1.5 w-1.5 rounded-full bg-success-2 animate-soft-pulse" aria-hidden="true"></span>
                Ready
              </span>
            </div>
          </div>
        </section>

        <div class="animate-rise" style="animation-delay: 60ms">
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
</div>
