<script lang="ts">
  import TopNav from './lib/components/TopNav.svelte';
  import LogsView from './lib/components/LogsView.svelte';
  import ClientsView from './lib/components/ClientsView.svelte';
  import UpstreamsView from './lib/components/UpstreamsView.svelte';
  import type { TabId } from './lib/types/dashboard';

  let activeTab = $state<TabId>('logs');
</script>

<div class="min-h-screen bg-gray-50 flex flex-col font-sans">
  <TopNav {activeTab} onTabChange={(tab) => (activeTab = tab)} />

  <main class="flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-6 sm:py-8">
    {#if activeTab === 'logs'}
      <LogsView />
    {:else if activeTab === 'clients'}
      <ClientsView />
    {:else if activeTab === 'upstreams'}
      <UpstreamsView />
    {/if}
  </main>
</div>
