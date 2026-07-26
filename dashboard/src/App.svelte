<script lang="ts">
  import { onMount, tick } from 'svelte';
  import TopNav from './lib/components/TopNav.svelte';
  import LogsView from './lib/components/LogsView.svelte';
  import ClientsView from './lib/components/ClientsView.svelte';
  import UpstreamsView from './lib/components/UpstreamsView.svelte';
  import CacheView from './lib/components/CacheView.svelte';
  import type { TabId } from './lib/types/dashboard';
  import { route, navigate, initRouter } from './lib/utils/router.svelte';

  const activeTab = $derived(displayedTab);
  const tabs: TabId[] = ['logs', 'clients', 'cache', 'upstreams'];
  let displayedTab = $state<TabId>(route.tab);
  let contentVisible = $state(false);
  let animatedTab = $state<TabId | null>(null);
  let pendingTab = $state<TabId | null>(null);
  let readyTabs = $state<Record<TabId, boolean>>({
    logs: false,
    clients: false,
    cache: false,
    upstreams: false,
  });
  let transitionId = 0;
  let revealId = 0;

  async function revealCurrentTab() {
    const tab = displayedTab;
    if (contentVisible || pendingTab !== null || !readyTabs[tab]) return;

    const id = ++revealId;
    // Initial content stays completely hidden until Svelte has committed its
    // loaded state and the browser has had a chance to paint it.
    await tick();
    await new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
    if (id === revealId && displayedTab === tab && pendingTab === null) {
      contentVisible = true;
    }
  }

  function markTabReady(tab: TabId) {
    readyTabs[tab] = true;
    void revealCurrentTab();
  }

  async function renderBeforeSwitch(tab: TabId, updateRoute: boolean) {
    if (tab === displayedTab) return;

    const id = ++transitionId;
    pendingTab = tab;
    while (!readyTabs[tab]) {
      await new Promise<void>((resolve) => window.setTimeout(resolve, 16));
      if (id !== transitionId || (!updateRoute && route.tab !== tab)) return;
    }

    // Let Svelte commit the loaded data, then give the browser a paint before
    // exposing the panel. The enter animation therefore never runs over a
    // skeleton or partially populated view.
    await tick();
    await new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
    if (id !== transitionId || (!updateRoute && route.tab !== tab)) return;

    displayedTab = tab;
    // The initial panel is revealed as already-rendered content. Only later
    // tab changes get an entrance animation; animating the first paint from
    // opacity: 0 was the visible flash on the logs page.
    animatedTab = contentVisible ? tab : null;
    contentVisible = true;
    pendingTab = null;
    if (updateRoute) navigate({ tab });
  }

  function requestTab(tab: TabId) {
    if (tab === displayedTab) return;
    void renderBeforeSwitch(tab, true);
  }

  $effect(() => {
    // Hash/back-forward navigation can target a tab without going through the
    // navigation buttons. Apply the same readiness gate in that case.
    if (route.tab !== displayedTab) void renderBeforeSwitch(route.tab, false);
  });

  onMount(() => {
    initRouter();

    const onMove = (e: MouseEvent) => {
      const el = (e.target as Element).closest<HTMLElement>('.stat, .data-table tbody tr');
      if (!el) return;
      const rect = el.getBoundingClientRect();
      el.style.setProperty('--glow-x', `${e.clientX - rect.left}px`);
      el.style.setProperty('--glow-y', `${e.clientY - rect.top}px`);
    };
    document.addEventListener('mousemove', onMove, { passive: true });
    return () => document.removeEventListener('mousemove', onMove);
  });

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

  $effect(() => {
    document.title = `${pageMeta[activeTab].title} · ReDNS`;
  });
</script>

<div class="min-h-dvh font-sans">
  <a
    href="#dashboard-content"
    class="sr-only focus:not-sr-only focus:fixed focus:left-4 focus:top-4 focus:z-50 focus:rounded-lg focus:border focus:border-line focus:bg-surface focus:px-4 focus:py-2 focus:text-sm focus:font-semibold focus:text-ink focus:shadow-card focus:outline-none focus:ring-2 focus:ring-accent"
  >
    Skip to dashboard content
  </a>

  <TopNav {activeTab} onTabChange={requestTab} />

  <main
    id="dashboard-content"
    class="dashboard-content mx-auto w-full max-w-[1400px] px-4 py-5 sm:px-6 sm:py-6 lg:px-8"
    class:content-ready={contentVisible}
    aria-busy={!contentVisible || pendingTab !== null}
  >
    <div class="tab-panels">
      {#each tabs as tab}
        <section
          class="tab-panel"
          class:tab-panel-active={displayedTab === tab}
          class:animate-rise={animatedTab === tab}
          aria-hidden={displayedTab !== tab}
        >
          {#if tab === 'logs'}
            <LogsView onReady={() => markTabReady('logs')} />
          {:else if tab === 'clients'}
            <ClientsView onReady={() => markTabReady('clients')} />
          {:else if tab === 'cache'}
            <CacheView onReady={() => markTabReady('cache')} />
          {:else if tab === 'upstreams'}
            <UpstreamsView onReady={() => markTabReady('upstreams')} />
          {/if}
        </section>
      {/each}
    </div>
  </main>
</div>

<style>
  .dashboard-content {
    visibility: hidden;
  }

  .dashboard-content.content-ready {
    visibility: visible;
  }

  .tab-panels {
    position: relative;
  }

  .tab-panel {
    visibility: hidden;
    pointer-events: none;
    position: absolute;
    inset-inline: 0;
    top: 0;
  }

  .tab-panel-active {
    visibility: visible;
    pointer-events: auto;
    position: relative;
    inset: auto;
  }
</style>
