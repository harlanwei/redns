<script lang="ts">
  import { onMount } from 'svelte';
  import { fade, slide } from 'svelte/transition';
  import type { DnsLogEntry, PaginatedLogsResponse } from '../types/dashboard';
  import { formatProtocol, formatRelativeTime, parseAnswer, formatUpstream } from '../utils/dashboard';
  import ErrorAlert from './ErrorAlert.svelte';

  let logsResponse = $state<PaginatedLogsResponse | null>(null);
  let searchQuery = $state('');
  let page = $state(1);
  let pageSize = $state(50);
  let pageInput = $state('');
  let loading = $state(true);
  let error = $state<string | null>(null);
  let autoRefresh = $state(false);
  let refreshInterval: ReturnType<typeof setInterval> | null = null;
  let selectedLog = $state<DnsLogEntry | null>(null);
  let geoipData = $state<Record<string, { city: string | null; asn: string | null; isp: string | null; proxy: boolean | null; hosting: boolean | null }>>({});

  $effect(() => {
    if (selectedLog) {
      geoipData = {};
      const ipsToFetch = new Set<string>();
      if (selectedLog.client_ip) ipsToFetch.add(selectedLog.client_ip);
      for (const row of selectedLog.result_rows || []) {
        const parsed = parseAnswer(row);
        if (parsed.type === 'A' || parsed.type === 'AAAA') {
          ipsToFetch.add(parsed.value);
        }
      }
      for (const ip of ipsToFetch) {
        fetch(`/api/geoip?ip=${encodeURIComponent(ip)}`)
          .then((res) => res.json())
          .then((data) => {
            geoipData[ip] = data;
          })
          .catch((err) => console.error('Failed to fetch geoip for', ip, err));
      }
    }
  });

  function getErrorMessage(err: unknown, fallback: string) {
    if (err instanceof Error && err.message) return err.message;
    return fallback;
  }

  async function fetchLogs(p = 1, query = '') {
    loading = true;
    error = null;
    try {
      const res = await fetch(`/api/logs?page=${p}&page_size=${pageSize}&q=${encodeURIComponent(query)}`);
      if (!res.ok) throw new Error('Failed to fetch logs');
      logsResponse = await res.json();
      page = p;
    } catch (err: unknown) {
      error = getErrorMessage(err, 'Failed to fetch logs');
    } finally {
      loading = false;
    }
  }

  async function clearLogs() {
    if (!confirm('Are you sure you want to clear all logs?')) return;
    try {
      const res = await fetch('/api/logs/clear', { method: 'POST' });
      if (!res.ok) throw new Error('Failed to clear logs');
      await fetchLogs(1, searchQuery);
    } catch (err: unknown) {
      error = getErrorMessage(err, 'Failed to clear logs');
    }
  }

  function handleSearchSubmit(e: Event) {
    e.preventDefault();
    fetchLogs(1, searchQuery);
  }

  $effect(() => {
    if (autoRefresh) {
      if (!refreshInterval) {
        refreshInterval = setInterval(() => {
          fetchLogs(page, searchQuery);
        }, 3000);
      }
    } else if (refreshInterval) {
      clearInterval(refreshInterval);
      refreshInterval = null;
    }
  });

  onMount(() => {
    fetchLogs();
    return () => {
      if (refreshInterval) clearInterval(refreshInterval);
    };
  });
</script>

{#if error}
  <ErrorAlert message={error} />
{/if}

{#if logsResponse?.summary}
  <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6" transition:fade>
    <div class="group relative glass rounded-2xl border border-line/60 p-5 overflow-hidden shadow-card hover:shadow-lift hover:-translate-y-0.5">
      <div class="absolute -top-10 -right-10 w-28 h-28 rounded-full opacity-20 blur-2xl" style="background: var(--ui-accent-grad);"></div>
      <div class="relative flex items-start justify-between">
        <div class="w-10 h-10 rounded-xl bg-grad-accent flex items-center justify-center shadow-glow">
          <svg class="w-5 h-5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-4.35-4.35M11 18a7 7 0 100-14 7 7 0 000 14z" /></svg>
        </div>
      </div>
      <div class="relative mt-4 text-3xl font-extrabold text-grad-accent tabular-nums leading-none">{logsResponse.summary.total_items.toLocaleString()}</div>
      <div class="relative mt-1.5 text-xs font-semibold text-muted uppercase tracking-wider">Total Queries</div>
    </div>

    <div class="group relative glass rounded-2xl border border-line/60 p-5 overflow-hidden shadow-card hover:shadow-lift hover:-translate-y-0.5">
      <div class="absolute -top-10 -right-10 w-28 h-28 rounded-full opacity-20 blur-2xl" style="background: var(--ui-info-grad);"></div>
      <div class="relative flex items-start justify-between">
        <div class="w-10 h-10 rounded-xl flex items-center justify-center shadow-md" style="background: var(--ui-info-grad);">
          <svg class="w-5 h-5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M17 20h5v-2a4 4 0 00-3-3.87M9 20H4v-2a4 4 0 013-3.87m6-1.13a4 4 0 100-8 4 4 0 000 8zm6 0a3 3 0 100-6 3 3 0 000 6zm-12 0a3 3 0 100-6 3 3 0 000 6z" /></svg>
        </div>
      </div>
      <div class="relative mt-4 text-3xl font-extrabold tabular-nums leading-none" style="color: var(--ui-info-text);">{logsResponse.summary.unique_clients.toLocaleString()}</div>
      <div class="relative mt-1.5 text-xs font-semibold text-muted uppercase tracking-wider">Unique Clients</div>
    </div>

    <div class="group relative glass rounded-2xl border border-line/60 p-5 overflow-hidden shadow-card hover:shadow-lift hover:-translate-y-0.5">
      <div class="absolute -top-10 -right-10 w-28 h-28 rounded-full opacity-20 blur-2xl" style="background: var(--ui-warn-grad);"></div>
      <div class="relative flex items-start justify-between">
        <div class="w-10 h-10 rounded-xl flex items-center justify-center shadow-md" style="background: var(--ui-warn-grad);">
          <svg class="w-5 h-5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M12 9v2m0 4h.01M5 19h14a2 2 0 001.84-2.75L13.74 4a2 2 0 00-3.48 0L3.16 16.25A2 2 0 005 19z" /></svg>
        </div>
      </div>
      <div class="relative mt-4 text-3xl font-extrabold tabular-nums leading-none" style="color: var(--ui-warn-text);">{logsResponse.summary.non_noerror.toLocaleString()}</div>
      <div class="relative mt-1.5 text-xs font-semibold text-muted uppercase tracking-wider">Non-NoError</div>
    </div>

    <div class="group relative glass rounded-2xl border border-line/60 p-5 overflow-hidden shadow-card hover:shadow-lift hover:-translate-y-0.5">
      <div class="absolute -top-10 -right-10 w-28 h-28 rounded-full opacity-20 blur-2xl" style="background: var(--ui-success-grad);"></div>
      <div class="relative flex items-start justify-between">
        <div class="w-10 h-10 rounded-xl flex items-center justify-center shadow-md" style="background: var(--ui-success-grad);">
          <svg class="w-5 h-5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z" /></svg>
        </div>
      </div>
      <div class="relative mt-4 text-3xl font-extrabold tabular-nums leading-none" style="color: var(--ui-success-text);">{logsResponse.summary.avg_latency_ms}<span class="text-lg text-faint font-bold ml-0.5">ms</span></div>
      <div class="relative mt-1.5 text-xs font-semibold text-muted uppercase tracking-wider">Avg Latency</div>
    </div>
  </div>
{/if}

<div class="glass rounded-2xl border border-line/60 shadow-card overflow-hidden flex flex-col">
  <div class="p-4 border-b border-line/60 flex flex-col md:flex-row justify-between items-center gap-4 glass-panel">
    <form onsubmit={handleSearchSubmit} class="w-full md:max-w-md relative">
      <div class="absolute inset-y-0 left-0 pl-3.5 flex items-center pointer-events-none">
        <svg class="h-5 w-5 text-faint" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
          <path fill-rule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z" clip-rule="evenodd" />
        </svg>
      </div>
      <input
        type="text"
        bind:value={searchQuery}
        placeholder="Search domains, IPs, record types..."
        class="block w-full pl-11 pr-3 py-2.5 border border-line rounded-xl bg-surface/80 placeholder-faint focus:outline-none focus:ring-2 focus:ring-accent focus:border-accent sm:text-sm shadow-soft"
      />
    </form>

    <div class="flex items-center gap-3 w-full md:w-auto">
      <label class="inline-flex items-center cursor-pointer mr-1">
        <input type="checkbox" bind:checked={autoRefresh} class="sr-only peer" />
        <div class="relative w-11 h-6 bg-line-2 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-accent rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-line-2 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-grad-accent"></div>
        <span class="ms-3 text-sm font-medium text-muted flex items-center gap-1.5">
          {#if autoRefresh}<span class="w-1.5 h-1.5 rounded-full bg-success-2 animate-soft-pulse"></span>{/if}
          Auto Refresh
        </span>
      </label>
      <button
        onclick={() => fetchLogs(1, searchQuery)}
        class="w-full md:w-auto inline-flex justify-center items-center gap-1.5 px-4 py-2.5 border border-line shadow-soft text-sm font-medium rounded-xl text-ink bg-surface hover:bg-panel hover:border-line-2 focus:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-surface focus-visible:ring-accent"
      >
        <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
        Refresh
      </button>
      <button
        onclick={clearLogs}
        class="w-full md:w-auto inline-flex justify-center items-center gap-1.5 px-4 py-2.5 border border-transparent shadow-soft text-sm font-semibold rounded-xl text-on-accent bg-grad-accent hover:shadow-glow focus:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-surface focus-visible:ring-accent"
      >
        <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
        Clear Logs
      </button>
    </div>
  </div>

  <div class="overflow-x-auto">
    <table class="min-w-full divide-y divide-line table-fixed">
      <thead class="glass-panel sticky top-0 z-10">
        <tr>
          <th scope="col" class="w-[14%] px-4 sm:px-6 py-3 text-left text-xs font-semibold text-muted uppercase tracking-wider">Time</th>
          <th scope="col" class="w-[18%] px-4 sm:px-6 py-3 text-left text-xs font-semibold text-muted uppercase tracking-wider">Client</th>
          <th scope="col" class="w-[36%] px-4 sm:px-6 py-3 text-left text-xs font-semibold text-muted uppercase tracking-wider">Query</th>
          <th scope="col" class="w-[11%] px-4 sm:px-6 py-3 text-left text-xs font-semibold text-muted uppercase tracking-wider">Status</th>
          <th scope="col" class="w-[10%] px-4 sm:px-6 py-3 text-right text-xs font-semibold text-muted uppercase tracking-wider">TTL</th>
          <th scope="col" class="w-[11%] px-4 sm:px-6 py-3 text-right text-xs font-semibold text-muted uppercase tracking-wider">Latency</th>
        </tr>
      </thead>
      <tbody class="divide-y divide-line/60">
        {#if loading && !logsResponse}
          {#each Array(8) as _, i}
            <tr>
              <td class="px-4 sm:px-6 py-4"><div class="skeleton h-4 rounded w-20"></div></td>
              <td class="px-4 sm:px-6 py-4"><div class="skeleton h-4 rounded w-28"></div></td>
              <td class="px-4 sm:px-6 py-4"><div class="skeleton h-4 rounded w-full max-w-xs"></div></td>
              <td class="px-4 sm:px-6 py-4"><div class="skeleton h-5 rounded-full w-16"></div></td>
              <td class="px-4 sm:px-6 py-4 text-right"><div class="skeleton h-4 rounded w-10 ml-auto"></div></td>
              <td class="px-4 sm:px-6 py-4 text-right"><div class="skeleton h-4 rounded w-12 ml-auto"></div></td>
            </tr>
          {/each}
        {:else if logsResponse?.items.length === 0}
          <tr><td colspan="6" class="px-6 py-16 text-center">
            <div class="inline-flex flex-col items-center gap-3 text-faint">
              <div class="w-14 h-14 rounded-2xl bg-panel border border-line flex items-center justify-center">
                <svg class="w-7 h-7" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5"><path stroke-linecap="round" stroke-linejoin="round" d="M9 17v-2a4 4 0 014-4h2m4-4H5a2 2 0 00-2 2v12a2 2 0 002 2h14a2 2 0 002-2V7a2 2 0 00-2-2z" opacity=".4"/><path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-4.35-4.35M11 18a7 7 0 100-14 7 7 0 000 14z"/></svg>
              </div>
              <span class="text-sm font-medium">No logs found</span>
            </div>
          </td></tr>
        {:else if logsResponse}
          {#each logsResponse.items as item (item.id)}
            <tr
              class="group hover:bg-accent-soft/60 cursor-pointer"
              in:fade={{ duration: 360 }}
              onclick={() => { selectedLog = item; }}
            >
              <td class="w-[14%] px-4 sm:px-6 py-4 whitespace-nowrap text-sm text-faint truncate" title={new Date(item.ts_unix_ms).toLocaleString()}>
                {formatRelativeTime(item.ts_unix_ms)}
              </td>
              <td class="w-[18%] px-4 sm:px-6 py-4 whitespace-nowrap text-sm text-ink font-medium truncate" title={`${item.client_ip} (${formatProtocol(item.protocol)})`}>
                {item.client_ip} <span class="text-faint text-xs font-normal">({formatProtocol(item.protocol)})</span>
              </td>
              <td class="w-[36%] px-4 sm:px-6 py-4 whitespace-nowrap text-sm truncate" title={`${item.qtype} ${item.qname}`}>
                <div class="flex items-center gap-2 truncate">
                  <span class="inline-flex items-center px-2 py-0.5 rounded-md text-xs font-bold bg-neutral-bg text-neutral-text border border-line">
                    {item.qtype}
                  </span>
                  <span class="text-ink font-medium truncate">{item.qname}</span>
                </div>
              </td>
              <td class="w-[11%] px-4 sm:px-6 py-4 whitespace-nowrap text-sm truncate">
                <span class="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-semibold {item.rcode.toLowerCase() === 'noerror' ? 'bg-success-bg text-success-text ring-1 ring-inset ring-success/20' : 'bg-danger-bg text-danger-text ring-1 ring-inset ring-danger/10'}">
                  <span class="w-1.5 h-1.5 rounded-full {item.rcode.toLowerCase() === 'noerror' ? 'bg-success-2' : 'bg-danger-2'}"></span>
                  {item.rcode}
                </span>
              </td>
              <td class="w-[10%] px-4 sm:px-6 py-4 whitespace-nowrap text-sm text-right font-medium text-ink tabular-nums truncate">
                {#if item.answer_ttl > 0}
                  {item.answer_ttl}<span class="text-faint text-xs font-normal ml-0.5">s</span>
                {:else}
                  <span class="text-faint">—</span>
                {/if}
              </td>
              <td class="w-[11%] px-4 sm:px-6 py-4 whitespace-nowrap text-sm text-right font-medium text-ink tabular-nums truncate">
                {item.latency_ms}<span class="text-faint text-xs font-normal ml-0.5">ms</span>
              </td>
            </tr>
          {/each}
        {/if}
      </tbody>
    </table>
  </div>

  {#if logsResponse && logsResponse.total_pages > 1}
    <div class="glass-panel px-4 py-3 border-t border-line/60 flex items-center justify-between sm:px-6">
      <div class="flex-1 flex justify-between sm:hidden">
        <button
          disabled={page === 1}
          onclick={() => fetchLogs(page - 1, searchQuery)}
          class="relative inline-flex items-center px-4 py-2 border border-line text-sm font-medium rounded-xl text-ink bg-surface hover:bg-panel disabled:opacity-40 disabled:cursor-not-allowed"
        >
          Previous
        </button>
        <button
          disabled={page >= logsResponse.total_pages}
          onclick={() => fetchLogs(page + 1, searchQuery)}
          class="ml-3 relative inline-flex items-center px-4 py-2 border border-line text-sm font-medium rounded-xl text-ink bg-surface hover:bg-panel disabled:opacity-40 disabled:cursor-not-allowed"
        >
          Next
        </button>
      </div>
      <div class="hidden sm:flex-1 sm:flex sm:items-center sm:justify-between">
        <div>
          <p class="text-sm text-muted">
            Showing page <span class="font-semibold text-ink">{logsResponse.page}</span> of <span class="font-semibold text-ink">{logsResponse.total_pages}</span>
          </p>
        </div>
        <div class="flex items-center gap-2">
          <nav class="relative z-0 inline-flex rounded-xl shadow-soft -space-x-px" aria-label="Pagination">
            <button
              onclick={() => fetchLogs(1, searchQuery)}
              class="relative inline-flex items-center px-3 py-2 rounded-l-xl border border-line bg-surface text-sm font-medium text-muted hover:bg-panel transition-colors"
            >
              First
            </button>
            {#if page > 2}
              <button onclick={() => fetchLogs(page - 2, searchQuery)} class="relative inline-flex items-center px-3 py-2 border border-line bg-surface text-sm font-medium text-muted hover:bg-panel transition-colors">{page - 2}</button>
            {/if}
            {#if page > 1}
              <button onclick={() => fetchLogs(page - 1, searchQuery)} class="relative inline-flex items-center px-3 py-2 border border-line bg-surface text-sm font-medium text-muted hover:bg-panel transition-colors">{page - 1}</button>
            {/if}
            <button class="relative inline-flex items-center px-3.5 py-2 border border-transparent bg-grad-accent text-on-accent text-sm font-bold z-10 cursor-default">{page}</button>
            {#if page < logsResponse.total_pages}
              <button onclick={() => fetchLogs(page + 1, searchQuery)} class="relative inline-flex items-center px-3 py-2 border border-line bg-surface text-sm font-medium text-muted hover:bg-panel transition-colors">{page + 1}</button>
            {/if}
            {#if page < logsResponse.total_pages - 1}
              <button onclick={() => fetchLogs(page + 2, searchQuery)} class="relative inline-flex items-center px-3 py-2 border border-line bg-surface text-sm font-medium text-muted hover:bg-panel transition-colors">{page + 2}</button>
            {/if}
            {#if page < logsResponse.total_pages - 2}
              <span class="relative inline-flex items-center px-3 py-2 border border-line bg-surface text-sm font-medium text-faint">...</span>
            {/if}
            <button
              onclick={() => fetchLogs(logsResponse.total_pages, searchQuery)}
              class="relative inline-flex items-center px-3 py-2 rounded-r-xl border border-line bg-surface text-sm font-medium text-muted hover:bg-panel transition-colors"
            >
              {logsResponse.total_pages}
            </button>
          </nav>
          <form
            onsubmit={(e) => {
              e.preventDefault();
              const p = parseInt(pageInput, 10);
              if (!isNaN(p) && p >= 1 && p <= logsResponse.total_pages) {
                fetchLogs(p, searchQuery);
                pageInput = '';
              }
            }}
            class="flex items-center ml-2"
          >
            <input type="number" min="1" max={logsResponse.total_pages} bind:value={pageInput} placeholder="Page" class="block w-16 px-2 py-1.5 text-sm border border-line bg-surface text-ink rounded-l-xl focus:ring-accent focus:border-accent placeholder-faint" />
            <button type="submit" class="inline-flex items-center px-3 py-1.5 border border-l-0 border-line rounded-r-xl bg-panel text-muted text-sm font-medium hover:bg-line transition-colors">Go</button>
          </form>
        </div>
      </div>
    </div>
  {/if}
</div>

{#if selectedLog}
  <div class="fixed inset-0 z-50 overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true" transition:fade={{ duration: 150 }}>
    <div class="flex items-end justify-center min-h-screen pt-4 px-2 pb-4 text-left sm:block sm:p-0 sm:text-center">
      <div class="fixed inset-0 z-0 bg-ink/70 backdrop-blur-md" aria-hidden="true" onclick={() => (selectedLog = null)}></div>
      <span class="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>
      <div class="relative z-10 inline-block w-full align-bottom glass rounded-2xl text-left overflow-hidden shadow-lift transform transition-all sm:my-8 sm:align-middle sm:max-w-2xl sm:w-full border border-line/60" transition:slide={{ duration: 200 }}>
        <div class="bg-grad-header text-header-text px-4 pt-5 pb-5 sm:px-6 relative overflow-hidden">
          <div class="absolute inset-0 opacity-50" style="background: radial-gradient(400px 160px at 85% -20%, rgba(124,143,252,0.4), transparent 70%);"></div>
          <div class="relative sm:flex sm:items-start">
            <div class="mt-3 text-left sm:mt-0 w-full">
              <h3 class="text-xl leading-6 font-bold flex items-center gap-2" id="modal-title">
                Query Results
              </h3>
              <div class="mt-2 text-sm text-header-muted flex gap-2 items-center">
                <span class="inline-flex items-center px-2 py-0.5 rounded-md text-xs font-bold bg-white/10 text-white border border-white/15">
                  {selectedLog?.qtype}
                </span>
                <span class="font-mono text-white/90 truncate max-w-sm">{selectedLog?.qname}</span>
              </div>
            </div>
          </div>
        </div>

        <div class="bg-surface px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
          <div class="flex flex-col gap-3 mb-4 text-sm glass-panel p-4 rounded-xl border border-line/60">
            <div class="flex flex-col sm:flex-row sm:justify-between sm:items-center border-b border-line/60 pb-2">
              <div class="text-muted font-medium mb-1 sm:mb-0">Time</div>
              <div class="font-medium text-ink text-left sm:text-right">{selectedLog ? new Date(selectedLog.ts_unix_ms).toLocaleString() : '-'}</div>
            </div>
            <div class="flex flex-col sm:flex-row sm:justify-between sm:items-center border-b border-line/60 pb-2">
              <div class="text-muted font-medium mb-1 sm:mb-0">Client</div>
              <div class="text-left sm:text-right">
                <div class="font-medium text-ink">{selectedLog?.client_ip} <span class="text-faint text-xs font-normal">({formatProtocol(selectedLog?.protocol || '')})</span></div>
                {#if selectedLog && geoipData[selectedLog?.client_ip || '']}
                  {#if geoipData[selectedLog?.client_ip || '']?.city || geoipData[selectedLog?.client_ip || '']?.asn || geoipData[selectedLog?.client_ip || '']?.isp || geoipData[selectedLog?.client_ip || '']?.proxy || geoipData[selectedLog?.client_ip || '']?.hosting}
                    <div class="mt-1 flex flex-wrap sm:justify-end gap-2 text-xs font-sans">
                      {#if geoipData[selectedLog?.client_ip || '']?.city}
                        <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-info-bg text-info-text border border-info/20">
                          <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 11a3 3 0 11-6 0 3 3 0 016 0z"></path></svg>
                          {geoipData[selectedLog?.client_ip || '']?.city}
                        </span>
                      {/if}
                      {#if geoipData[selectedLog?.client_ip || '']?.asn}
                        <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-neutral-bg text-neutral-text border border-line">
                          <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9"></path></svg>
                          {geoipData[selectedLog?.client_ip || '']?.asn}
                        </span>
                      {/if}
                      {#if geoipData[selectedLog?.client_ip || '']?.isp}
                        <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-info-bg text-info-text border border-info/20">
                          <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
                          {geoipData[selectedLog?.client_ip || '']?.isp}
                        </span>
                      {/if}
                      {#if geoipData[selectedLog?.client_ip || '']?.proxy}
                        <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-danger-bg text-danger-text border border-danger/20">
                          <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 11c0 3.517-1.009 6.799-2.753 9.571m-3.44-2.04l.054-.09A13.916 13.916 0 008 11a4 4 0 118 0c0 1.017-.07 2.019-.203 3m-2.118 6.844A21.88 21.88 0 0015.171 17m3.839 1.132c.645-2.266.99-4.659.99-7.132A8 8 0 008 4.07M3 15.364c.64-1.319 1-2.8 1-4.364 0-1.457.39-2.823 1.07-4"></path></svg>
                          Proxy
                        </span>
                      {/if}
                      {#if geoipData[selectedLog?.client_ip || '']?.hosting}
                        <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-warn-bg text-warn-text border border-warn/20">
                          <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01"></path></svg>
                          Hosting
                        </span>
                      {/if}
                    </div>
                  {/if}
                {/if}
              </div>
            </div>
            <div class="flex flex-col sm:flex-row sm:justify-between sm:items-center border-b border-line/60 pb-2">
              <div class="text-muted font-medium mb-1 sm:mb-0">Status</div>
              <div class="text-left sm:text-right">
                <span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-md text-xs font-semibold {selectedLog?.rcode?.toLowerCase() === 'noerror' ? 'bg-success-bg text-success-text' : 'bg-danger-bg text-danger-text'}">
                  <span class="w-1.5 h-1.5 rounded-full {selectedLog?.rcode?.toLowerCase() === 'noerror' ? 'bg-success-2' : 'bg-danger-2'}"></span>
                  {selectedLog?.rcode}
                </span>
              </div>
            </div>
            <div class="flex flex-col sm:flex-row sm:justify-between sm:items-center border-b border-line/60 pb-2">
              <div class="text-muted font-medium mb-1 sm:mb-0">Latency</div>
              <div class="font-medium text-ink text-left sm:text-right">{selectedLog?.latency_ms}<span class="text-faint text-xs font-normal ml-0.5">ms</span></div>
            </div>
            {#if (selectedLog?.upstreams?.length ?? 0) > 0}
              <div class="flex flex-col sm:flex-row sm:justify-between sm:items-start">
                <div class="text-muted font-medium mb-1 sm:mb-0 mt-1">Upstream</div>
                <div class="flex flex-wrap gap-1">
                  {#each selectedLog?.upstreams ?? [] as upstream}
                    {#if upstream === '__C__'}
                      <span class="inline-flex items-center px-2 py-0.5 rounded-md text-xs font-semibold bg-info-bg text-info-text ring-1 ring-inset ring-info/20">System Cache</span>
                    {:else}
                      <span class="inline-flex items-center px-2 py-0.5 rounded-md text-xs font-semibold bg-neutral-bg text-neutral-text ring-1 ring-inset ring-line-2">{formatUpstream(upstream)}</span>
                    {/if}
                  {/each}
                </div>
              </div>
            {/if}
          </div>

          <div class="mt-4 border border-line/60 rounded-xl overflow-hidden">
            <table class="min-w-full divide-y divide-line/60">
              <thead class="glass-panel">
                <tr>
                  <th scope="col" class="px-6 py-3 text-left text-xs font-semibold text-muted uppercase tracking-wider w-1/6">Type</th>
                  <th scope="col" class="px-6 py-3 text-left text-xs font-semibold text-muted uppercase tracking-wider">Value</th>
                  <th scope="col" class="px-6 py-3 text-right text-xs font-semibold text-muted uppercase tracking-wider w-1/6">TTL</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-line/60">
                {#each selectedLog?.result_rows || [] as row}
                  {@const parsed = parseAnswer(row)}
                  <tr class="hover:bg-accent-soft/50">
                    <td class="px-6 py-3 whitespace-nowrap text-left text-sm text-muted font-medium">{parsed.type}</td>
                    <td class="px-6 py-3 text-left text-sm text-ink font-mono break-all">
                      {parsed.value}
                      {#if (parsed.type === 'A' || parsed.type === 'AAAA') && geoipData[parsed.value]}
                        {#if geoipData[parsed.value]?.city || geoipData[parsed.value]?.asn || geoipData[parsed.value]?.isp || geoipData[parsed.value]?.proxy || geoipData[parsed.value]?.hosting}
                          <div class="mt-1 flex flex-wrap gap-2 text-xs font-sans">
                            {#if geoipData[parsed.value]?.city}
                              <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-info-bg text-info-text border border-info/20">
                                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 11a3 3 0 11-6 0 3 3 0 016 0z"></path></svg>
                                {geoipData[parsed.value]?.city}
                              </span>
                            {/if}
                            {#if geoipData[parsed.value]?.asn}
                              <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-neutral-bg text-neutral-text border border-line">
                                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9"></path></svg>
                                {geoipData[parsed.value]?.asn}
                              </span>
                            {/if}
                            {#if geoipData[parsed.value]?.isp}
                              <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-info-bg text-info-text border border-info/20">
                                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
                                {geoipData[parsed.value]?.isp}
                              </span>
                            {/if}
                            {#if geoipData[parsed.value]?.proxy}
                              <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-danger-bg text-danger-text border border-danger/20">
                                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 11c0 3.517-1.009 6.799-2.753 9.571m-3.44-2.04l.054-.09A13.916 13.916 0 008 11a4 4 0 118 0c0 1.017-.07 2.019-.203 3m-2.118 6.844A21.88 21.88 0 0015.171 17m3.839 1.132c.645-2.266.99-4.659.99-7.132A8 8 0 008 4.07M3 15.364c.64-1.319 1-2.8 1-4.364 0-1.457.39-2.823 1.07-4"></path></svg>
                                Proxy
                              </span>
                            {/if}
                            {#if geoipData[parsed.value]?.hosting}
                              <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-warn-bg text-warn-text border border-warn/20">
                                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01"></path></svg>
                                Hosting
                              </span>
                            {/if}
                          </div>
                        {/if}
                      {/if}
                    </td>
                    <td class="px-6 py-3 text-right whitespace-nowrap text-sm text-muted font-medium tabular-nums">
                      {#if parsed.ttl !== undefined}
                        {parsed.ttl}<span class="text-faint text-xs ml-0.5">s</span>
                      {:else}
                        <span class="text-faint">—</span>
                      {/if}
                    </td>
                  </tr>
                {/each}
                {#if (selectedLog?.result_rows || []).length === 0}
                  <tr><td colspan="3" class="px-6 py-8 text-sm text-faint text-center italic">No answers recorded</td></tr>
                {/if}
              </tbody>
            </table>
          </div>
        </div>
        <div class="glass-panel px-4 py-3 sm:px-6 sm:flex sm:flex-row-reverse border-t border-line/60">
          <button type="button" class="mt-3 w-full inline-flex justify-center rounded-xl border border-line shadow-soft px-4 py-2 bg-surface text-base font-medium text-ink hover:bg-panel focus:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-surface focus-visible:ring-accent sm:mt-0 sm:ml-3 sm:w-auto sm:text-sm transition-colors" onclick={() => (selectedLog = null)}>Close</button>
        </div>
      </div>
    </div>
  </div>
{/if}
