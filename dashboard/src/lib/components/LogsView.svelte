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

  function formatLogAge(ts: number) {
    return formatRelativeTime(ts).replace(' ago', '');
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

  function handlePageSizeChange(e: Event) {
    pageSize = Number((e.currentTarget as HTMLSelectElement).value);
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
  <section class="mb-5 rounded-md border border-line bg-surface shadow-card overflow-hidden" aria-label="Query summary" transition:fade>
    <div class="grid divide-y divide-line sm:grid-cols-4 sm:divide-x sm:divide-y-0">
      <div class="bg-header p-4 text-header-text">
        <div class="text-xs font-semibold uppercase tracking-[0.06em] text-accent-3">Queries</div>
        <div class="mt-2 text-3xl font-bold tracking-tight text-white tabular-nums">{logsResponse.summary.total_items.toLocaleString()}</div>
        <div class="mt-1 text-xs text-header-muted">Current retention window</div>
      </div>
      <div class="p-4">
        <div class="text-xs font-semibold uppercase tracking-[0.06em] text-muted">Clients</div>
        <div class="mt-2 text-2xl font-bold text-accent-2 tabular-nums">{logsResponse.summary.unique_clients.toLocaleString()}</div>
        <div class="mt-1 text-xs text-faint">Unique sources</div>
      </div>
      <div class="p-4">
        <div class="text-xs font-semibold uppercase tracking-[0.06em] text-muted">Non-NoError</div>
        <div class="mt-2 text-2xl font-bold text-warn-text tabular-nums">{logsResponse.summary.non_noerror.toLocaleString()}</div>
        <div class="mt-1 text-xs text-faint">Needs review</div>
      </div>
      <div class="p-4">
        <div class="text-xs font-semibold uppercase tracking-[0.06em] text-muted">Avg latency</div>
        <div class="mt-2 text-2xl font-bold text-success-text tabular-nums">{logsResponse.summary.avg_latency_ms}<span class="ml-0.5 text-sm font-semibold text-faint">ms</span></div>
        <div class="mt-1 text-xs text-faint">Resolver response</div>
      </div>
    </div>
  </section>
{/if}

<section class="rounded-md border border-line bg-surface shadow-card overflow-hidden flex flex-col" aria-label="DNS query logs">
  <div class="border-b border-line bg-panel">
    <div class="flex flex-col gap-4 p-4 lg:flex-row lg:items-center lg:justify-between">
      <div>
        <h2 class="text-base font-bold text-ink">Log explorer</h2>
        <p class="mt-1 text-sm text-muted">
          {logsResponse ? `${logsResponse.total_items.toLocaleString()} records across ${logsResponse.total_pages.toLocaleString()} pages` : 'Waiting for resolver records'}
        </p>
      </div>

      <div class="flex flex-col gap-2 sm:flex-row sm:items-center">
        <label class="inline-flex items-center justify-between gap-3 rounded-md border border-line bg-surface px-3 py-2 sm:justify-start">
          <span class="flex items-center gap-2 text-sm font-medium text-muted">
            {#if autoRefresh}<span class="w-1.5 h-1.5 rounded-full bg-success-2 animate-soft-pulse"></span>{/if}
            Auto refresh
          </span>
          <input type="checkbox" bind:checked={autoRefresh} class="sr-only peer" />
          <span class="relative h-6 w-11 rounded-full bg-line-2 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-accent peer-checked:bg-accent after:absolute after:start-[2px] after:top-[2px] after:h-5 after:w-5 after:rounded-full after:border after:border-line-2 after:bg-white after:transition-all after:content-[''] peer-checked:after:translate-x-full peer-checked:after:border-white"></span>
        </label>
        <button
          onclick={() => fetchLogs(1, searchQuery)}
          class="inline-flex justify-center items-center gap-1.5 px-3 py-2 border border-accent/25 text-sm font-semibold rounded-md text-accent-2 bg-accent-soft hover:bg-accent-fill hover:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-panel focus-visible:ring-accent"
        >
          <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
          Refresh
        </button>
        <button
          onclick={clearLogs}
          class="inline-flex justify-center items-center gap-1.5 px-3 py-2 border border-danger/20 text-sm font-semibold rounded-md text-danger-text bg-danger-bg hover:border-danger/35 focus:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-panel focus-visible:ring-danger"
        >
          <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
          Clear logs
        </button>
      </div>
    </div>

    <div class="grid gap-3 border-t border-line p-4 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-end">
      <form onsubmit={handleSearchSubmit} class="relative">
        <label for="log-search" class="mb-1.5 block text-xs font-semibold uppercase tracking-[0.08em] text-accent-2">Search</label>
        <div class="relative">
          <div class="absolute inset-y-0 left-0 pl-3.5 flex items-center pointer-events-none">
            <svg class="h-5 w-5 text-accent-2" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
              <path fill-rule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z" clip-rule="evenodd" />
            </svg>
          </div>
          <input
            id="log-search"
            type="text"
            bind:value={searchQuery}
            placeholder="Domain, IP, record type, status"
            class="block w-full pl-11 pr-3 py-2.5 border border-line rounded-md bg-surface placeholder-faint focus:outline-none focus:ring-2 focus:ring-accent focus:border-accent sm:text-sm"
          />
        </div>
      </form>

      <label class="block min-w-36">
        <span class="mb-1.5 block text-xs font-semibold uppercase tracking-[0.08em] text-muted">Rows</span>
        <select
          bind:value={pageSize}
          onchange={handlePageSizeChange}
          class="block w-full rounded-md border border-line bg-surface px-3 py-2.5 text-sm font-medium text-ink focus:outline-none focus:ring-2 focus:ring-accent focus:border-accent"
        >
          <option value={25}>25 per page</option>
          <option value={50}>50 per page</option>
          <option value={100}>100 per page</option>
        </select>
      </label>
    </div>
  </div>

  <div class="overflow-hidden">
    <table class="w-full table-fixed divide-y divide-line text-xs sm:text-sm">
      <colgroup>
        <col class="w-[10%] sm:w-[10%]" />
        <col class="w-[20%] sm:w-[20%]" />
        <col class="w-[36%] sm:w-[38%]" />
        <col class="w-[21%] sm:w-[13%]" />
        <col class="hidden sm:table-column sm:w-[8%]" />
        <col class="w-[13%] sm:w-[11%]" />
      </colgroup>
      <thead class="bg-accent-soft/45 sticky top-0 z-10">
        <tr>
          <th scope="col" class="px-1 sm:px-3 lg:px-4 py-3 text-left text-[10px] sm:text-[11px] font-semibold text-muted uppercase tracking-[0.04em]">Age</th>
          <th scope="col" class="px-1 sm:px-3 lg:px-4 py-3 text-left text-[10px] sm:text-[11px] font-semibold text-muted uppercase tracking-[0.04em]">Client</th>
          <th scope="col" class="px-1 sm:px-3 lg:px-4 py-3 text-left text-[10px] sm:text-[11px] font-semibold text-muted uppercase tracking-[0.04em]">Query</th>
          <th scope="col" class="px-1 sm:px-3 lg:px-4 py-3 text-left text-[10px] sm:text-[11px] font-semibold text-muted uppercase tracking-[0.04em]">Rcode</th>
          <th scope="col" class="hidden px-1 py-3 text-right text-[10px] font-semibold text-muted uppercase tracking-[0.04em] sm:table-cell sm:px-3 sm:text-[11px] lg:px-4">TTL</th>
          <th scope="col" class="px-1 sm:px-3 lg:px-4 py-3 text-right text-[10px] sm:text-[11px] font-semibold text-muted uppercase tracking-[0.04em]">
            <span class="hidden sm:inline">Latency</span><span class="sm:hidden">ms</span>
          </th>
        </tr>
      </thead>
      <tbody class="divide-y divide-line/60">
        {#if loading && !logsResponse}
          {#each Array(8) as _, i}
            <tr>
              <td class="px-1 sm:px-3 lg:px-4 py-4"><div class="skeleton h-4 rounded w-full"></div></td>
              <td class="px-1 sm:px-3 lg:px-4 py-4"><div class="skeleton h-4 rounded w-full"></div></td>
              <td class="px-1 sm:px-3 lg:px-4 py-4"><div class="skeleton h-4 rounded w-full"></div></td>
              <td class="px-1 sm:px-3 lg:px-4 py-4"><div class="skeleton h-5 rounded-full w-full"></div></td>
              <td class="hidden px-1 py-4 text-right sm:table-cell sm:px-3 lg:px-4"><div class="skeleton h-4 rounded w-full"></div></td>
              <td class="px-1 sm:px-3 lg:px-4 py-4 text-right"><div class="skeleton h-4 rounded w-full"></div></td>
            </tr>
          {/each}
        {:else if logsResponse?.items.length === 0}
          <tr><td colspan="6" class="px-6 py-16 text-center">
            <div class="inline-flex flex-col items-center gap-3 text-faint">
              <div class="w-14 h-14 rounded-md bg-accent-soft border border-accent/20 text-accent-2 flex items-center justify-center">
                <svg class="w-7 h-7" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5"><path stroke-linecap="round" stroke-linejoin="round" d="M9 17v-2a4 4 0 014-4h2m4-4H5a2 2 0 00-2 2v12a2 2 0 002 2h14a2 2 0 002-2V7a2 2 0 00-2-2z" opacity=".4"/><path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-4.35-4.35M11 18a7 7 0 100-14 7 7 0 000 14z"/></svg>
              </div>
              <span class="text-sm font-medium">No logs found</span>
            </div>
          </td></tr>
        {:else if logsResponse}
          {#each logsResponse.items as item (item.id)}
            <tr
              class="group hover:bg-accent-soft/60 cursor-pointer"
              onclick={() => { selectedLog = item; }}
            >
              <td class="px-1 sm:px-3 lg:px-4 py-3 align-top text-faint leading-5 break-all" title={new Date(item.ts_unix_ms).toLocaleString()}>
                {formatLogAge(item.ts_unix_ms)}
              </td>
              <td class="px-1 sm:px-3 lg:px-4 py-3 align-top text-ink font-medium whitespace-normal break-all leading-5" title={`${item.client_ip} (${formatProtocol(item.protocol)})`}>
                {item.client_ip} <span class="text-faint text-[11px] font-normal whitespace-nowrap">({formatProtocol(item.protocol)})</span>
              </td>
              <td class="px-1 sm:px-3 lg:px-4 py-3 align-top whitespace-normal" title={`${item.qtype} ${item.qname}`}>
                <div class="flex min-w-0 flex-col gap-1 leading-5 sm:flex-row sm:items-start sm:gap-2">
                  <span class="inline-flex w-fit shrink-0 items-center px-1.5 py-0.5 rounded-md text-[11px] font-bold bg-accent-soft text-accent-2 border border-accent/20">
                    {item.qtype}
                  </span>
                  <span class="min-w-0 break-all font-medium text-ink">{item.qname}</span>
                </div>
              </td>
              <td class="px-1 sm:px-3 lg:px-4 py-3 align-top">
                <span class="inline-flex max-w-full items-center gap-1 whitespace-nowrap px-1 py-0.5 rounded-full text-[10px] font-semibold leading-4 sm:px-1.5 sm:text-[11px] {item.rcode.toLowerCase() === 'noerror' ? 'bg-success-bg text-success-text ring-1 ring-inset ring-success/20' : 'bg-danger-bg text-danger-text ring-1 ring-inset ring-danger/10'}">
                  <span class="hidden h-1.5 w-1.5 rounded-full sm:inline-block {item.rcode.toLowerCase() === 'noerror' ? 'bg-success-2' : 'bg-danger-2'}"></span>
                  {item.rcode}
                </span>
              </td>
              <td class="hidden px-1 py-3 align-top break-all text-right font-medium text-ink tabular-nums leading-5 sm:table-cell sm:px-3 lg:px-4">
                {#if item.answer_ttl > 0}
                  {item.answer_ttl}<span class="text-faint text-[11px] font-normal ml-0.5">s</span>
                {:else}
                  <span class="text-faint">—</span>
                {/if}
              </td>
              <td class="px-1 sm:px-3 lg:px-4 py-3 align-top break-all text-right font-medium text-ink tabular-nums leading-5">
                {item.latency_ms}<span class="text-faint text-[11px] font-normal ml-0.5">ms</span>
              </td>
            </tr>
          {/each}
        {/if}
      </tbody>
    </table>
  </div>

  {#if logsResponse && logsResponse.total_pages > 1}
    <div class="bg-panel px-4 py-3 border-t border-line flex items-center justify-between sm:px-6">
      <div class="flex-1 flex justify-between sm:hidden">
        <button
          disabled={page === 1}
          onclick={() => fetchLogs(page - 1, searchQuery)}
          class="relative inline-flex items-center px-4 py-2 border border-line text-sm font-medium rounded-md text-ink bg-surface hover:bg-panel disabled:opacity-40 disabled:cursor-not-allowed"
        >
          Previous
        </button>
        <button
          disabled={page >= logsResponse.total_pages}
          onclick={() => fetchLogs(page + 1, searchQuery)}
          class="ml-3 relative inline-flex items-center px-4 py-2 border border-line text-sm font-medium rounded-md text-ink bg-surface hover:bg-panel disabled:opacity-40 disabled:cursor-not-allowed"
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
          <nav class="relative z-0 inline-flex rounded-md shadow-soft -space-x-px" aria-label="Pagination">
            <button
              onclick={() => fetchLogs(1, searchQuery)}
              class="relative inline-flex items-center px-3 py-2 rounded-l-md border border-line bg-surface text-sm font-medium text-muted hover:bg-panel transition-colors"
            >
              First
            </button>
            {#if page > 2}
              <button onclick={() => fetchLogs(page - 2, searchQuery)} class="relative inline-flex items-center px-3 py-2 border border-line bg-surface text-sm font-medium text-muted hover:bg-panel transition-colors">{page - 2}</button>
            {/if}
            {#if page > 1}
              <button onclick={() => fetchLogs(page - 1, searchQuery)} class="relative inline-flex items-center px-3 py-2 border border-line bg-surface text-sm font-medium text-muted hover:bg-panel transition-colors">{page - 1}</button>
            {/if}
            <button class="relative inline-flex items-center px-3.5 py-2 border border-transparent bg-accent-fill text-white text-sm font-bold z-10 cursor-default">{page}</button>
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
              class="relative inline-flex items-center px-3 py-2 rounded-r-md border border-line bg-surface text-sm font-medium text-muted hover:bg-panel transition-colors"
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
            <input type="number" min="1" max={logsResponse.total_pages} bind:value={pageInput} placeholder="Page" class="block w-16 px-2 py-1.5 text-sm border border-line bg-surface text-ink rounded-l-md focus:ring-accent focus:border-accent placeholder-faint" />
            <button type="submit" class="inline-flex items-center px-3 py-1.5 border border-l-0 border-line rounded-r-md bg-panel text-muted text-sm font-medium hover:bg-line transition-colors">Go</button>
          </form>
        </div>
      </div>
    </div>
  {/if}
</section>

{#if selectedLog}
  <div class="fixed inset-0 z-50 overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true" transition:fade={{ duration: 150 }}>
    <div class="flex items-end justify-center min-h-screen pt-4 px-2 pb-4 text-left sm:block sm:p-0 sm:text-center">
      <div class="fixed inset-0 z-0 bg-ink/70" aria-hidden="true" onclick={() => (selectedLog = null)}></div>
      <span class="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>
      <div class="relative z-10 inline-block w-full align-bottom bg-surface rounded-md text-left overflow-hidden shadow-lift transform transition-all sm:my-8 sm:align-middle sm:max-w-2xl sm:w-full border border-line" transition:slide={{ duration: 200 }}>
        <div class="bg-header text-header-text px-4 pt-5 pb-5 sm:px-6 relative overflow-hidden">
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
          <div class="flex flex-col gap-3 mb-4 text-sm bg-panel p-4 rounded-md border border-line">
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

          <div class="mt-4 border border-line/60 rounded-md overflow-hidden">
            <table class="min-w-full divide-y divide-line/60">
              <thead class="bg-panel">
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
        <div class="bg-panel px-4 py-3 sm:px-6 sm:flex sm:flex-row-reverse border-t border-line">
          <button type="button" class="mt-3 w-full inline-flex justify-center rounded-md border border-line shadow-soft px-4 py-2 bg-surface text-base font-medium text-ink hover:bg-panel focus:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-surface focus-visible:ring-accent sm:mt-0 sm:ml-3 sm:w-auto sm:text-sm transition-colors" onclick={() => (selectedLog = null)}>Close</button>
        </div>
      </div>
    </div>
  </div>
{/if}
