<script lang="ts">
  import { onMount } from 'svelte';
  import { fade } from 'svelte/transition';
  import type { DnsLogEntry, PaginatedLogsResponse } from '../types/dashboard';
  import { formatProtocol, formatRelativeTime } from '../utils/dashboard';
  import ErrorAlert from './ErrorAlert.svelte';
  import SlowQueriesCard from './SlowQueriesCard.svelte';
  import QueryDetailModal from './QueryDetailModal.svelte';

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
  <section class="mb-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-4" aria-label="Query summary" transition:fade>
    <!-- Primary metric: total queries, on the dark header surface for emphasis. -->
    <div class="relative overflow-hidden rounded-xl bg-header p-4 text-header-text shadow-card">
      <div class="pointer-events-none absolute -right-6 -top-8 h-24 w-24 rounded-full bg-[radial-gradient(circle,color-mix(in_srgb,var(--ui-accent)_60%,transparent),transparent_70%)]" aria-hidden="true"></div>
      <div class="relative flex items-center gap-1.5 text-[11px] font-semibold uppercase tracking-[0.08em] text-accent-3">
        <svg class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z" /></svg>
        Queries
      </div>
      <div class="relative mt-2 font-mono text-3xl font-semibold tracking-tight text-white tabular-nums">{logsResponse.summary.total_items.toLocaleString()}</div>
      <div class="relative mt-1 text-xs text-header-muted">Current retention window</div>
    </div>

    <div class="rounded-xl border border-line bg-surface p-4 shadow-soft">
      <div class="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted">Clients</div>
      <div class="mt-2 font-mono text-2xl font-semibold text-ink tabular-nums">{logsResponse.summary.unique_clients.toLocaleString()}</div>
      <div class="mt-1 text-xs text-faint">Unique sources</div>
    </div>

    <div class="rounded-xl border border-line bg-surface p-4 shadow-soft">
      <div class="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted">Non-NoError</div>
      <div class="mt-2 font-mono text-2xl font-semibold {logsResponse.summary.non_noerror > 0 ? 'text-warn-text' : 'text-ink'} tabular-nums">{logsResponse.summary.non_noerror.toLocaleString()}</div>
      <div class="mt-1 text-xs text-faint">Responses needing review</div>
    </div>

    <div class="rounded-xl border border-line bg-surface p-4 shadow-soft">
      <div class="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted">Avg latency</div>
      <div class="mt-2 font-mono text-2xl font-semibold text-ink tabular-nums">{logsResponse.summary.avg_latency_ms}<span class="ml-0.5 text-sm font-medium text-faint">ms</span></div>
      <div class="mt-1 text-xs text-faint">Resolver response</div>
    </div>
  </section>
{/if}

<SlowQueriesCard onSelectLog={(log) => (selectedLog = log)} />

<section class="flex flex-col overflow-hidden rounded-xl border border-line bg-surface shadow-card" aria-label="DNS query logs">
  <div class="border-b border-line bg-panel">
    <div class="flex flex-col gap-4 p-4 lg:flex-row lg:items-center lg:justify-between">
      <div>
        <h2 class="text-base font-bold text-ink">Log explorer</h2>
        <p class="mt-1 text-sm text-muted">
          {logsResponse ? `${logsResponse.total_items.toLocaleString()} records across ${logsResponse.total_pages.toLocaleString()} pages` : 'Waiting for resolver records'}
        </p>
      </div>

      <div class="flex flex-col gap-2 sm:flex-row sm:items-center">
        <label class="inline-flex items-center justify-between gap-3 rounded-lg border border-line bg-surface px-3 py-2 sm:justify-start">
          <span class="flex items-center gap-2 text-sm font-medium text-muted">
            {#if autoRefresh}<span class="h-1.5 w-1.5 rounded-full bg-success-2 animate-soft-pulse"></span>{/if}
            Auto refresh
          </span>
          <input type="checkbox" bind:checked={autoRefresh} class="peer sr-only" />
          <span class="relative h-6 w-11 rounded-full bg-line-2 after:absolute after:start-[2px] after:top-[2px] after:h-5 after:w-5 after:rounded-full after:border after:border-line-2 after:bg-white after:transition-all after:content-[''] peer-checked:bg-accent peer-checked:after:translate-x-full peer-checked:after:border-white peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-accent"></span>
        </label>
        <button
          onclick={() => fetchLogs(1, searchQuery)}
          class="inline-flex items-center justify-center gap-1.5 rounded-lg border border-accent/25 bg-accent-soft px-3 py-2 text-sm font-semibold text-accent-2 hover:bg-accent-fill hover:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-panel active:scale-[0.98]"
        >
          <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
          Refresh
        </button>
        <button
          onclick={clearLogs}
          class="inline-flex items-center justify-center gap-1.5 rounded-lg border border-danger/20 bg-danger-bg px-3 py-2 text-sm font-semibold text-danger-text hover:border-danger/35 focus:outline-none focus-visible:ring-2 focus-visible:ring-danger focus-visible:ring-offset-2 focus-visible:ring-offset-panel active:scale-[0.98]"
        >
          <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
          Clear logs
        </button>
      </div>
    </div>

    <div class="grid gap-3 border-t border-line p-4 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-end">
      <form onsubmit={handleSearchSubmit} class="relative">
        <label for="log-search" class="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.08em] text-muted">Search</label>
        <div class="relative">
          <div class="pointer-events-none absolute inset-y-0 left-0 flex items-center pl-3.5">
            <svg class="h-5 w-5 text-faint" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
              <path fill-rule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z" clip-rule="evenodd" />
            </svg>
          </div>
          <input
            id="log-search"
            type="text"
            bind:value={searchQuery}
            placeholder="Domain, IP, record type, status"
            class="block w-full rounded-lg border border-line bg-surface py-2.5 pl-11 pr-3 placeholder-faint focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent sm:text-sm"
          />
        </div>
      </form>

      <label class="block min-w-36">
        <span class="mb-1.5 block text-[11px] font-semibold uppercase tracking-[0.08em] text-muted">Rows</span>
        <select
          bind:value={pageSize}
          onchange={handlePageSizeChange}
          class="block w-full rounded-lg border border-line bg-surface px-3 py-2.5 text-sm font-medium text-ink focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent"
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
      <thead class="sticky top-0 z-10 bg-panel/95 backdrop-blur">
        <tr>
          <th scope="col" class="px-1 py-3 text-left text-[10px] font-semibold uppercase tracking-[0.05em] text-faint sm:px-3 sm:text-[11px] lg:px-4">Age</th>
          <th scope="col" class="px-1 py-3 text-left text-[10px] font-semibold uppercase tracking-[0.05em] text-faint sm:px-3 sm:text-[11px] lg:px-4">Client</th>
          <th scope="col" class="px-1 py-3 text-left text-[10px] font-semibold uppercase tracking-[0.05em] text-faint sm:px-3 sm:text-[11px] lg:px-4">Query</th>
          <th scope="col" class="px-1 py-3 text-left text-[10px] font-semibold uppercase tracking-[0.05em] text-faint sm:px-3 sm:text-[11px] lg:px-4">Rcode</th>
          <th scope="col" class="hidden px-1 py-3 text-right text-[10px] font-semibold uppercase tracking-[0.05em] text-faint sm:table-cell sm:px-3 sm:text-[11px] lg:px-4">TTL</th>
          <th scope="col" class="px-1 py-3 text-right text-[10px] font-semibold uppercase tracking-[0.05em] text-faint sm:px-3 sm:text-[11px] lg:px-4">
            <span class="hidden sm:inline">Latency</span><span class="sm:hidden">ms</span>
          </th>
        </tr>
      </thead>
      <tbody class="divide-y divide-line/60">
        {#if loading && !logsResponse}
          {#each Array(8) as _, i}
            <tr>
              <td class="px-1 py-4 sm:px-3 lg:px-4"><div class="skeleton h-4 w-full rounded"></div></td>
              <td class="px-1 py-4 sm:px-3 lg:px-4"><div class="skeleton h-4 w-full rounded"></div></td>
              <td class="px-1 py-4 sm:px-3 lg:px-4"><div class="skeleton h-4 w-full rounded"></div></td>
              <td class="px-1 py-4 sm:px-3 lg:px-4"><div class="skeleton h-5 w-full rounded-full"></div></td>
              <td class="hidden px-1 py-4 text-right sm:table-cell sm:px-3 lg:px-4"><div class="skeleton h-4 w-full rounded"></div></td>
              <td class="px-1 py-4 text-right sm:px-3 lg:px-4"><div class="skeleton h-4 w-full rounded"></div></td>
            </tr>
          {/each}
        {:else if logsResponse?.items.length === 0}
          <tr><td colspan="6" class="px-6 py-16 text-center">
            <div class="inline-flex flex-col items-center gap-3 text-faint">
              <div class="flex h-14 w-14 items-center justify-center rounded-xl border border-accent/20 bg-accent-soft text-accent-2">
                <svg class="h-7 w-7" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5"><path stroke-linecap="round" stroke-linejoin="round" d="M9 17v-2a4 4 0 014-4h2m4-4H5a2 2 0 00-2 2v12a2 2 0 002 2h14a2 2 0 002-2V7a2 2 0 00-2-2z" opacity=".4"/><path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-4.35-4.35M11 18a7 7 0 100-14 7 7 0 000 14z"/></svg>
              </div>
              <span class="text-sm font-medium">No logs found</span>
              {#if searchQuery}
                <button onclick={() => { searchQuery = ''; fetchLogs(1, ''); }} class="text-sm font-semibold text-accent-2 hover:underline">Clear search</button>
              {/if}
            </div>
          </td></tr>
        {:else if logsResponse}
          {#each logsResponse.items as item (item.id)}
            <tr
              class="group cursor-pointer hover:bg-accent-soft/60"
              onclick={() => { selectedLog = item; }}
            >
              <td class="break-all px-1 py-3 align-top font-mono text-[11px] leading-5 text-faint sm:px-3 sm:text-xs lg:px-4" title={new Date(item.ts_unix_ms).toLocaleString()}>
                {formatLogAge(item.ts_unix_ms)}
              </td>
              <td class="whitespace-normal break-all px-1 py-3 align-top font-mono text-[11px] font-medium leading-5 text-ink sm:px-3 sm:text-xs lg:px-4" title={`${item.client_ip} (${formatProtocol(item.protocol)})`}>
                {item.client_ip} <span class="whitespace-nowrap text-[10px] font-normal text-faint">({formatProtocol(item.protocol)})</span>
              </td>
              <td class="whitespace-normal px-1 py-3 align-top sm:px-3 lg:px-4" title={`${item.qtype} ${item.qname}`}>
                <div class="flex min-w-0 flex-col gap-1 leading-5 sm:flex-row sm:items-start sm:gap-2">
                  <span class="inline-flex w-fit shrink-0 items-center rounded-md border border-accent/20 bg-accent-soft px-1.5 py-0.5 font-mono text-[10px] font-semibold text-accent-2">
                    {item.qtype}
                  </span>
                  <span class="min-w-0 break-all font-medium text-ink">{item.qname}</span>
                </div>
              </td>
              <td class="px-1 py-3 align-top sm:px-3 lg:px-4">
                <span class="inline-flex max-w-full items-center gap-1 whitespace-nowrap rounded-md px-1 py-0.5 text-[10px] font-semibold leading-4 sm:px-1.5 sm:text-[11px] {item.rcode.toLowerCase() === 'noerror' ? 'bg-success-bg text-success-text ring-1 ring-inset ring-success/20' : 'bg-danger-bg text-danger-text ring-1 ring-inset ring-danger/15'}">
                  <span class="hidden h-1.5 w-1.5 rounded-full sm:inline-block {item.rcode.toLowerCase() === 'noerror' ? 'bg-success-2' : 'bg-danger-2'}"></span>
                  {item.rcode}
                </span>
              </td>
              <td class="hidden break-all px-1 py-3 text-right align-top font-mono font-medium leading-5 text-ink tabular-nums sm:table-cell sm:px-3 lg:px-4">
                {#if item.answer_ttl > 0}
                  {item.answer_ttl}<span class="ml-0.5 text-[10px] font-normal text-faint">s</span>
                {:else}
                  <span class="text-faint">-</span>
                {/if}
              </td>
              <td class="break-all px-1 py-3 text-right align-top font-mono font-medium leading-5 text-ink tabular-nums sm:px-3 lg:px-4">
                {item.latency_ms}<span class="ml-0.5 text-[10px] font-normal text-faint">ms</span>
              </td>
            </tr>
          {/each}
        {/if}
      </tbody>
    </table>
  </div>

  {#if logsResponse && logsResponse.total_pages > 1}
    <div class="flex items-center justify-between border-t border-line bg-panel px-4 py-3 sm:px-6">
      <div class="flex flex-1 justify-between sm:hidden">
        <button
          disabled={page === 1}
          onclick={() => fetchLogs(page - 1, searchQuery)}
          class="relative inline-flex items-center rounded-lg border border-line bg-surface px-4 py-2 text-sm font-medium text-ink hover:bg-panel disabled:cursor-not-allowed disabled:opacity-40"
        >
          Previous
        </button>
        <button
          disabled={page >= logsResponse.total_pages}
          onclick={() => fetchLogs(page + 1, searchQuery)}
          class="relative ml-3 inline-flex items-center rounded-lg border border-line bg-surface px-4 py-2 text-sm font-medium text-ink hover:bg-panel disabled:cursor-not-allowed disabled:opacity-40"
        >
          Next
        </button>
      </div>
      <div class="hidden sm:flex sm:flex-1 sm:items-center sm:justify-between">
        <div>
          <p class="text-sm text-muted">
            Page <span class="font-semibold text-ink tabular-nums">{logsResponse.page}</span> of <span class="font-semibold text-ink tabular-nums">{logsResponse.total_pages}</span>
          </p>
        </div>
        <div class="flex items-center gap-2">
          <nav class="relative z-0 inline-flex -space-x-px rounded-lg shadow-soft" aria-label="Pagination">
            <button
              onclick={() => fetchLogs(1, searchQuery)}
              class="relative inline-flex items-center rounded-l-lg border border-line bg-surface px-3 py-2 text-sm font-medium text-muted hover:bg-panel"
            >
              First
            </button>
            {#if page > 2}
              <button onclick={() => fetchLogs(page - 2, searchQuery)} class="relative inline-flex items-center border border-line bg-surface px-3 py-2 text-sm font-medium text-muted tabular-nums hover:bg-panel">{page - 2}</button>
            {/if}
            {#if page > 1}
              <button onclick={() => fetchLogs(page - 1, searchQuery)} class="relative inline-flex items-center border border-line bg-surface px-3 py-2 text-sm font-medium text-muted tabular-nums hover:bg-panel">{page - 1}</button>
            {/if}
            <button class="relative z-10 inline-flex cursor-default items-center border border-transparent bg-accent-fill px-3.5 py-2 text-sm font-bold text-white tabular-nums">{page}</button>
            {#if page < logsResponse.total_pages}
              <button onclick={() => fetchLogs(page + 1, searchQuery)} class="relative inline-flex items-center border border-line bg-surface px-3 py-2 text-sm font-medium text-muted tabular-nums hover:bg-panel">{page + 1}</button>
            {/if}
            {#if page < logsResponse.total_pages - 1}
              <button onclick={() => fetchLogs(page + 2, searchQuery)} class="relative inline-flex items-center border border-line bg-surface px-3 py-2 text-sm font-medium text-muted tabular-nums hover:bg-panel">{page + 2}</button>
            {/if}
            {#if page < logsResponse.total_pages - 2}
              <span class="relative inline-flex items-center border border-line bg-surface px-3 py-2 text-sm font-medium text-faint">...</span>
            {/if}
            <button
              onclick={() => fetchLogs(logsResponse.total_pages, searchQuery)}
              class="relative inline-flex items-center rounded-r-lg border border-line bg-surface px-3 py-2 text-sm font-medium text-muted tabular-nums hover:bg-panel"
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
            class="ml-2 flex items-center"
          >
            <input type="number" min="1" max={logsResponse.total_pages} bind:value={pageInput} placeholder="Page" class="block w-16 rounded-l-lg border border-line bg-surface px-2 py-1.5 text-sm text-ink tabular-nums placeholder-faint focus:border-accent focus:ring-accent" />
            <button type="submit" class="inline-flex items-center rounded-r-lg border border-l-0 border-line bg-panel px-3 py-1.5 text-sm font-medium text-muted hover:bg-line">Go</button>
          </form>
        </div>
      </div>
    </div>
  {/if}
</section>

<QueryDetailModal log={selectedLog} onClose={() => (selectedLog = null)} />
