<script lang="ts">
  import { onMount } from 'svelte';
  import type { DnsLogEntry, LogSummary, PaginatedLogsResponse } from '../types/dashboard';
  import { formatProtocol, formatRelativeTime } from '../utils/dashboard';
  import ErrorAlert from './ErrorAlert.svelte';
  import SlowQueriesCard from './SlowQueriesCard.svelte';
  import QueryDetailModal from './QueryDetailModal.svelte';

  let { onReady = () => {} } = $props<{ onReady?: () => void }>();

  const emptySummary: LogSummary = {
    total_items: 0,
    unique_clients: 0,
    non_noerror: 0,
    avg_latency_ms: 0,
  };

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
  let logSummary = $derived(logsResponse?.summary ?? emptySummary);
  let logsLoaded = $state(false);
  let slowQueriesLoaded = $state(false);

  function notifyReady() {
    if (logsLoaded && slowQueriesLoaded) onReady();
  }

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
      logsLoaded = true;
      notifyReady();
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

<div class="space-y-4">
  <section class="stat-row" aria-label="Query summary">
    <div class="stat">
      <div class="stat-label">Queries</div>
      <div class="stat-value">{logSummary.total_items.toLocaleString()}</div>
      <div class="stat-hint">Retention window</div>
    </div>
    <div class="stat">
      <div class="stat-label">Clients</div>
      <div class="stat-value">{logSummary.unique_clients.toLocaleString()}</div>
      <div class="stat-hint">Unique sources</div>
    </div>
    <div class="stat {logSummary.non_noerror > 0 ? 'stat-danger' : ''}">
      <div class="stat-label">Non-NoError</div>
      <div class="stat-value {logSummary.non_noerror > 0 ? 'text-warn-text' : ''}">{logSummary.non_noerror.toLocaleString()}</div>
      <div class="stat-hint">Needs review</div>
    </div>
    <div class="stat">
      <div class="stat-label">Avg latency</div>
      <div class="stat-value">{logSummary.avg_latency_ms}<span class="ml-0.5 text-sm font-medium text-faint">ms</span></div>
      <div class="stat-hint">Response time</div>
    </div>
  </section>

  <SlowQueriesCard
    onSelectLog={(log) => (selectedLog = log)}
    onReady={() => {
      slowQueriesLoaded = true;
      notifyReady();
    }}
  />

  <section class="panel overflow-hidden" aria-label="DNS query logs">
    <div class="panel-head space-y-3 p-4">
      <div class="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <div>
          <h2 class="text-sm font-semibold text-ink">Log explorer</h2>
          <p class="mt-0.5 text-xs text-muted sm:text-sm">
            {logsResponse
              ? `${logsResponse.total_items.toLocaleString()} records · ${logsResponse.total_pages.toLocaleString()} pages`
              : 'Waiting for records'}
          </p>
        </div>
        <div class="flex flex-wrap items-center gap-2">
          <label class="inline-flex items-center gap-2 rounded-lg border border-line bg-surface px-3 py-1.5 text-sm text-muted">
            {#if autoRefresh}<span class="h-1.5 w-1.5 rounded-full bg-success-2 animate-soft-pulse"></span>{/if}
            Auto refresh
            <input type="checkbox" bind:checked={autoRefresh} class="sr-only" />
            <span class="toggle-track"></span>
          </label>
          <button onclick={() => fetchLogs(1, searchQuery)} class="btn btn-secondary">Refresh</button>
          <button onclick={clearLogs} class="btn btn-danger">Clear</button>
        </div>
      </div>

      <div class="grid gap-2 sm:grid-cols-[minmax(0,1fr)_8.5rem]">
        <form onsubmit={handleSearchSubmit}>
          <label for="log-search" class="sr-only">Search</label>
          <div class="relative">
            <svg class="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-faint" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
              <path fill-rule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z" clip-rule="evenodd" />
            </svg>
            <input
              id="log-search"
              type="search"
              bind:value={searchQuery}
              placeholder="Search domain, IP, type, status…"
              class="field pl-9"
            />
          </div>
        </form>
        <label class="block">
          <span class="sr-only">Rows per page</span>
          <select bind:value={pageSize} onchange={handlePageSizeChange} class="field">
            <option value={25}>25 / page</option>
            <option value={50}>50 / page</option>
            <option value={100}>100 / page</option>
          </select>
        </label>
      </div>
    </div>

    <div class="overflow-hidden">
      <table class="data-table text-xs sm:text-sm">
        <colgroup>
          <col class="w-[4.5rem] sm:w-[5.5rem]" />
          <col class="w-[7.5rem] sm:w-[10rem]" />
          <col />
          <col class="w-[5.75rem] sm:w-[6.5rem]" />
          <col class="hidden w-[4.5rem] sm:table-column" />
          <col class="w-[3.5rem] sm:w-[4rem]" />
        </colgroup>
        <thead>
          <tr>
            <th scope="col">Age</th>
            <th scope="col">Client</th>
            <th scope="col">Query</th>
            <th scope="col">Rcode</th>
            <th scope="col" class="hidden sm:table-cell !text-right">TTL</th>
            <th scope="col" class="!text-right">ms</th>
          </tr>
        </thead>
        <tbody>
          {#if loading && !logsResponse}
            {#each Array(8) as _}
              <tr>
                <td><div class="skeleton h-4 w-12 rounded"></div></td>
                <td><div class="skeleton h-4 w-20 rounded"></div></td>
                <td><div class="skeleton h-4 w-3/4 max-w-xs rounded"></div></td>
                <td><div class="skeleton h-5 w-16 rounded-full"></div></td>
                <td class="hidden sm:table-cell"><div class="skeleton ml-auto h-4 w-10 rounded"></div></td>
                <td><div class="skeleton ml-auto h-4 w-8 rounded"></div></td>
              </tr>
            {/each}
          {:else if logsResponse?.items.length === 0}
            <tr>
              <td colspan="6" class="!overflow-visible !whitespace-normal !py-16 text-center">
                <div class="empty-state">
                  <div class="empty-state-icon">
                    <svg class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.75"><path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-4.35-4.35M11 18a7 7 0 100-14 7 7 0 000 14z"/></svg>
                  </div>
                  <span class="text-sm font-medium">No logs found</span>
                  {#if searchQuery}
                    <button onclick={() => { searchQuery = ''; fetchLogs(1, ''); }} class="text-sm font-semibold text-accent-2 hover:underline">Clear search</button>
                  {/if}
                </div>
              </td>
            </tr>
          {:else if logsResponse}
            {#each logsResponse.items as item (item.id)}
              <tr class="row-interactive" onclick={() => { selectedLog = item; }}>
                <td class="font-mono text-[11px] text-faint sm:text-xs" title={new Date(item.ts_unix_ms).toLocaleString()}>
                  <span class="cell-clip">{formatLogAge(item.ts_unix_ms)}</span>
                </td>
                <td class="font-mono text-[11px] font-medium text-ink sm:text-xs" title={`${item.client_ip} (${formatProtocol(item.protocol)})`}>
                  <span class="cell-clip">
                    {item.client_ip}
                    <span class="text-[10px] font-normal text-faint">({formatProtocol(item.protocol)})</span>
                  </span>
                </td>
                <td title={`${item.qtype} ${item.qname}`}>
                  <div class="cell-clip-inline">
                    <span class="chip chip-accent shrink-0 font-mono">{item.qtype}</span>
                    <span class="cell-clip-text font-medium text-ink">{item.qname}</span>
                  </div>
                </td>
                <td>
                  <span class="chip {item.rcode.toLowerCase() === 'noerror' ? 'chip-success' : 'chip-danger'}">
                    {item.rcode}
                  </span>
                </td>
                <td class="hidden text-right font-mono font-medium text-ink tabular-nums sm:table-cell">
                  {#if item.answer_ttl > 0}
                    {item.answer_ttl}<span class="text-[10px] font-normal text-faint">s</span>
                  {:else}
                    <span class="text-faint">—</span>
                  {/if}
                </td>
                <td class="text-right font-mono font-medium text-ink tabular-nums">
                  {item.latency_ms}
                </td>
              </tr>
            {/each}
          {/if}
        </tbody>
      </table>
    </div>

    {#if logsResponse && logsResponse.total_pages > 1}
      <div class="flex items-center justify-between border-t border-line px-4 py-3">
        <div class="flex flex-1 justify-between sm:hidden">
          <button disabled={page === 1} onclick={() => fetchLogs(page - 1, searchQuery)} class="btn btn-secondary disabled:opacity-40">Previous</button>
          <button disabled={page >= logsResponse.total_pages} onclick={() => fetchLogs(page + 1, searchQuery)} class="btn btn-secondary disabled:opacity-40">Next</button>
        </div>
        <div class="hidden sm:flex sm:flex-1 sm:items-center sm:justify-between">
          <p class="text-sm text-muted">
            Page <span class="font-semibold text-ink tabular-nums">{logsResponse.page}</span>
            of <span class="font-semibold text-ink tabular-nums">{logsResponse.total_pages}</span>
          </p>
          <div class="flex items-center gap-2">
            <nav class="inline-flex -space-x-px rounded-lg shadow-soft" aria-label="Pagination">
              <button onclick={() => fetchLogs(1, searchQuery)} class="rounded-l-lg border border-line bg-surface px-3 py-1.5 text-sm text-muted hover:bg-panel">First</button>
              {#if page > 2}
                <button onclick={() => fetchLogs(page - 2, searchQuery)} class="border border-line bg-surface px-3 py-1.5 text-sm text-muted tabular-nums hover:bg-panel">{page - 2}</button>
              {/if}
              {#if page > 1}
                <button onclick={() => fetchLogs(page - 1, searchQuery)} class="border border-line bg-surface px-3 py-1.5 text-sm text-muted tabular-nums hover:bg-panel">{page - 1}</button>
              {/if}
              <button class="border border-transparent bg-accent-fill px-3 py-1.5 text-sm font-semibold text-on-accent tabular-nums">{page}</button>
              {#if page < logsResponse.total_pages}
                <button onclick={() => fetchLogs(page + 1, searchQuery)} class="border border-line bg-surface px-3 py-1.5 text-sm text-muted tabular-nums hover:bg-panel">{page + 1}</button>
              {/if}
              {#if page < logsResponse.total_pages - 1}
                <button onclick={() => fetchLogs(page + 2, searchQuery)} class="border border-line bg-surface px-3 py-1.5 text-sm text-muted tabular-nums hover:bg-panel">{page + 2}</button>
              {/if}
              {#if page < logsResponse.total_pages - 2}
                <span class="border border-line bg-surface px-3 py-1.5 text-sm text-faint">…</span>
              {/if}
              <button onclick={() => fetchLogs(logsResponse.total_pages, searchQuery)} class="rounded-r-lg border border-line bg-surface px-3 py-1.5 text-sm text-muted tabular-nums hover:bg-panel">{logsResponse.total_pages}</button>
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
              class="flex items-center"
            >
              <input type="number" min="1" max={logsResponse.total_pages} bind:value={pageInput} placeholder="Page" class="field w-16 rounded-r-none py-1.5 text-sm tabular-nums" />
              <button type="submit" class="rounded-r-lg border border-l-0 border-line bg-panel px-3 py-1.5 text-sm font-medium text-muted hover:bg-line">Go</button>
            </form>
          </div>
        </div>
      </div>
    {/if}
  </section>
</div>

<QueryDetailModal log={selectedLog} onClose={() => (selectedLog = null)} />
