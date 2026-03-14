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
    <div class="bg-white rounded-lg shadow-sm border border-gray-200 p-4 sm:p-5 flex flex-col border-t-4 border-t-navy-500">
      <span class="text-xs sm:text-sm font-semibold text-gray-500 uppercase tracking-wider mb-1">Total Queries</span>
      <span class="text-2xl sm:text-3xl font-bold text-navy-900">{logsResponse.summary.total_items.toLocaleString()}</span>
    </div>
    <div class="bg-white rounded-lg shadow-sm border border-gray-200 p-4 sm:p-5 flex flex-col border-t-4 border-t-navy-400">
      <span class="text-xs sm:text-sm font-semibold text-gray-500 uppercase tracking-wider mb-1">Unique Clients</span>
      <span class="text-2xl sm:text-3xl font-bold text-navy-900">{logsResponse.summary.unique_clients.toLocaleString()}</span>
    </div>
    <div class="bg-white rounded-lg shadow-sm border border-gray-200 p-4 sm:p-5 flex flex-col border-t-4 border-t-amber-500">
      <span class="text-xs sm:text-sm font-semibold text-gray-500 uppercase tracking-wider mb-1">Non-NoError</span>
      <span class="text-2xl sm:text-3xl font-bold text-navy-900">{logsResponse.summary.non_noerror.toLocaleString()}</span>
    </div>
    <div class="bg-white rounded-lg shadow-sm border border-gray-200 p-4 sm:p-5 flex flex-col border-t-4 border-t-navy-600">
      <span class="text-xs sm:text-sm font-semibold text-gray-500 uppercase tracking-wider mb-1">Avg Latency</span>
      <span class="text-2xl sm:text-3xl font-bold text-navy-900">{logsResponse.summary.avg_latency_ms} <span class="text-base sm:text-lg text-gray-400 font-normal">ms</span></span>
    </div>
  </div>
{/if}

<div class="bg-white shadow-sm rounded-lg border border-gray-200 overflow-hidden flex flex-col">
  <div class="p-4 border-b border-gray-200 flex flex-col md:flex-row justify-between items-center gap-4 bg-gray-50/50">
    <form onsubmit={handleSearchSubmit} class="w-full md:max-w-md relative">
      <div class="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
        <svg class="h-5 w-5 text-gray-400" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
          <path fill-rule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z" clip-rule="evenodd" />
        </svg>
      </div>
      <input
        type="text"
        bind:value={searchQuery}
        placeholder="Search domains, IPs, record types..."
        class="block w-full pl-10 pr-3 py-2 border border-gray-300 rounded-md leading-5 bg-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-navy-500 focus:border-navy-500 sm:text-sm transition-shadow shadow-sm"
      />
    </form>

    <div class="flex items-center gap-3 w-full md:w-auto">
      <label class="inline-flex items-center cursor-pointer mr-2">
        <input type="checkbox" bind:checked={autoRefresh} class="sr-only peer" />
        <div class="relative w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-navy-300 rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-navy-600"></div>
        <span class="ms-3 text-sm font-medium text-gray-700">Auto Refresh</span>
      </label>
      <button
        onclick={() => fetchLogs(1, searchQuery)}
        class="w-full md:w-auto inline-flex justify-center items-center px-4 py-2 border border-gray-300 shadow-sm text-sm font-medium rounded-md text-navy-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-navy-500 transition-colors"
      >
        Refresh
      </button>
      <button
        onclick={clearLogs}
        class="w-full md:w-auto inline-flex justify-center items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-navy-600 hover:bg-navy-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-navy-500 transition-colors"
      >
        Clear Logs
      </button>
    </div>
  </div>

  <div class="overflow-x-auto">
    <table class="min-w-full divide-y divide-gray-200 table-fixed">
      <thead class="bg-gray-50">
        <tr>
          <th scope="col" class="w-[15%] px-4 sm:px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Time</th>
          <th scope="col" class="w-[20%] px-4 sm:px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Client</th>
          <th scope="col" class="w-[40%] px-4 sm:px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Query</th>
          <th scope="col" class="w-[12%] px-4 sm:px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Status</th>
          <th scope="col" class="w-[13%] px-4 sm:px-6 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider">Latency</th>
        </tr>
      </thead>
      <tbody class="bg-white divide-y divide-gray-100 relative">
        {#if loading && !logsResponse}
          <tr><td colspan="5" class="px-6 py-12 text-center text-gray-500">Loading logs...</td></tr>
        {:else if logsResponse?.items.length === 0}
          <tr><td colspan="5" class="px-6 py-12 text-center text-gray-500">No logs found.</td></tr>
        {:else if logsResponse}
          {#each logsResponse.items as item (item.id)}
            <tr
              class="hover:bg-navy-50/50 transition-colors group cursor-pointer"
              in:fade={{ duration: 360 }}
              onclick={() => {
                selectedLog = item;
              }}
            >
              <td class="w-[15%] px-4 sm:px-6 py-4 whitespace-nowrap text-sm text-gray-500 truncate" title={new Date(item.ts_unix_ms).toLocaleString()}>
                {formatRelativeTime(item.ts_unix_ms)}
              </td>
              <td class="w-[20%] px-4 sm:px-6 py-4 whitespace-nowrap text-sm text-navy-900 font-medium truncate" title={`${item.client_ip} (${formatProtocol(item.protocol)})`}>
                {item.client_ip} <span class="text-gray-400 text-xs font-normal">({formatProtocol(item.protocol)})</span>
              </td>
              <td class="w-[40%] px-4 sm:px-6 py-4 whitespace-nowrap text-sm truncate" title={`${item.qtype} ${item.qname}`}>
                <div class="flex items-center gap-2 truncate">
                  <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-gray-100 text-gray-700 border border-gray-200 flex-shrink-0">
                    {item.qtype}
                  </span>
                  <span class="text-navy-900 font-medium truncate">{item.qname}</span>
                </div>
              </td>
              <td class="w-[12%] px-4 sm:px-6 py-4 whitespace-nowrap text-sm truncate">
                <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-semibold {item.rcode.toLowerCase() === 'noerror' ? 'bg-green-50 text-green-700 ring-1 ring-inset ring-green-600/20' : 'bg-red-50 text-red-700 ring-1 ring-inset ring-red-600/10'}">
                  {item.rcode}
                </span>
              </td>
              <td class="w-[13%] px-4 sm:px-6 py-4 whitespace-nowrap text-sm text-right font-medium text-navy-900 truncate">
                {item.latency_ms} <span class="text-gray-400 text-xs font-normal">ms</span>
              </td>
            </tr>
          {/each}
        {/if}
      </tbody>
    </table>
  </div>

  {#if logsResponse && logsResponse.total_pages > 1}
    <div class="bg-gray-50/50 px-4 py-3 border-t border-gray-200 flex items-center justify-between sm:px-6">
      <div class="flex-1 flex justify-between sm:hidden">
        <button
          disabled={page === 1}
          onclick={() => fetchLogs(page - 1, searchQuery)}
          class="relative inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-navy-700 bg-white hover:bg-gray-50 disabled:bg-gray-100 disabled:text-gray-400"
        >
          Previous
        </button>
        <button
          disabled={page >= logsResponse.total_pages}
          onclick={() => fetchLogs(page + 1, searchQuery)}
          class="ml-3 relative inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-navy-700 bg-white hover:bg-gray-50 disabled:bg-gray-100 disabled:text-gray-400"
        >
          Next
        </button>
      </div>
      <div class="hidden sm:flex-1 sm:flex sm:items-center sm:justify-between">
        <div>
          <p class="text-sm text-gray-700">
            Showing page <span class="font-medium text-navy-900">{logsResponse.page}</span> of <span class="font-medium text-navy-900">{logsResponse.total_pages}</span>
          </p>
        </div>
        <div class="flex items-center gap-2">
          <nav class="relative z-0 inline-flex rounded-md shadow-sm -space-x-px" aria-label="Pagination">
            <button
              onclick={() => fetchLogs(1, searchQuery)}
              class="relative inline-flex items-center px-3 py-2 rounded-l-md border border-gray-300 bg-white text-sm font-medium text-gray-500 hover:bg-gray-50 transition-colors"
            >
              First
            </button>
            {#if page > 2}
              <button
                onclick={() => fetchLogs(page - 2, searchQuery)}
                class="relative inline-flex items-center px-3 py-2 border border-gray-300 bg-white text-sm font-medium text-gray-500 hover:bg-gray-50 transition-colors"
              >
                {page - 2}
              </button>
            {/if}
            {#if page > 1}
              <button
                onclick={() => fetchLogs(page - 1, searchQuery)}
                class="relative inline-flex items-center px-3 py-2 border border-gray-300 bg-white text-sm font-medium text-gray-500 hover:bg-gray-50 transition-colors"
              >
                {page - 1}
              </button>
            {/if}
            <button class="relative inline-flex items-center px-3 py-2 border border-navy-500 bg-navy-50 text-sm font-bold text-navy-700 z-10 cursor-default">
              {page}
            </button>
            {#if page < logsResponse.total_pages}
              <button
                onclick={() => fetchLogs(page + 1, searchQuery)}
                class="relative inline-flex items-center px-3 py-2 border border-gray-300 bg-white text-sm font-medium text-gray-500 hover:bg-gray-50 transition-colors"
              >
                {page + 1}
              </button>
            {/if}
            {#if page < logsResponse.total_pages - 1}
              <button
                onclick={() => fetchLogs(page + 2, searchQuery)}
                class="relative inline-flex items-center px-3 py-2 border border-gray-300 bg-white text-sm font-medium text-gray-500 hover:bg-gray-50 transition-colors"
              >
                {page + 2}
              </button>
            {/if}
            {#if page < logsResponse.total_pages - 2}
              <span class="relative inline-flex items-center px-3 py-2 border border-gray-300 bg-white text-sm font-medium text-gray-500">...</span>
            {/if}
            <button
              onclick={() => fetchLogs(logsResponse.total_pages, searchQuery)}
              class="relative inline-flex items-center px-3 py-2 rounded-r-md border border-gray-300 bg-white text-sm font-medium text-gray-500 hover:bg-gray-50 transition-colors"
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
            <input
              type="number"
              min="1"
              max={logsResponse.total_pages}
              bind:value={pageInput}
              placeholder="Page"
              class="block w-16 px-2 py-1.5 text-sm border border-gray-300 rounded-l-md focus:ring-navy-500 focus:border-navy-500"
            />
            <button
              type="submit"
              class="inline-flex items-center px-3 py-1.5 border border-l-0 border-gray-300 rounded-r-md bg-gray-50 text-gray-700 text-sm font-medium hover:bg-gray-100 transition-colors"
            >
              Go
            </button>
          </form>
        </div>
      </div>
    </div>
  {/if}
</div>

{#if selectedLog}
  <div class="fixed inset-0 z-50 overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true" transition:fade={{ duration: 150 }}>
    <div class="flex items-end justify-center min-h-screen pt-4 px-2 pb-4 text-left sm:block sm:p-0 sm:text-center">
      <div class="fixed inset-0 z-0 bg-gray-900/50 transition-opacity backdrop-blur-sm" aria-hidden="true" onclick={() => (selectedLog = null)}></div>
      <span class="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>
      <div class="relative z-10 inline-block w-full align-bottom bg-white rounded-xl text-left overflow-hidden shadow-2xl transform transition-all sm:my-8 sm:align-middle sm:max-w-2xl sm:w-full border border-gray-200" transition:slide={{ duration: 200 }}>
        <div class="bg-white px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
          <div class="sm:flex sm:items-start">
            <div class="mt-3 text-left sm:mt-0 w-full">
              <h3 class="text-xl leading-6 font-bold text-navy-900 flex items-center gap-2" id="modal-title">
                Query Results
              </h3>
              <div class="mt-2 text-sm text-gray-500 mb-4 flex gap-2 items-center">
                <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-gray-100 text-gray-700 border border-gray-200">
                  {selectedLog?.qtype}
                </span>
                <span class="font-mono text-navy-900 truncate max-w-sm">{selectedLog?.qname}</span>
              </div>

              <div class="flex flex-col gap-3 mb-4 text-sm bg-gray-50 p-4 rounded-lg border border-gray-100">
                <div class="flex flex-col sm:flex-row sm:justify-between sm:items-center border-b border-gray-200 pb-2">
                  <div class="text-gray-500 font-medium mb-1 sm:mb-0">Time</div>
                  <div class="font-medium text-navy-900 text-left sm:text-right">{selectedLog ? new Date(selectedLog.ts_unix_ms).toLocaleString() : '-'}</div>
                </div>
                <div class="flex flex-col sm:flex-row sm:justify-between sm:items-center border-b border-gray-200 pb-2">
                  <div class="text-gray-500 font-medium mb-1 sm:mb-0">Client</div>
                  <div class="text-left sm:text-right">
                    <div class="font-medium text-navy-900">{selectedLog?.client_ip} <span class="text-gray-400 text-xs font-normal">({formatProtocol(selectedLog?.protocol || '')})</span></div>
                    {#if selectedLog && geoipData[selectedLog?.client_ip || '']}
                      {#if geoipData[selectedLog?.client_ip || '']?.city || geoipData[selectedLog?.client_ip || '']?.asn || geoipData[selectedLog?.client_ip || '']?.isp || geoipData[selectedLog?.client_ip || '']?.proxy || geoipData[selectedLog?.client_ip || '']?.hosting}
                        <div class="mt-1 flex flex-wrap sm:justify-end gap-2 text-xs font-sans">
                          {#if geoipData[selectedLog?.client_ip || '']?.city}
                            <span class="inline-flex items-center gap-1 px-1 py-0.5 rounded-md bg-blue-50 text-blue-700 border border-blue-100">
                              <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 11a3 3 0 11-6 0 3 3 0 016 0z"></path></svg>
                              {geoipData[selectedLog?.client_ip || '']?.city}
                            </span>
                          {/if}
                          {#if geoipData[selectedLog?.client_ip || '']?.asn}
                            <span class="inline-flex items-center gap-1 px-1 py-0.5 rounded-md bg-purple-50 text-purple-700 border border-purple-100">
                              <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9"></path></svg>
                              {geoipData[selectedLog?.client_ip || '']?.asn}
                            </span>
                          {/if}
                          {#if geoipData[selectedLog?.client_ip || '']?.isp}
                            <span class="inline-flex items-center gap-1 px-1 py-0.5 rounded-md bg-indigo-50 text-indigo-700 border border-indigo-100">
                              <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
                              {geoipData[selectedLog?.client_ip || '']?.isp}
                            </span>
                          {/if}
                          {#if geoipData[selectedLog?.client_ip || '']?.proxy}
                            <span class="inline-flex items-center gap-1 px-1 py-0.5 rounded-md bg-red-50 text-red-700 border border-red-100">
                              <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 11c0 3.517-1.009 6.799-2.753 9.571m-3.44-2.04l.054-.09A13.916 13.916 0 008 11a4 4 0 118 0c0 1.017-.07 2.019-.203 3m-2.118 6.844A21.88 21.88 0 0015.171 17m3.839 1.132c.645-2.266.99-4.659.99-7.132A8 8 0 008 4.07M3 15.364c.64-1.319 1-2.8 1-4.364 0-1.457.39-2.823 1.07-4"></path></svg>
                              Proxy
                            </span>
                          {/if}
                          {#if geoipData[selectedLog?.client_ip || '']?.hosting}
                            <span class="inline-flex items-center gap-1 px-1 py-0.5 rounded-md bg-orange-50 text-orange-700 border border-orange-100">
                              <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01"></path></svg>
                              Hosting
                            </span>
                          {/if}
                        </div>
                      {/if}
                    {/if}
                  </div>
                </div>                <div class="flex flex-col sm:flex-row sm:justify-between sm:items-center border-b border-gray-200 pb-2">
                  <div class="text-gray-500 font-medium mb-1 sm:mb-0">Status</div>
                  <div class="text-left sm:text-right">
                    <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold {selectedLog?.rcode?.toLowerCase() === 'noerror' ? 'bg-green-50 text-green-700' : 'bg-red-50 text-red-700'}">
                      {selectedLog?.rcode}
                    </span>
                  </div>
                </div>
                <div class="flex flex-col sm:flex-row sm:justify-between sm:items-center border-b border-gray-200 pb-2">
                  <div class="text-gray-500 font-medium mb-1 sm:mb-0">Latency</div>
                  <div class="font-medium text-navy-900 text-left sm:text-right">{selectedLog?.latency_ms} <span class="text-gray-400 text-xs font-normal">ms</span></div>
                </div>
                {#if (selectedLog?.upstreams?.length ?? 0) > 0}
                  <div class="flex flex-col sm:flex-row sm:justify-between sm:items-start">
                    <div class="text-gray-500 font-medium mb-1 sm:mb-0 mt-1">Upstreams</div>
                    <div class="flex flex-wrap gap-1 justify-end">
                      {#each selectedLog?.upstreams ?? [] as upstream}
                        {#if upstream === '__C__'}
                          <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-cyan-50 text-cyan-700 ring-1 ring-inset ring-cyan-600/20">System Cache</span>
                        {:else}
                          <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-gray-100 text-gray-700 ring-1 ring-inset ring-gray-500/10">{formatUpstream(upstream)}</span>
                        {/if}
                      {/each}
                    </div>
                  </div>
                {/if}
              </div>

              <div class="mt-4 border border-gray-200 rounded-lg overflow-hidden">
                <table class="min-w-full divide-y divide-gray-200">
                  <thead class="bg-gray-50">
                    <tr>
                      <th scope="col" class="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider w-1/4">Type</th>
                      <th scope="col" class="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Value</th>
                    </tr>
                  </thead>
                  <tbody class="bg-white divide-y divide-gray-100">
                    {#each selectedLog?.result_rows || [] as row}
                      {@const parsed = parseAnswer(row)}
                      <tr class="hover:bg-gray-50">
                        <td class="px-6 py-3 whitespace-nowrap text-left text-sm text-gray-500 font-medium">{parsed.type}</td>
                        <td class="px-6 py-3 text-left text-sm text-navy-900 font-mono break-all">
                          {parsed.value}
                          {#if (parsed.type === 'A' || parsed.type === 'AAAA') && geoipData[parsed.value]}
                            {#if geoipData[parsed.value]?.city || geoipData[parsed.value]?.asn || geoipData[parsed.value]?.isp || geoipData[parsed.value]?.proxy || geoipData[parsed.value]?.hosting}
                              <div class="mt-1 flex flex-wrap gap-2 text-xs font-sans">
                                {#if geoipData[parsed.value]?.city}
                                  <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-blue-50 text-blue-700 border border-blue-100">
                                    <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 11a3 3 0 11-6 0 3 3 0 016 0z"></path></svg>
                                    {geoipData[parsed.value]?.city}
                                  </span>
                                {/if}
                                {#if geoipData[parsed.value]?.asn}
                                  <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-purple-50 text-purple-700 border border-purple-100">
                                    <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9"></path></svg>
                                    {geoipData[parsed.value]?.asn}
                                  </span>
                                {/if}
                                {#if geoipData[parsed.value]?.isp}
                                  <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-indigo-50 text-indigo-700 border border-indigo-100">
                                    <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
                                    {geoipData[parsed.value]?.isp}
                                  </span>
                                {/if}
                                {#if geoipData[parsed.value]?.proxy}
                                  <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-red-50 text-red-700 border border-red-100">
                                    <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 11c0 3.517-1.009 6.799-2.753 9.571m-3.44-2.04l.054-.09A13.916 13.916 0 008 11a4 4 0 118 0c0 1.017-.07 2.019-.203 3m-2.118 6.844A21.88 21.88 0 0015.171 17m3.839 1.132c.645-2.266.99-4.659.99-7.132A8 8 0 008 4.07M3 15.364c.64-1.319 1-2.8 1-4.364 0-1.457.39-2.823 1.07-4"></path></svg>
                                    Proxy
                                  </span>
                                {/if}
                                {#if geoipData[parsed.value]?.hosting}
                                  <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-orange-50 text-orange-700 border border-orange-100">
                                    <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01"></path></svg>
                                    Hosting
                                  </span>
                                {/if}
                              </div>
                            {/if}
                          {/if}
                        </td>
                      </tr>
                    {/each}
                    {#if (selectedLog?.result_rows || []).length === 0}
                      <tr><td colspan="2" class="px-6 py-8 text-sm text-gray-500 text-center italic">No answers recorded</td></tr>
                    {/if}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
        <div class="bg-gray-50 px-4 py-3 sm:px-6 sm:flex sm:flex-row-reverse border-t border-gray-200">
          <button type="button" class="mt-3 w-full inline-flex justify-center rounded-md border border-gray-300 shadow-sm px-4 py-2 bg-white text-base font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-navy-500 sm:mt-0 sm:ml-3 sm:w-auto sm:text-sm transition-colors" onclick={() => (selectedLog = null)}>Close</button>
        </div>
      </div>
    </div>
  </div>
{/if}
