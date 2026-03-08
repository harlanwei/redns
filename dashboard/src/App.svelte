<script lang="ts">
  import { onMount } from 'svelte';
  import { fade, slide } from 'svelte/transition';

  type DnsLogEntry = {
    id: number;
    ts_unix_ms: number;
    client_ip: string;
    protocol: string;
    qname: string;
    qtype: string;
    rcode: string;
    upstreams: string[];
    result: string;
    result_rows: string[];
    latency_ms: number;
  };

  type LogSummary = {
    total_items: number;
    unique_clients: number;
    non_noerror: number;
    avg_latency_ms: number;
  };

  type PaginatedLogsResponse = {
    items: DnsLogEntry[];
    page: number;
    page_size: number;
    total_items: number;
    total_pages: number;
    summary: LogSummary;
  };

  type ClientStatsEntry = {
    ip: string;
    query_total: number;
  };

  type ClientStatsResponse = {
    items: ClientStatsEntry[];
    total_clients: number;
    total_queries: number;
    top_client: string | null;
    top_volume: number;
  };

  type UpstreamMetrics = {
    name: string;
    protocol: string;
    query_total: number;
    completed_total: number;
    inflight_total: number;
    canceled_total: number;
    adopted_total: number;
    final_selected_total: number;
    rejected_rcode_total: number;
    error_total: number;
    avg_latency_ms: number;
  };

  let logsResponse = $state<PaginatedLogsResponse | null>(null);
  let clientsResponse = $state<ClientStatsResponse | null>(null);
  let upstreams = $state<UpstreamMetrics[]>([]);
  let searchQuery = $state('');
  let page = $state(1);
  let pageSize = $state(25);
  let pageInput = $state('');
  let loading = $state(true);
  let error = $state<string | null>(null);
  let activeTab = $state<'logs' | 'clients' | 'upstreams'>('logs');
  let mobileMenuOpen = $state(false);
  let autoRefresh = $state(false);
  let refreshInterval: ReturnType<typeof setInterval> | null = null;

  let selectedLog = $state<DnsLogEntry | null>(null);
  let upstreamSortCol = $state<'name' | 'protocol' | 'query_total' | 'completed_total' | 'error_total' | 'avg_latency_ms' | 'inflight_total' | 'canceled_total' | 'adopted_total' | 'final_selected_total' | 'rejected_rcode_total'>('query_total');
  let upstreamSortAsc = $state(false);

  let sortedUpstreams = $derived([...upstreams].sort((a, b) => {
    let cmp = 0;
    if (upstreamSortCol === 'name' || upstreamSortCol === 'protocol') {
      cmp = String(a[upstreamSortCol] || '').localeCompare(String(b[upstreamSortCol] || ''));
    } else if (upstreamSortCol === 'avg_latency_ms') {
      const aVal = a.completed_total === 0 ? Infinity : Number(a.avg_latency_ms || 0);
      const bVal = b.completed_total === 0 ? Infinity : Number(b.avg_latency_ms || 0);
      cmp = aVal - bVal;
    } else {
      cmp = Number(a[upstreamSortCol] || 0) - Number(b[upstreamSortCol] || 0);
    }
    return upstreamSortAsc ? cmp : -cmp;
  }));

  function sortBy(col: typeof upstreamSortCol) {
    if (upstreamSortCol === col) {
      upstreamSortAsc = !upstreamSortAsc;
    } else {
      upstreamSortCol = col;
      upstreamSortAsc = col === 'avg_latency_ms' || col === 'name' || col === 'protocol';
    }
  }

  function parseAnswer(row: string) {
    const parts = row.split(' ');
    if (parts.length >= 3) {
      return { type: parts[1], value: parts.slice(2).join(' ') };
    }
    return { type: '-', value: row };
  }

  function formatProtocol(p: string) {
    if (!p) return '-';
    p = p.toLowerCase();
    if (p === 'udp') return 'UDP';
    if (p === 'tcp') return 'TCP';
    if (p === 'tls' || p === 'dot') return 'DoT';
    if (p === 'https' || p === 'doh') return 'DoH';
    if (p === 'quic' || p === 'doq') return 'DoQ';
    if (p === 'h3' || p === 'doh3') return 'DoH3';
    return p.toUpperCase();
  }

  async function fetchLogs(p = 1, query = '') {
    loading = true;
    error = null;
    try {
      const res = await fetch(`/api/logs?page=${p}&page_size=${pageSize}&q=${encodeURIComponent(query)}`);
      if (!res.ok) throw new Error('Failed to fetch logs');
      logsResponse = await res.json();
      page = p;
    } catch (err: any) {
      error = err.message;
    } finally {
      loading = false;
    }
  }

  async function fetchClients() {
    loading = true;
    try {
      const res = await fetch('/api/clients');
      if (!res.ok) throw new Error('Failed to fetch clients');
      clientsResponse = await res.json();
    } catch (err: any) {
      error = err.message;
    } finally {
      loading = false;
    }
  }

  async function fetchUpstreams() {
    loading = true;
    try {
      const res = await fetch('/api/upstreams');
      if (!res.ok) throw new Error('Failed to fetch upstreams');
      upstreams = await res.json();
    } catch (err: any) {
      error = err.message;
    } finally {
      loading = false;
    }
  }

  async function clearLogs() {
    if (!confirm('Are you sure you want to clear all logs?')) return;
    try {
      await fetch('/api/logs/clear', { method: 'POST' });
      fetchLogs(1, searchQuery);
      fetchClients();
    } catch (err: any) {
      alert('Failed to clear logs');
    }
  }

  function handleSearchSubmit(e: Event) {
    e.preventDefault();
    fetchLogs(1, searchQuery);
  }

  function handleTabChange(tab: 'logs' | 'clients' | 'upstreams') {
    activeTab = tab;
    mobileMenuOpen = false;
    if (tab === 'logs') fetchLogs(page, searchQuery);
    else if (tab === 'clients') fetchClients();
    else if (tab === 'upstreams') fetchUpstreams();
  }

  $effect(() => {
    if (autoRefresh && activeTab === 'logs') {
      if (!refreshInterval) {
        refreshInterval = setInterval(() => {
          fetchLogs(page, searchQuery);
        }, 3000);
      }
    } else {
      if (refreshInterval) {
        clearInterval(refreshInterval);
        refreshInterval = null;
      }
    }
  });

  onMount(() => {
    fetchLogs();
    return () => {
      if (refreshInterval) clearInterval(refreshInterval);
    };
  });

  function formatRelativeTime(ts: number) {
    const diff = Date.now() - ts;
    const seconds = Math.floor(diff / 1000);
    if (seconds <= 2) return 'Now';
    if (seconds < 60) return `${seconds}s ago`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m ago`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${hours}h ago`;
    return `${Math.floor(hours / 24)}d ago`;
  }
</script>

<div class="min-h-screen bg-gray-50 flex flex-col font-sans">
  <!-- Top Navigation -->
  <header class="bg-navy-900 text-white shadow-md z-20 sticky top-0">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      <div class="flex justify-between h-16 items-center">
        <!-- Logo -->
        <div class="flex items-center gap-3">
          <div class="w-8 h-8 rounded bg-navy-600 flex items-center justify-center font-bold text-white shadow-inner">
            R
          </div>
          <h1 class="text-xl font-bold tracking-tight">ReDNS <span class="text-navy-300 font-normal">Dashboard</span></h1>
        </div>
        
        <!-- Desktop Nav -->
        <nav class="hidden sm:flex space-x-2">
          {#each ['logs', 'clients', 'upstreams'] as tab}
            <button
              onclick={() => handleTabChange(tab as any)}
              class="px-4 py-2 rounded-md text-sm font-medium transition-colors duration-200 {activeTab === tab ? 'bg-navy-700 text-white shadow-sm ring-1 ring-navy-600' : 'text-navy-200 hover:bg-navy-800 hover:text-white capitalize'}"
            >
              <span class="capitalize">{tab}</span>
            </button>
          {/each}
        </nav>

        <!-- Mobile Menu Button -->
        <div class="sm:hidden flex items-center">
          <button 
            onclick={() => mobileMenuOpen = !mobileMenuOpen}
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

    <!-- Mobile Nav Dropdown -->
    {#if mobileMenuOpen}
      <div class="sm:hidden bg-navy-800 border-t border-navy-700" transition:slide>
        <div class="px-2 pt-2 pb-3 space-y-1">
          {#each ['logs', 'clients', 'upstreams'] as tab}
            <button
              onclick={() => handleTabChange(tab as any)}
              class="block w-full text-left px-3 py-2 rounded-md text-base font-medium {activeTab === tab ? 'bg-navy-900 text-white' : 'text-navy-200 hover:bg-navy-700 hover:text-white capitalize'}"
            >
              <span class="capitalize">{tab}</span>
            </button>
          {/each}
        </div>
      </div>
    {/if}
  </header>

  <main class="flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-6 sm:py-8">
    
    {#if error}
      <div class="bg-red-50 border-l-4 border-red-500 p-4 mb-6 rounded-r-md shadow-sm" transition:fade>
        <div class="flex">
          <div class="flex-shrink-0">
            <!-- Error Icon -->
            <svg class="h-5 w-5 text-red-400" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
              <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clip-rule="evenodd" />
            </svg>
          </div>
          <div class="ml-3">
            <p class="text-sm text-red-700">{error}</p>
          </div>
        </div>
      </div>
    {/if}

    <!-- Logs View -->
    {#if activeTab === 'logs'}
      <!-- Stats Overview -->
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
        <!-- Toolbar -->
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
              <input type="checkbox" bind:checked={autoRefresh} class="sr-only peer">
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

        <!-- Table -->
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
                  <!-- svelte-ignore a11y_click_events_have_key_events -->
                  <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
                  <tr 
                    class="hover:bg-navy-50/50 transition-colors group {item.rcode.toLowerCase() === 'noerror' ? 'cursor-pointer' : ''}"
                    in:fade={{ duration: 360 }}
                    onclick={() => { if (item.rcode.toLowerCase() === 'noerror') selectedLog = item; }}
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

        <!-- Pagination -->
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
                  <button
                    class="relative inline-flex items-center px-3 py-2 border border-navy-500 bg-navy-50 text-sm font-bold text-navy-700 z-10 cursor-default"
                  >
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
                     <span class="relative inline-flex items-center px-3 py-2 border border-gray-300 bg-white text-sm font-medium text-gray-500">
                       ...
                     </span>
                  {/if}
                  <button
                    onclick={() => fetchLogs(logsResponse!.total_pages, searchQuery)}
                    class="relative inline-flex items-center px-3 py-2 rounded-r-md border border-gray-300 bg-white text-sm font-medium text-gray-500 hover:bg-gray-50 transition-colors"
                  >
                    {logsResponse.total_pages}
                  </button>
                </nav>
                <form 
                  onsubmit={(e) => {
                    e.preventDefault();
                    const p = parseInt(pageInput, 10);
                    if (!isNaN(p) && p >= 1 && p <= logsResponse!.total_pages) {
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

      <!-- Logs Modal -->
      {#if selectedLog}
        <div class="fixed inset-0 z-50 overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true" transition:fade={{duration: 150}}>
          <div class="flex items-end justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
            <!-- svelte-ignore a11y_click_events_have_key_events -->
            <!-- svelte-ignore a11y_no_static_element_interactions -->
            <div class="fixed inset-0 bg-gray-900 bg-opacity-50 transition-opacity backdrop-blur-sm" aria-hidden="true" onclick={() => selectedLog = null}></div>
            <span class="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>
            <div class="inline-block align-bottom bg-white rounded-xl text-left overflow-hidden shadow-2xl transform transition-all sm:my-8 sm:align-middle sm:max-w-2xl sm:w-full border border-gray-200" transition:slide={{duration: 200}}>
              <div class="bg-white px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
                <div class="sm:flex sm:items-start">
                  <div class="mt-3 text-center sm:mt-0 sm:text-left w-full">
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
                          <div class="font-medium text-navy-900 text-left sm:text-right">{selectedLog?.client_ip} <span class="text-gray-400 text-xs font-normal">({formatProtocol(selectedLog?.protocol || '')})</span></div>
                        </div>
                        <div class="flex flex-col sm:flex-row sm:justify-between sm:items-center border-b border-gray-200 pb-2">
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
                        {#if selectedLog?.upstreams && selectedLog.upstreams.length > 0}
                          <div class="flex flex-col sm:flex-row sm:justify-between sm:items-start">
                             <div class="text-gray-500 font-medium mb-1 sm:mb-0 mt-1">Upstreams</div>
                             <div class="font-medium text-navy-900 text-left sm:text-right max-w-full sm:max-w-[70%] break-words">{selectedLog.upstreams.join(', ')}</div>
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
                              <td class="px-6 py-3 whitespace-nowrap text-sm text-gray-500 font-medium">{parsed.type}</td>
                              <td class="px-6 py-3 text-sm text-navy-900 font-mono break-all">{parsed.value}</td>
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
                <button type="button" class="mt-3 w-full inline-flex justify-center rounded-md border border-gray-300 shadow-sm px-4 py-2 bg-white text-base font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-navy-500 sm:mt-0 sm:ml-3 sm:w-auto sm:text-sm transition-colors" onclick={() => selectedLog = null}>Close</button>
              </div>
            </div>
          </div>
        </div>
      {/if}

    <!-- Clients View -->
    {:else if activeTab === 'clients'}
      <div class="bg-white shadow-sm rounded-lg border border-gray-200 overflow-hidden" in:fade>
        <div class="p-4 sm:p-6 border-b border-gray-200 bg-gray-50/50 flex justify-between items-center">
            <h2 class="text-lg font-bold text-navy-900">Top Clients</h2>
            <button onclick={fetchClients} class="text-sm text-navy-600 hover:text-navy-800 font-medium transition-colors">Refresh</button>
        </div>
        <div class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200">
             <thead class="bg-gray-50">
              <tr>
                <th scope="col" class="px-4 sm:px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">IP Address</th>
                <th scope="col" class="px-4 sm:px-6 py-3 text-right text-xs font-semibold text-gray-600 uppercase tracking-wider">Total Queries</th>
                <th scope="col" class="px-4 sm:px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Volume</th>
              </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-100">
               {#if loading && !clientsResponse}
                <tr><td colspan="3" class="px-6 py-12 text-center text-gray-500">Loading clients...</td></tr>
              {:else if clientsResponse?.items.length === 0}
                <tr><td colspan="3" class="px-6 py-12 text-center text-gray-500">No client data found.</td></tr>
              {:else if clientsResponse}
                {#each clientsResponse.items as client}
                  <tr class="hover:bg-navy-50/50 transition-colors">
                    <td class="px-4 sm:px-6 py-4 whitespace-nowrap text-sm font-medium text-navy-900">{client.ip}</td>
                    <td class="px-4 sm:px-6 py-4 whitespace-nowrap text-sm text-right font-medium text-gray-700">{client.query_total.toLocaleString()}</td>
                    <td class="px-4 sm:px-6 py-4 whitespace-nowrap text-sm w-full min-w-[150px] max-w-xs">
                        <div class="w-full bg-gray-100 rounded-full h-2.5 border border-gray-200 overflow-hidden">
                            <div class="bg-navy-500 h-2.5 rounded-full" style="width: {(client.query_total / Math.max(clientsResponse.top_volume, 1)) * 100}%"></div>
                        </div>
                    </td>
                  </tr>
                {/each}
              {/if}
            </tbody>
          </table>
        </div>
      </div>

    <!-- Upstreams View -->
    {:else if activeTab === 'upstreams'}
      <div class="bg-white shadow-sm rounded-lg border border-gray-200 overflow-hidden" in:fade>
         <div class="p-4 sm:p-6 border-b border-gray-200 bg-gray-50/50 flex justify-between items-center">
            <h2 class="text-lg font-bold text-navy-900">Upstream Servers Metrics</h2>
            <button onclick={fetchUpstreams} class="text-sm text-navy-600 hover:text-navy-800 font-medium transition-colors">Refresh</button>
        </div>
        <div class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200 text-xs sm:text-sm">
             <thead class="bg-gray-50">
              <tr>
                <!-- svelte-ignore a11y_click_events_have_key_events -->
                <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
                <th scope="col" class="px-2 sm:px-4 py-3 text-left font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('name')}>
                  Upstream <span class="text-navy-500">{upstreamSortCol === 'name' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
                </th>
                <!-- svelte-ignore a11y_click_events_have_key_events -->
                <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
                <th scope="col" class="px-2 sm:px-4 py-3 text-left font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('protocol')}>
                  Type <span class="text-navy-500">{upstreamSortCol === 'protocol' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
                </th>
                <!-- svelte-ignore a11y_click_events_have_key_events -->
                <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
                <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('query_total')}>
                  Queries <span class="text-navy-500">{upstreamSortCol === 'query_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
                </th>
                <!-- svelte-ignore a11y_click_events_have_key_events -->
                <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
                <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('completed_total')}>
                  Completed <span class="text-navy-500">{upstreamSortCol === 'completed_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
                </th>
                <!-- svelte-ignore a11y_click_events_have_key_events -->
                <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
                <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('canceled_total')}>
                  Canceled <span class="text-navy-500">{upstreamSortCol === 'canceled_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
                </th>
                <!-- svelte-ignore a11y_click_events_have_key_events -->
                <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
                <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('adopted_total')}>
                  Adopted <span class="text-navy-500">{upstreamSortCol === 'adopted_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
                </th>
                <!-- svelte-ignore a11y_click_events_have_key_events -->
                <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
                <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('final_selected_total')}>
                  Selected <span class="text-navy-500">{upstreamSortCol === 'final_selected_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
                </th>
                <!-- svelte-ignore a11y_click_events_have_key_events -->
                <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
                <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('rejected_rcode_total')}>
                  Rejected <span class="text-navy-500">{upstreamSortCol === 'rejected_rcode_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
                </th>
                <!-- svelte-ignore a11y_click_events_have_key_events -->
                <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
                <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('error_total')}>
                  Errors <span class="text-navy-500">{upstreamSortCol === 'error_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
                </th>
                <!-- svelte-ignore a11y_click_events_have_key_events -->
                <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
                <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('avg_latency_ms')}>
                  Avg Latency <span class="text-navy-500">{upstreamSortCol === 'avg_latency_ms' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
                </th>
              </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-100">
                {#if loading && upstreams.length === 0}
                    <tr><td colspan="10" class="px-6 py-12 text-center text-gray-500">Loading upstreams...</td></tr>
                {:else if upstreams.length === 0}
                    <tr><td colspan="10" class="px-6 py-12 text-center text-gray-500">No upstreams found.</td></tr>
                {:else}
                    {#each sortedUpstreams as us}
                    {@const q = Math.max(us.query_total, 1)}
                    <tr class="hover:bg-navy-50/50 transition-colors">
                        <td class="px-2 sm:px-4 py-4 whitespace-nowrap font-medium text-navy-900">{us.name}</td>
                        <td class="px-2 sm:px-4 py-4 whitespace-nowrap font-medium text-gray-600">{formatProtocol(us.protocol)}</td>
                        <td class="px-2 sm:px-4 py-4 whitespace-nowrap text-right text-navy-600 font-medium">{us.query_total.toLocaleString()}</td>
                        <td class="px-2 sm:px-4 py-4 whitespace-nowrap text-right font-medium">{us.completed_total.toLocaleString()}</td>
                        <td class="px-2 sm:px-4 py-4 whitespace-nowrap text-right text-gray-500 font-medium">{us.canceled_total.toLocaleString()}</td>
                        <td class="px-2 sm:px-4 py-4 whitespace-nowrap text-right font-medium">
                          {us.adopted_total.toLocaleString()} <span class="text-xs text-gray-400 font-normal ml-1">({(us.adopted_total / q * 100).toFixed(1)}%)</span>
                        </td>
                        <td class="px-2 sm:px-4 py-4 whitespace-nowrap text-right text-emerald-600 font-medium">
                          {us.final_selected_total.toLocaleString()} <span class="text-xs font-normal ml-1">({(us.final_selected_total / q * 100).toFixed(1)}%)</span>
                        </td>
                        <td class="px-2 sm:px-4 py-4 whitespace-nowrap text-right text-orange-600 font-medium">
                          {us.rejected_rcode_total.toLocaleString()} <span class="text-xs font-normal ml-1">({(us.rejected_rcode_total / q * 100).toFixed(1)}%)</span>
                        </td>
                        <td class="px-2 sm:px-4 py-4 whitespace-nowrap text-right text-red-600 font-medium">
                          {us.error_total.toLocaleString()} <span class="text-xs font-normal ml-1">({(us.error_total / q * 100).toFixed(1)}%)</span>
                        </td>
                        <td class="px-2 sm:px-4 py-4 whitespace-nowrap text-right font-medium text-navy-900">
                           {#if us.completed_total === 0}
                             <span class="text-gray-400 italic">Infinity</span>
                           {:else}
                             {us.avg_latency_ms.toFixed(1)} <span class="text-xs text-gray-400 font-normal">ms</span>
                           {/if}
                        </td>
                    </tr>
                    {/each}
                {/if}
            </tbody>
          </table>
        </div>
      </div>
    {/if}

  </main>
</div>
