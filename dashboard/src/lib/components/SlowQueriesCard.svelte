<script lang="ts">
  import { onMount } from 'svelte';
  import { fade, slide } from 'svelte/transition';
  import type { DnsLogEntry } from '../types/dashboard';
  import { formatProtocol, formatRelativeTime } from '../utils/dashboard';
  import ErrorAlert from './ErrorAlert.svelte';

  let { onSelectLog } = $props<{
    onSelectLog: (log: DnsLogEntry) => void;
  }>();

  let slowQueries = $state<DnsLogEntry[]>([]);
  let loading = $state(true);
  let error = $state<string | null>(null);
  let collapsed = $state(false);

  function getErrorMessage(err: unknown, fallback: string) {
    if (err instanceof Error && err.message) return err.message;
    return fallback;
  }

  function formatLogAge(ts: number) {
    return formatRelativeTime(ts).replace(' ago', '');
  }

  async function fetchSlowQueries() {
    loading = true;
    error = null;
    try {
      const res = await fetch('/api/logs/slow');
      if (!res.ok) throw new Error('Failed to fetch slow queries');
      slowQueries = await res.json();
    } catch (err: unknown) {
      error = getErrorMessage(err, 'Failed to fetch slow queries');
    } finally {
      loading = false;
    }
  }

  onMount(() => {
    fetchSlowQueries();
  });
</script>

{#if error}
  <ErrorAlert message={error} />
{/if}

<section class="mb-5 rounded-md border border-line bg-surface shadow-card overflow-hidden" aria-label="Slowest queries" in:fade>
  <div class="border-b border-line bg-panel">
    <div class="flex flex-col gap-3 p-4 sm:flex-row sm:items-center sm:justify-between">
      <button
        type="button"
        onclick={() => (collapsed = !collapsed)}
        aria-expanded={!collapsed}
        aria-controls="slow-queries-body"
        class="group flex min-w-0 items-center gap-3 text-left focus:outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-panel rounded-md"
      >
        <svg class="h-5 w-5 shrink-0 text-muted transition-transform duration-200 {collapsed ? '' : 'rotate-90'}" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M9 5l7 7-7 7" /></svg>
        <span class="min-w-0">
          <span class="block text-base font-bold text-ink group-hover:text-accent-2 transition-colors">Slowest queries</span>
          <span class="block mt-1 text-sm text-muted">
            {loading
              ? 'Loading the slowest recent queries'
              : `${slowQueries.length.toLocaleString()} highest-latency queries in the retention window`}
          </span>
        </span>
      </button>
      <button
        onclick={fetchSlowQueries}
        class="inline-flex justify-center items-center gap-1.5 px-3 py-2 border border-accent/25 text-sm font-semibold rounded-md text-accent-2 bg-accent-soft hover:bg-accent-fill hover:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-panel focus-visible:ring-accent"
      >
        <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
        Refresh
      </button>
    </div>
  </div>

  {#if !collapsed}
    <div id="slow-queries-body" class="overflow-hidden" transition:slide={{ duration: 200 }}>
      <table class="w-full table-fixed divide-y divide-line text-xs sm:text-sm">
      <colgroup>
        <col class="w-[10%] sm:w-[10%]" />
        <col class="w-[20%] sm:w-[20%]" />
        <col class="w-[34%] sm:w-[36%]" />
        <col class="w-[23%] sm:w-[15%]" />
        <col class="w-[13%] sm:w-[19%]" />
      </colgroup>
      <thead class="bg-accent-soft/45 sticky top-0 z-10">
        <tr>
          <th scope="col" class="px-1 sm:px-3 lg:px-4 py-3 text-left text-[10px] sm:text-[11px] font-semibold text-muted uppercase tracking-[0.04em]">Age</th>
          <th scope="col" class="px-1 sm:px-3 lg:px-4 py-3 text-left text-[10px] sm:text-[11px] font-semibold text-muted uppercase tracking-[0.04em]">Client</th>
          <th scope="col" class="px-1 sm:px-3 lg:px-4 py-3 text-left text-[10px] sm:text-[11px] font-semibold text-muted uppercase tracking-[0.04em]">Query</th>
          <th scope="col" class="px-1 sm:px-3 lg:px-4 py-3 text-left text-[10px] sm:text-[11px] font-semibold text-muted uppercase tracking-[0.04em]">Rcode</th>
          <th scope="col" class="px-1 sm:px-3 lg:px-4 py-3 text-right text-[10px] sm:text-[11px] font-semibold text-warn-text uppercase tracking-[0.04em]">
            <span class="hidden sm:inline">Latency</span><span class="sm:hidden">ms</span>
          </th>
        </tr>
      </thead>
      <tbody class="divide-y divide-line/60">
        {#if loading && slowQueries.length === 0}
          {#each Array(5) as _}
            <tr>
              <td class="px-1 sm:px-3 lg:px-4 py-4"><div class="skeleton h-4 rounded w-full"></div></td>
              <td class="px-1 sm:px-3 lg:px-4 py-4"><div class="skeleton h-4 rounded w-full"></div></td>
              <td class="px-1 sm:px-3 lg:px-4 py-4"><div class="skeleton h-4 rounded w-full"></div></td>
              <td class="px-1 sm:px-3 lg:px-4 py-4"><div class="skeleton h-5 rounded-full w-full"></div></td>
              <td class="px-1 sm:px-3 lg:px-4 py-4 text-right"><div class="skeleton h-4 rounded w-full ml-auto"></div></td>
            </tr>
          {/each}
        {:else if slowQueries.length === 0}
          <tr><td colspan="5" class="px-6 py-16 text-center">
            <div class="inline-flex flex-col items-center gap-3 text-faint">
              <div class="w-14 h-14 rounded-md bg-accent-soft border border-accent/20 text-accent-2 flex items-center justify-center">
                <svg class="w-7 h-7" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5"><path stroke-linecap="round" stroke-linejoin="round" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
              </div>
              <span class="text-sm font-medium">No slow queries recorded</span>
            </div>
          </td></tr>
        {:else}
          {#each slowQueries as item, i (item.id)}
            {@const latencyClass =
              item.latency_ms >= 1000
                ? 'text-danger-text'
                : item.latency_ms >= 250
                  ? 'text-warn-text'
                  : 'text-success-text'}
            <tr
              class="group hover:bg-accent-soft/60 cursor-pointer"
              onclick={() => onSelectLog(item)}
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
              <td class="px-1 sm:px-3 lg:px-4 py-3 align-top break-all text-right font-bold {latencyClass} tabular-nums leading-5">
                <span class="hidden sm:inline text-faint text-[11px] font-normal mr-0.5 tabular-nums">{i + 1}.</span>
                {item.latency_ms.toLocaleString()}<span class="text-faint text-[11px] font-normal ml-0.5">ms</span>
              </td>
            </tr>
          {/each}
        {/if}
      </tbody>
    </table>
    </div>
  {/if}
</section>
