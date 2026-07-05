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

<section class="mb-5 overflow-hidden rounded-xl border border-line bg-surface shadow-card" aria-label="Slowest queries" in:fade>
  <div class="border-b border-line bg-panel">
    <div class="flex flex-col gap-3 p-4 sm:flex-row sm:items-center sm:justify-between">
      <button
        type="button"
        onclick={() => (collapsed = !collapsed)}
        aria-expanded={!collapsed}
        aria-controls="slow-queries-body"
        class="group flex min-w-0 items-center gap-3 rounded-lg text-left focus:outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-panel"
      >
        <span class="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-warn/20 bg-warn-bg text-warn-text">
          <svg class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.75" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
        </span>
        <span class="min-w-0">
          <span class="flex items-center gap-2 text-base font-bold text-ink transition-colors group-hover:text-accent-2">
            Slowest queries
            <svg class="h-4 w-4 shrink-0 text-faint transition-transform duration-200 {collapsed ? '' : 'rotate-90'}" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M9 5l7 7-7 7" /></svg>
          </span>
          <span class="mt-1 block text-sm text-muted">
            {loading
              ? 'Loading the slowest recent queries'
              : `${slowQueries.length.toLocaleString()} highest-latency queries in the retention window`}
          </span>
        </span>
      </button>
      <button
        onclick={fetchSlowQueries}
        class="inline-flex items-center justify-center gap-1.5 rounded-lg border border-accent/25 bg-accent-soft px-3 py-2 text-sm font-semibold text-accent-2 hover:bg-accent-fill hover:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-panel active:scale-[0.98]"
      >
        <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
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
      <thead class="sticky top-0 z-10 bg-panel/95 backdrop-blur">
        <tr>
          <th scope="col" class="px-1 py-3 text-left text-[10px] font-semibold uppercase tracking-[0.05em] text-faint sm:px-3 sm:text-[11px] lg:px-4">Age</th>
          <th scope="col" class="px-1 py-3 text-left text-[10px] font-semibold uppercase tracking-[0.05em] text-faint sm:px-3 sm:text-[11px] lg:px-4">Client</th>
          <th scope="col" class="px-1 py-3 text-left text-[10px] font-semibold uppercase tracking-[0.05em] text-faint sm:px-3 sm:text-[11px] lg:px-4">Query</th>
          <th scope="col" class="px-1 py-3 text-left text-[10px] font-semibold uppercase tracking-[0.05em] text-faint sm:px-3 sm:text-[11px] lg:px-4">Rcode</th>
          <th scope="col" class="px-1 py-3 text-right text-[10px] font-semibold uppercase tracking-[0.05em] text-warn-text sm:px-3 sm:text-[11px] lg:px-4">
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
              <div class="flex h-14 w-14 items-center justify-center rounded-xl border border-accent/20 bg-accent-soft text-accent-2">
                <svg class="h-7 w-7" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5"><path stroke-linecap="round" stroke-linejoin="round" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
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
              class="group cursor-pointer hover:bg-accent-soft/60"
              onclick={() => onSelectLog(item)}
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
              <td class="break-all px-1 py-3 text-right align-top font-mono font-bold leading-5 tabular-nums {latencyClass} sm:px-3 lg:px-4">
                <span class="mr-0.5 hidden text-[10px] font-normal text-faint tabular-nums sm:inline">{i + 1}.</span>
                {item.latency_ms.toLocaleString()}<span class="ml-0.5 text-[10px] font-normal text-faint">ms</span>
              </td>
            </tr>
          {/each}
        {/if}
      </tbody>
    </table>
    </div>
  {/if}
</section>
