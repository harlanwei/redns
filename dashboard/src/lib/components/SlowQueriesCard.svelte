<script lang="ts">
  import { onMount } from 'svelte';
  import { fade, slide } from 'svelte/transition';
  import type { DnsLogEntry } from '../types/dashboard';
  import { formatProtocol, formatRelativeTime } from '../utils/dashboard';
  import ErrorAlert from './ErrorAlert.svelte';

  let { onSelectLog, onReady = () => {} } = $props<{
    onSelectLog: (log: DnsLogEntry) => void;
    onReady?: () => void;
  }>();

  let slowQueries = $state<DnsLogEntry[]>([]);
  let loading = $state(true);
  let error = $state<string | null>(null);
  // Collapsed by default so the main log table stays primary.
  let collapsed = $state(true);

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
      onReady();
    }
  }

  onMount(() => {
    fetchSlowQueries();
  });
</script>

{#if error}
  <ErrorAlert message={error} />
{/if}

<section class="panel overflow-hidden" aria-label="Slowest queries" in:fade>
  <div class="flex flex-col gap-2 p-3 sm:flex-row sm:items-center sm:justify-between sm:p-4">
    <button
      type="button"
      onclick={() => (collapsed = !collapsed)}
      aria-expanded={!collapsed}
      aria-controls="slow-queries-body"
      class="group flex min-w-0 items-center gap-3 rounded-lg text-left focus:outline-none focus-visible:ring-2 focus-visible:ring-accent"
    >
      <span class="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-warn-bg text-warn-text">
        <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.75" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
      </span>
      <span class="min-w-0">
        <span class="flex items-center gap-2 text-sm font-semibold text-ink group-hover:text-accent-2">
          Slowest queries
          <svg class="h-3.5 w-3.5 text-faint transition-transform {collapsed ? '' : 'rotate-90'}" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M9 5l7 7-7 7" /></svg>
        </span>
        <span class="mt-0.5 block text-xs text-muted">
          {loading ? 'Loading…' : `${slowQueries.length.toLocaleString()} highest-latency queries`}
        </span>
      </span>
    </button>
    <button onclick={fetchSlowQueries} class="btn btn-secondary self-start sm:self-auto">Refresh</button>
  </div>

  {#if !collapsed}
    <div id="slow-queries-body" class="overflow-hidden border-t border-line" transition:slide={{ duration: 180 }}>
      <table class="data-table text-xs sm:text-sm">
        <colgroup>
          <col class="w-[4.5rem] sm:w-[5.5rem]" />
          <col class="w-[7.5rem] sm:w-[10rem]" />
          <col />
          <col class="w-[5.75rem] sm:w-[6.5rem]" />
          <col class="w-[5.5rem] sm:w-[6.5rem]" />
        </colgroup>
        <thead>
          <tr>
            <th scope="col">Age</th>
            <th scope="col">Client</th>
            <th scope="col">Query</th>
            <th scope="col">Rcode</th>
            <th scope="col" class="!text-right text-warn-text">Latency</th>
          </tr>
        </thead>
        <tbody>
          {#if loading && slowQueries.length === 0}
            {#each Array(4) as _}
              <tr>
                <td><div class="skeleton h-4 w-12 rounded"></div></td>
                <td><div class="skeleton h-4 w-20 rounded"></div></td>
                <td><div class="skeleton h-4 w-3/4 max-w-xs rounded"></div></td>
                <td><div class="skeleton h-5 w-16 rounded-full"></div></td>
                <td><div class="skeleton ml-auto h-4 w-12 rounded"></div></td>
              </tr>
            {/each}
          {:else if slowQueries.length === 0}
            <tr>
              <td colspan="5" class="!overflow-visible !whitespace-normal !py-12 text-center">
                <div class="empty-state">
                  <span class="text-sm font-medium">No slow queries recorded</span>
                </div>
              </td>
            </tr>
          {:else}
            {#each slowQueries as item, i (item.id)}
              {@const latencyClass =
                item.latency_ms >= 1000
                  ? 'text-danger-text'
                  : item.latency_ms >= 250
                    ? 'text-warn-text'
                    : 'text-success-text'}
              <tr class="row-interactive" onclick={() => onSelectLog(item)}>
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
                  <span class="chip {item.rcode.toLowerCase() === 'noerror' ? 'chip-success' : 'chip-danger'}">{item.rcode}</span>
                </td>
                <td class="text-right font-mono font-semibold tabular-nums {latencyClass}">
                  <span class="mr-1 hidden text-[10px] font-normal text-faint sm:inline">{i + 1}.</span>
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
