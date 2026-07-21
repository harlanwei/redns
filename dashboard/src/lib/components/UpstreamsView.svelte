<script lang="ts">
  import { onMount } from 'svelte';
  import { fade } from 'svelte/transition';
  import type { UpstreamMetrics, UpstreamSortCol } from '../types/dashboard';
  import { formatProtocol, sortUpstreams } from '../utils/dashboard';
  import ErrorAlert from './ErrorAlert.svelte';

  let upstreams = $state<UpstreamMetrics[]>([]);
  let loading = $state(true);
  let error = $state<string | null>(null);
  let upstreamSortCol = $state<UpstreamSortCol>('query_total');
  let upstreamSortAsc = $state(false);

  let sortedUpstreams = $derived(sortUpstreams(upstreams, upstreamSortCol, upstreamSortAsc));
  let upstreamQueryTotal = $derived(upstreams.reduce((sum, us) => sum + us.query_total, 0));
  let upstreamErrorTotal = $derived(upstreams.reduce((sum, us) => sum + us.error_total, 0));
  let upstreamInflightTotal = $derived(upstreams.reduce((sum, us) => sum + us.inflight_total, 0));
  let upstreamCompletedTotal = $derived(upstreams.reduce((sum, us) => sum + us.completed_total, 0));
  let upstreamWeightedLatency = $derived(
    upstreamCompletedTotal > 0
      ? upstreams.reduce((sum, us) => sum + us.avg_latency_ms * us.completed_total, 0) / upstreamCompletedTotal
      : 0,
  );

  function getErrorMessage(err: unknown, fallback: string) {
    if (err instanceof Error && err.message) return err.message;
    return fallback;
  }

  function sortBy(col: UpstreamSortCol) {
    if (upstreamSortCol === col) {
      upstreamSortAsc = !upstreamSortAsc;
    } else {
      upstreamSortCol = col;
      upstreamSortAsc = col === 'avg_latency_ms' || col === 'name' || col === 'protocol';
    }
  }

  const columns: { key: UpstreamSortCol; label: string; align: 'left' | 'right' }[] = [
    { key: 'name', label: 'Upstream', align: 'left' },
    { key: 'protocol', label: 'Type', align: 'left' },
    { key: 'query_total', label: 'Queries', align: 'right' },
    { key: 'completed_total', label: 'Done', align: 'right' },
    { key: 'canceled_total', label: 'Canceled', align: 'right' },
    { key: 'adopted_total', label: 'Adopted', align: 'right' },
    { key: 'final_selected_total', label: 'Selected', align: 'right' },
    { key: 'rejected_rcode_total', label: 'Rejected', align: 'right' },
    { key: 'error_total', label: 'Errors', align: 'right' },
    { key: 'avg_latency_ms', label: 'Avg ms', align: 'right' },
  ];

  async function fetchUpstreams() {
    loading = true;
    error = null;
    try {
      const res = await fetch('/api/upstreams');
      if (!res.ok) throw new Error('Failed to fetch upstreams');
      upstreams = await res.json();
    } catch (err: unknown) {
      error = getErrorMessage(err, 'Failed to fetch upstreams');
    } finally {
      loading = false;
    }
  }

  onMount(() => {
    fetchUpstreams();
  });
</script>

{#if error}
  <ErrorAlert message={error} />
{/if}

<div class="space-y-4" in:fade>
  {#if upstreams.length > 0}
    <section class="stat-row" aria-label="Upstream summary">
      <div class="stat">
        <div class="stat-label">Queries</div>
        <div class="stat-value">{upstreamQueryTotal.toLocaleString()}</div>
        <div class="stat-hint">Sent upstream</div>
      </div>
      <div class="stat {upstreamErrorTotal > 0 ? 'stat-danger' : ''}">
        <div class="stat-label">Errors</div>
        <div class="stat-value {upstreamErrorTotal > 0 ? 'text-danger-text' : ''}">{upstreamErrorTotal.toLocaleString()}</div>
        <div class="stat-hint">Failed exchanges</div>
      </div>
      <div class="stat">
        <div class="stat-label">Inflight</div>
        <div class="stat-value">{upstreamInflightTotal.toLocaleString()}</div>
        <div class="stat-hint">In progress</div>
      </div>
      <div class="stat">
        <div class="stat-label">Avg latency</div>
        <div class="stat-value">{upstreamWeightedLatency.toFixed(1)}<span class="ml-0.5 text-sm font-medium text-faint">ms</span></div>
        <div class="stat-hint">Weighted</div>
      </div>
    </section>
  {/if}

  <section class="panel overflow-hidden" aria-label="Upstream server metrics">
    <div class="panel-head flex items-center justify-between gap-3 p-4">
      <div>
        <h2 class="text-sm font-semibold text-ink">Servers</h2>
        <p class="mt-0.5 text-xs text-muted sm:text-sm">Click a column header to sort</p>
      </div>
      <button onclick={fetchUpstreams} class="btn btn-secondary">Refresh</button>
    </div>

    <div class="overflow-hidden">
      <table class="data-table text-[11px] sm:text-xs">
        <colgroup>
          <col class="w-[14%] sm:w-[16%]" />
          <col class="w-[4.5rem]" />
          <col />
          <col />
          <col />
          <col />
          <col />
          <col />
          <col />
          <col class="w-[4.25rem]" />
        </colgroup>
        <thead>
          <tr>
            {#each columns as col}
              {@const active = upstreamSortCol === col.key}
              <th
                scope="col"
                aria-sort={active ? (upstreamSortAsc ? 'ascending' : 'descending') : 'none'}
                class="cursor-pointer hover:bg-accent-soft/50 {col.align === 'right' ? '!text-right' : '!text-left'} {active ? '!text-accent-2' : ''}"
                onclick={() => sortBy(col.key)}
                title={col.label}
              >
                <span class="cell-clip inline-flex max-w-full items-center gap-0.5 {col.align === 'right' ? 'flex-row-reverse' : ''}">
                  {col.label}
                  {#if active}
                    <span class="shrink-0 text-accent-2">{upstreamSortAsc ? '↑' : '↓'}</span>
                  {/if}
                </span>
              </th>
            {/each}
          </tr>
        </thead>
        <tbody>
          {#if loading && upstreams.length === 0}
            {#each Array(4) as _}
              <tr>
                <td><div class="skeleton h-4 w-20 rounded"></div></td>
                <td><div class="skeleton h-4 w-10 rounded"></div></td>
                {#each Array(8) as _}
                  <td class="text-right"><div class="skeleton ml-auto h-4 w-10 rounded"></div></td>
                {/each}
              </tr>
            {/each}
          {:else if upstreams.length === 0}
            <tr>
              <td colspan="10" class="!overflow-visible !whitespace-normal !py-16 text-center">
                <div class="empty-state">
                  <div class="empty-state-icon">
                    <svg class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.75"><path stroke-linecap="round" stroke-linejoin="round" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2"/></svg>
                  </div>
                  <span class="text-sm font-medium">No upstreams found</span>
                </div>
              </td>
            </tr>
          {:else}
            {#each sortedUpstreams as us}
              {@const q = Math.max(us.query_total, 1)}
              <tr>
                <td class="font-medium text-ink" title={us.name}>
                  <span class="cell-clip">{us.name}</span>
                </td>
                <td><span class="chip chip-accent font-mono">{formatProtocol(us.protocol)}</span></td>
                <td class="text-right font-mono font-semibold text-accent-2 tabular-nums" title={us.query_total.toLocaleString()}>
                  {us.query_total.toLocaleString()}
                </td>
                <td class="text-right font-mono text-ink tabular-nums" title={us.completed_total.toLocaleString()}>
                  {us.completed_total.toLocaleString()}
                </td>
                <td class="text-right font-mono text-faint tabular-nums" title={us.canceled_total.toLocaleString()}>
                  {us.canceled_total.toLocaleString()}
                </td>
                <td class="text-right font-mono text-ink tabular-nums" title={`${us.adopted_total.toLocaleString()} (${(us.adopted_total / q * 100).toFixed(1)}%)`}>
                  {us.adopted_total.toLocaleString()}<span class="text-[10px] text-faint"> ({(us.adopted_total / q * 100).toFixed(0)}%)</span>
                </td>
                <td class="text-right font-mono font-semibold text-success-text tabular-nums" title={`${us.final_selected_total.toLocaleString()} (${(us.final_selected_total / q * 100).toFixed(1)}%)`}>
                  {us.final_selected_total.toLocaleString()}<span class="text-[10px] font-normal"> ({(us.final_selected_total / q * 100).toFixed(0)}%)</span>
                </td>
                <td class="text-right font-mono font-semibold text-warn-text tabular-nums" title={`${us.rejected_rcode_total.toLocaleString()} (${(us.rejected_rcode_total / q * 100).toFixed(1)}%)`}>
                  {us.rejected_rcode_total.toLocaleString()}<span class="text-[10px] font-normal"> ({(us.rejected_rcode_total / q * 100).toFixed(0)}%)</span>
                </td>
                <td class="text-right font-mono font-semibold text-danger-text tabular-nums" title={`${us.error_total.toLocaleString()} (${(us.error_total / q * 100).toFixed(1)}%)`}>
                  {us.error_total.toLocaleString()}<span class="text-[10px] font-normal"> ({(us.error_total / q * 100).toFixed(0)}%)</span>
                </td>
                <td class="text-right font-mono text-ink tabular-nums">
                  {#if us.completed_total === 0}
                    <span class="italic text-faint">∞</span>
                  {:else}
                    {us.avg_latency_ms.toFixed(1)}
                  {/if}
                </td>
              </tr>
            {/each}
          {/if}
        </tbody>
      </table>
    </div>
  </section>
</div>
