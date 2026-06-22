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

<div class="glass rounded-2xl border border-line/60 shadow-card overflow-hidden" in:fade>
  <div class="p-4 sm:p-6 border-b border-line/60 glass-panel flex justify-between items-center gap-4">
    <div class="flex items-center gap-3">
      <div class="w-10 h-10 rounded-xl bg-grad-accent flex items-center justify-center shadow-glow shrink-0">
        <svg class="w-5 h-5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01" /></svg>
      </div>
      <div>
        <h2 class="text-lg font-bold text-ink">Upstream Servers</h2>
        <p class="text-sm text-faint mt-0.5">Click a column header to sort</p>
      </div>
    </div>
    <button onclick={fetchUpstreams} class="inline-flex items-center gap-1.5 text-sm text-accent hover:text-accent-2 font-medium transition-colors px-3 py-1.5 rounded-lg hover:bg-accent-soft">
      <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
      Refresh
    </button>
  </div>
  <div class="overflow-x-auto">
    <table class="min-w-full divide-y divide-line/60 text-xs sm:text-sm">
      <thead class="glass-panel">
        <tr>
          <th scope="col" class="px-2 sm:px-4 py-3 text-left font-semibold text-muted uppercase tracking-wider cursor-pointer hover:bg-accent-soft/40 transition-colors whitespace-nowrap" onclick={() => sortBy('name')}>
            Upstream <span class="text-accent">{upstreamSortCol === 'name' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-left font-semibold text-muted uppercase tracking-wider cursor-pointer hover:bg-accent-soft/40 transition-colors whitespace-nowrap" onclick={() => sortBy('protocol')}>
            Type <span class="text-accent">{upstreamSortCol === 'protocol' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-muted uppercase tracking-wider cursor-pointer hover:bg-accent-soft/40 transition-colors whitespace-nowrap" onclick={() => sortBy('query_total')}>
            Queries <span class="text-accent">{upstreamSortCol === 'query_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-muted uppercase tracking-wider cursor-pointer hover:bg-accent-soft/40 transition-colors whitespace-nowrap" onclick={() => sortBy('completed_total')}>
            Completed <span class="text-accent">{upstreamSortCol === 'completed_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-muted uppercase tracking-wider cursor-pointer hover:bg-accent-soft/40 transition-colors whitespace-nowrap" onclick={() => sortBy('canceled_total')}>
            Canceled <span class="text-accent">{upstreamSortCol === 'canceled_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-muted uppercase tracking-wider cursor-pointer hover:bg-accent-soft/40 transition-colors whitespace-nowrap" onclick={() => sortBy('adopted_total')}>
            Adopted <span class="text-accent">{upstreamSortCol === 'adopted_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-muted uppercase tracking-wider cursor-pointer hover:bg-accent-soft/40 transition-colors whitespace-nowrap" onclick={() => sortBy('final_selected_total')}>
            Selected <span class="text-accent">{upstreamSortCol === 'final_selected_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-muted uppercase tracking-wider cursor-pointer hover:bg-accent-soft/40 transition-colors whitespace-nowrap" onclick={() => sortBy('rejected_rcode_total')}>
            Rejected <span class="text-accent">{upstreamSortCol === 'rejected_rcode_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-muted uppercase tracking-wider cursor-pointer hover:bg-accent-soft/40 transition-colors whitespace-nowrap" onclick={() => sortBy('error_total')}>
            Errors <span class="text-accent">{upstreamSortCol === 'error_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-muted uppercase tracking-wider cursor-pointer hover:bg-accent-soft/40 transition-colors whitespace-nowrap" onclick={() => sortBy('avg_latency_ms')}>
            Avg Latency <span class="text-accent">{upstreamSortCol === 'avg_latency_ms' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
        </tr>
      </thead>
      <tbody class="divide-y divide-line/60">
        {#if loading && upstreams.length === 0}
          {#each Array(4) as _, i}
            <tr>
              <td class="px-2 sm:px-4 py-4"><div class="skeleton h-4 rounded w-32"></div></td>
              <td class="px-2 sm:px-4 py-4"><div class="skeleton h-4 rounded w-10"></div></td>
              <td class="px-2 sm:px-4 py-4 text-right"><div class="skeleton h-4 rounded w-12 ml-auto"></div></td>
              <td class="px-2 sm:px-4 py-4 text-right"><div class="skeleton h-4 rounded w-12 ml-auto"></div></td>
              <td class="px-2 sm:px-4 py-4 text-right"><div class="skeleton h-4 rounded w-12 ml-auto"></div></td>
              <td class="px-2 sm:px-4 py-4 text-right"><div class="skeleton h-4 rounded w-16 ml-auto"></div></td>
              <td class="px-2 sm:px-4 py-4 text-right"><div class="skeleton h-4 rounded w-16 ml-auto"></div></td>
              <td class="px-2 sm:px-4 py-4 text-right"><div class="skeleton h-4 rounded w-16 ml-auto"></div></td>
              <td class="px-2 sm:px-4 py-4 text-right"><div class="skeleton h-4 rounded w-16 ml-auto"></div></td>
              <td class="px-2 sm:px-4 py-4 text-right"><div class="skeleton h-4 rounded w-14 ml-auto"></div></td>
            </tr>
          {/each}
        {:else if upstreams.length === 0}
          <tr><td colspan="10" class="px-6 py-16 text-center">
            <div class="inline-flex flex-col items-center gap-3 text-faint">
              <div class="w-14 h-14 rounded-2xl bg-panel border border-line flex items-center justify-center">
                <svg class="w-7 h-7" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5"><path stroke-linecap="round" stroke-linejoin="round" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2"/></svg>
              </div>
              <span class="text-sm font-medium">No upstreams found</span>
            </div>
          </td></tr>
        {:else}
          {#each sortedUpstreams as us}
            {@const q = Math.max(us.query_total, 1)}
            <tr class="hover:bg-accent-soft/60 transition-colors">
              <td class="px-2 sm:px-4 py-4 whitespace-nowrap font-medium text-ink">{us.name}</td>
              <td class="px-2 sm:px-4 py-4 whitespace-nowrap"><span class="inline-flex items-center px-2 py-0.5 rounded-md text-xs font-semibold bg-neutral-bg text-neutral-text border border-line">{formatProtocol(us.protocol)}</span></td>
              <td class="px-2 sm:px-4 py-4 whitespace-nowrap text-right text-ink font-semibold tabular-nums">{us.query_total.toLocaleString()}</td>
              <td class="px-2 sm:px-4 py-4 whitespace-nowrap text-right font-medium text-ink tabular-nums">{us.completed_total.toLocaleString()}</td>
              <td class="px-2 sm:px-4 py-4 whitespace-nowrap text-right text-faint font-medium tabular-nums">{us.canceled_total.toLocaleString()}</td>
              <td class="px-2 sm:px-4 py-4 whitespace-nowrap text-right font-medium text-ink tabular-nums">
                {us.adopted_total.toLocaleString()} <span class="text-xs text-faint font-normal ml-1">({(us.adopted_total / q * 100).toFixed(1)}%)</span>
              </td>
              <td class="px-2 sm:px-4 py-4 whitespace-nowrap text-right text-success-text font-semibold tabular-nums">
                {us.final_selected_total.toLocaleString()} <span class="text-xs font-normal ml-1">({(us.final_selected_total / q * 100).toFixed(1)}%)</span>
              </td>
              <td class="px-2 sm:px-4 py-4 whitespace-nowrap text-right text-warn-text font-semibold tabular-nums">
                {us.rejected_rcode_total.toLocaleString()} <span class="text-xs font-normal ml-1">({(us.rejected_rcode_total / q * 100).toFixed(1)}%)</span>
              </td>
              <td class="px-2 sm:px-4 py-4 whitespace-nowrap text-right text-danger-text font-semibold tabular-nums">
                {us.error_total.toLocaleString()} <span class="text-xs font-normal ml-1">({(us.error_total / q * 100).toFixed(1)}%)</span>
              </td>
              <td class="px-2 sm:px-4 py-4 whitespace-nowrap text-right font-medium text-ink tabular-nums">
                {#if us.completed_total === 0}
                  <span class="text-faint italic">Infinity</span>
                {:else}
                  {us.avg_latency_ms.toFixed(1)}<span class="text-xs text-faint font-normal ml-0.5">ms</span>
                {/if}
              </td>
            </tr>
          {/each}
        {/if}
      </tbody>
    </table>
  </div>
</div>
