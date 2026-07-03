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

<section class="rounded-md border border-line bg-surface shadow-card overflow-hidden" aria-label="Upstream server metrics" in:fade>
  <div class="p-4 sm:p-5 border-b border-line bg-panel flex justify-between items-center gap-4">
    <div>
      <h2 class="text-base font-semibold text-ink">Upstream servers</h2>
      <p class="text-sm text-faint mt-0.5">Click a column header to sort</p>
    </div>
    <button onclick={fetchUpstreams} class="inline-flex items-center gap-1.5 text-sm text-accent-2 font-semibold transition-colors px-3 py-1.5 rounded-md border border-accent/25 bg-accent-soft hover:bg-accent-fill hover:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-accent">
      <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
      Refresh
    </button>
  </div>
  {#if upstreams.length > 0}
    <div class="grid border-b border-line bg-surface sm:grid-cols-4 sm:divide-x sm:divide-line">
      <div class="p-4">
        <div class="text-xs font-semibold uppercase tracking-[0.06em] text-muted">Queries</div>
        <div class="mt-2 text-2xl font-bold text-accent-2 tabular-nums">{upstreamQueryTotal.toLocaleString()}</div>
      </div>
      <div class="border-t border-line p-4 sm:border-t-0">
        <div class="text-xs font-semibold uppercase tracking-[0.06em] text-muted">Errors</div>
        <div class="mt-2 text-2xl font-bold text-danger-text tabular-nums">{upstreamErrorTotal.toLocaleString()}</div>
      </div>
      <div class="border-t border-line p-4 sm:border-t-0">
        <div class="text-xs font-semibold uppercase tracking-[0.06em] text-muted">Inflight</div>
        <div class="mt-2 text-2xl font-bold text-info-text tabular-nums">{upstreamInflightTotal.toLocaleString()}</div>
      </div>
      <div class="border-t border-line p-4 sm:border-t-0">
        <div class="text-xs font-semibold uppercase tracking-[0.06em] text-muted">Avg latency</div>
        <div class="mt-2 text-2xl font-bold text-accent-2 tabular-nums">{upstreamWeightedLatency.toFixed(1)}<span class="text-sm text-faint">ms</span></div>
      </div>
    </div>
  {/if}
  <div class="overflow-x-auto">
    <table class="min-w-full divide-y divide-line/60 text-xs sm:text-sm">
      <thead class="bg-accent-soft/45">
        <tr>
          <th scope="col" class="px-2 sm:px-4 py-3 text-left font-semibold text-muted uppercase tracking-[0.08em] cursor-pointer hover:bg-accent-soft/50 transition-colors whitespace-nowrap" onclick={() => sortBy('name')}>
            Upstream <span class="text-accent-2">{upstreamSortCol === 'name' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-left font-semibold text-muted uppercase tracking-[0.08em] cursor-pointer hover:bg-accent-soft/50 transition-colors whitespace-nowrap" onclick={() => sortBy('protocol')}>
            Type <span class="text-accent-2">{upstreamSortCol === 'protocol' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-muted uppercase tracking-[0.08em] cursor-pointer hover:bg-accent-soft/50 transition-colors whitespace-nowrap" onclick={() => sortBy('query_total')}>
            Queries <span class="text-accent-2">{upstreamSortCol === 'query_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-muted uppercase tracking-[0.08em] cursor-pointer hover:bg-accent-soft/50 transition-colors whitespace-nowrap" onclick={() => sortBy('completed_total')}>
            Completed <span class="text-accent-2">{upstreamSortCol === 'completed_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-muted uppercase tracking-[0.08em] cursor-pointer hover:bg-accent-soft/50 transition-colors whitespace-nowrap" onclick={() => sortBy('canceled_total')}>
            Canceled <span class="text-accent-2">{upstreamSortCol === 'canceled_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-muted uppercase tracking-[0.08em] cursor-pointer hover:bg-accent-soft/50 transition-colors whitespace-nowrap" onclick={() => sortBy('adopted_total')}>
            Adopted <span class="text-accent-2">{upstreamSortCol === 'adopted_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-muted uppercase tracking-[0.08em] cursor-pointer hover:bg-accent-soft/50 transition-colors whitespace-nowrap" onclick={() => sortBy('final_selected_total')}>
            Selected <span class="text-accent-2">{upstreamSortCol === 'final_selected_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-muted uppercase tracking-[0.08em] cursor-pointer hover:bg-accent-soft/50 transition-colors whitespace-nowrap" onclick={() => sortBy('rejected_rcode_total')}>
            Rejected <span class="text-accent-2">{upstreamSortCol === 'rejected_rcode_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-muted uppercase tracking-[0.08em] cursor-pointer hover:bg-accent-soft/50 transition-colors whitespace-nowrap" onclick={() => sortBy('error_total')}>
            Errors <span class="text-accent-2">{upstreamSortCol === 'error_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-muted uppercase tracking-[0.08em] cursor-pointer hover:bg-accent-soft/50 transition-colors whitespace-nowrap" onclick={() => sortBy('avg_latency_ms')}>
            Avg Latency <span class="text-accent-2">{upstreamSortCol === 'avg_latency_ms' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
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
              <div class="w-14 h-14 rounded-md bg-accent-soft border border-accent/20 text-accent-2 flex items-center justify-center">
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
              <td class="px-2 sm:px-4 py-4 whitespace-nowrap"><span class="inline-flex items-center px-2 py-0.5 rounded-md text-xs font-semibold bg-accent-soft text-accent-2 border border-accent/20">{formatProtocol(us.protocol)}</span></td>
              <td class="px-2 sm:px-4 py-4 whitespace-nowrap text-right text-accent-2 font-semibold tabular-nums">{us.query_total.toLocaleString()}</td>
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
</section>
