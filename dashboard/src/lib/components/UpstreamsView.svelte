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

  // Columns declared once so header + sort affordances stay in sync.
  const columns: { key: UpstreamSortCol; label: string; align: 'left' | 'right' }[] = [
    { key: 'name', label: 'Upstream', align: 'left' },
    { key: 'protocol', label: 'Type', align: 'left' },
    { key: 'query_total', label: 'Queries', align: 'right' },
    { key: 'completed_total', label: 'Completed', align: 'right' },
    { key: 'canceled_total', label: 'Canceled', align: 'right' },
    { key: 'adopted_total', label: 'Adopted', align: 'right' },
    { key: 'final_selected_total', label: 'Selected', align: 'right' },
    { key: 'rejected_rcode_total', label: 'Rejected', align: 'right' },
    { key: 'error_total', label: 'Errors', align: 'right' },
    { key: 'avg_latency_ms', label: 'Avg latency', align: 'right' },
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

<div class="space-y-5" in:fade>
  {#if upstreams.length > 0}
    <section class="grid gap-3 sm:grid-cols-2 lg:grid-cols-4" aria-label="Upstream summary">
      <div class="relative overflow-hidden rounded-xl bg-header p-4 text-header-text shadow-card">
        <div class="pointer-events-none absolute -right-6 -top-8 h-24 w-24 rounded-full bg-[radial-gradient(circle,color-mix(in_srgb,var(--ui-accent)_60%,transparent),transparent_70%)]" aria-hidden="true"></div>
        <div class="relative text-[11px] font-semibold uppercase tracking-[0.08em] text-accent-3">Queries</div>
        <div class="relative mt-2 font-mono text-2xl font-semibold text-white tabular-nums">{upstreamQueryTotal.toLocaleString()}</div>
        <div class="relative mt-1 text-xs text-header-muted">Sent to upstreams</div>
      </div>
      <div class="rounded-xl border border-line bg-surface p-4 shadow-soft">
        <div class="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted">Errors</div>
        <div class="mt-2 font-mono text-2xl font-semibold {upstreamErrorTotal > 0 ? 'text-danger-text' : 'text-ink'} tabular-nums">{upstreamErrorTotal.toLocaleString()}</div>
        <div class="mt-1 text-xs text-faint">Failed exchanges</div>
      </div>
      <div class="rounded-xl border border-line bg-surface p-4 shadow-soft">
        <div class="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted">Inflight</div>
        <div class="mt-2 font-mono text-2xl font-semibold text-ink tabular-nums">{upstreamInflightTotal.toLocaleString()}</div>
        <div class="mt-1 text-xs text-faint">In progress now</div>
      </div>
      <div class="rounded-xl border border-line bg-surface p-4 shadow-soft">
        <div class="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted">Avg latency</div>
        <div class="mt-2 font-mono text-2xl font-semibold text-ink tabular-nums">{upstreamWeightedLatency.toFixed(1)}<span class="ml-0.5 text-sm font-medium text-faint">ms</span></div>
        <div class="mt-1 text-xs text-faint">Completion-weighted</div>
      </div>
    </section>
  {/if}

  <section class="overflow-hidden rounded-xl border border-line bg-surface shadow-card" aria-label="Upstream server metrics">
    <div class="flex items-center justify-between gap-4 border-b border-line bg-panel p-4 sm:p-5">
      <div>
        <h2 class="text-base font-bold text-ink">Upstream servers</h2>
        <p class="mt-0.5 text-sm text-muted">Select a column header to sort.</p>
      </div>
      <button onclick={fetchUpstreams} class="inline-flex items-center gap-1.5 rounded-lg border border-accent/25 bg-accent-soft px-3 py-2 text-sm font-semibold text-accent-2 hover:bg-accent-fill hover:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-accent active:scale-[0.98]">
        <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
        Refresh
      </button>
    </div>
    <div class="overflow-x-auto">
      <table class="min-w-full divide-y divide-line/60 text-xs sm:text-sm">
        <thead class="bg-panel/95 backdrop-blur">
          <tr>
            {#each columns as col}
              {@const active = upstreamSortCol === col.key}
              <th
                scope="col"
                aria-sort={active ? (upstreamSortAsc ? 'ascending' : 'descending') : 'none'}
                class="cursor-pointer whitespace-nowrap px-2 py-3 font-semibold uppercase tracking-[0.06em] hover:bg-accent-soft/50 sm:px-4 {col.align === 'right' ? 'text-right' : 'text-left'} {active ? 'text-accent-2' : 'text-faint'}"
                onclick={() => sortBy(col.key)}
              >
                <span class="inline-flex items-center gap-1 {col.align === 'right' ? 'flex-row-reverse' : ''}">
                  {col.label}
                  <span class="text-accent-2">{active ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
                </span>
              </th>
            {/each}
          </tr>
        </thead>
        <tbody class="divide-y divide-line/60">
          {#if loading && upstreams.length === 0}
            {#each Array(4) as _, i}
              <tr>
                <td class="px-2 py-4 sm:px-4"><div class="skeleton h-4 w-32 rounded"></div></td>
                <td class="px-2 py-4 sm:px-4"><div class="skeleton h-4 w-10 rounded"></div></td>
                {#each Array(8) as _}
                  <td class="px-2 py-4 text-right sm:px-4"><div class="skeleton ml-auto h-4 w-14 rounded"></div></td>
                {/each}
              </tr>
            {/each}
          {:else if upstreams.length === 0}
            <tr><td colspan="10" class="px-6 py-16 text-center">
              <div class="inline-flex flex-col items-center gap-3 text-faint">
                <div class="flex h-14 w-14 items-center justify-center rounded-xl border border-accent/20 bg-accent-soft text-accent-2">
                  <svg class="h-7 w-7" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5"><path stroke-linecap="round" stroke-linejoin="round" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2"/></svg>
                </div>
                <span class="text-sm font-medium">No upstreams found</span>
              </div>
            </td></tr>
          {:else}
            {#each sortedUpstreams as us}
              {@const q = Math.max(us.query_total, 1)}
              <tr class="hover:bg-accent-soft/60">
                <td class="whitespace-nowrap px-2 py-4 font-medium text-ink sm:px-4">{us.name}</td>
                <td class="whitespace-nowrap px-2 py-4 sm:px-4"><span class="inline-flex items-center rounded-md border border-accent/20 bg-accent-soft px-2 py-0.5 font-mono text-[11px] font-semibold text-accent-2">{formatProtocol(us.protocol)}</span></td>
                <td class="whitespace-nowrap px-2 py-4 text-right font-mono font-semibold text-accent-2 tabular-nums sm:px-4">{us.query_total.toLocaleString()}</td>
                <td class="whitespace-nowrap px-2 py-4 text-right font-mono font-medium text-ink tabular-nums sm:px-4">{us.completed_total.toLocaleString()}</td>
                <td class="whitespace-nowrap px-2 py-4 text-right font-mono font-medium text-faint tabular-nums sm:px-4">{us.canceled_total.toLocaleString()}</td>
                <td class="whitespace-nowrap px-2 py-4 text-right font-mono font-medium text-ink tabular-nums sm:px-4">
                  {us.adopted_total.toLocaleString()} <span class="ml-1 text-[11px] font-normal text-faint">({(us.adopted_total / q * 100).toFixed(1)}%)</span>
                </td>
                <td class="whitespace-nowrap px-2 py-4 text-right font-mono font-semibold text-success-text tabular-nums sm:px-4">
                  {us.final_selected_total.toLocaleString()} <span class="ml-1 text-[11px] font-normal">({(us.final_selected_total / q * 100).toFixed(1)}%)</span>
                </td>
                <td class="whitespace-nowrap px-2 py-4 text-right font-mono font-semibold text-warn-text tabular-nums sm:px-4">
                  {us.rejected_rcode_total.toLocaleString()} <span class="ml-1 text-[11px] font-normal">({(us.rejected_rcode_total / q * 100).toFixed(1)}%)</span>
                </td>
                <td class="whitespace-nowrap px-2 py-4 text-right font-mono font-semibold text-danger-text tabular-nums sm:px-4">
                  {us.error_total.toLocaleString()} <span class="ml-1 text-[11px] font-normal">({(us.error_total / q * 100).toFixed(1)}%)</span>
                </td>
                <td class="whitespace-nowrap px-2 py-4 text-right font-mono font-medium text-ink tabular-nums sm:px-4">
                  {#if us.completed_total === 0}
                    <span class="italic text-faint">Infinity</span>
                  {:else}
                    {us.avg_latency_ms.toFixed(1)}<span class="ml-0.5 text-[11px] font-normal text-faint">ms</span>
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
