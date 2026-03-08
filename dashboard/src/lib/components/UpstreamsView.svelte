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

<div class="bg-white shadow-sm rounded-lg border border-gray-200 overflow-hidden" in:fade>
  <div class="p-4 sm:p-6 border-b border-gray-200 bg-gray-50/50 flex justify-between items-center">
    <h2 class="text-lg font-bold text-navy-900">Upstream Servers Metrics</h2>
    <button onclick={fetchUpstreams} class="text-sm text-navy-600 hover:text-navy-800 font-medium transition-colors">Refresh</button>
  </div>
  <div class="overflow-x-auto">
    <table class="min-w-full divide-y divide-gray-200 text-xs sm:text-sm">
      <thead class="bg-gray-50">
        <tr>
          <th scope="col" class="px-2 sm:px-4 py-3 text-left font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('name')}>
            Upstream <span class="text-navy-500">{upstreamSortCol === 'name' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-left font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('protocol')}>
            Type <span class="text-navy-500">{upstreamSortCol === 'protocol' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('query_total')}>
            Queries <span class="text-navy-500">{upstreamSortCol === 'query_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('completed_total')}>
            Completed <span class="text-navy-500">{upstreamSortCol === 'completed_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('canceled_total')}>
            Canceled <span class="text-navy-500">{upstreamSortCol === 'canceled_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('adopted_total')}>
            Adopted <span class="text-navy-500">{upstreamSortCol === 'adopted_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('final_selected_total')}>
            Selected <span class="text-navy-500">{upstreamSortCol === 'final_selected_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('rejected_rcode_total')}>
            Rejected <span class="text-navy-500">{upstreamSortCol === 'rejected_rcode_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
          <th scope="col" class="px-2 sm:px-4 py-3 text-right font-semibold text-gray-600 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors whitespace-nowrap" onclick={() => sortBy('error_total')}>
            Errors <span class="text-navy-500">{upstreamSortCol === 'error_total' ? (upstreamSortAsc ? '↑' : '↓') : ''}</span>
          </th>
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
