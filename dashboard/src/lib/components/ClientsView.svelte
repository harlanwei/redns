<script lang="ts">
  import { onMount } from 'svelte';
  import { fade } from 'svelte/transition';
  import type { ClientStatsResponse } from '../types/dashboard';
  import ErrorAlert from './ErrorAlert.svelte';

  let clientsResponse = $state<ClientStatsResponse | null>(null);
  let loading = $state(true);
  let error = $state<string | null>(null);

  function getErrorMessage(err: unknown, fallback: string) {
    if (err instanceof Error && err.message) return err.message;
    return fallback;
  }

  async function fetchClients() {
    loading = true;
    error = null;
    try {
      const res = await fetch('/api/clients');
      if (!res.ok) throw new Error('Failed to fetch clients');
      clientsResponse = await res.json();
    } catch (err: unknown) {
      error = getErrorMessage(err, 'Failed to fetch clients');
    } finally {
      loading = false;
    }
  }

  function clientLabel(client: { hostname: string | null; ips: string[] }): string {
    return client.hostname ?? client.ips[0] ?? 'unknown';
  }

  onMount(() => {
    fetchClients();
  });
</script>

{#if error}
  <ErrorAlert message={error} />
{/if}

<div class="bg-white shadow-sm rounded-lg border border-gray-200 overflow-hidden" in:fade>
  <div class="p-4 sm:p-6 border-b border-gray-200 bg-gray-50/50 flex justify-between items-center">
    <h2 class="text-lg font-bold text-navy-900">Top Clients</h2>
    <button onclick={fetchClients} class="text-sm text-navy-600 hover:text-navy-800 font-medium transition-colors">Refresh</button>
  </div>
  <div class="overflow-x-auto">
    <table class="min-w-full divide-y divide-gray-200">
      <thead class="bg-gray-50">
        <tr>
          <th scope="col" class="px-4 sm:px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Client</th>
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
              <td class="px-4 sm:px-6 py-4 text-sm">
                <div class="font-medium text-navy-900">
                  {#if client.hostname}
                    {client.hostname}
                  {:else}
                    {client.ips[0] ?? 'unknown'}
                  {/if}
                  {#if client.mac}
                    <span class="ml-2 text-xs text-gray-400 font-normal">{client.mac}</span>
                  {/if}
                </div>
                <div class="mt-0.5 space-x-2">
                  {#each client.ips as ip}
                    <span class="text-xs text-gray-500">{ip}</span>
                  {/each}
                </div>
              </td>
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
