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

  onMount(() => {
    fetchClients();
  });
</script>

{#if error}
  <ErrorAlert message={error} />
{/if}

<section class="rounded-md border border-line bg-surface shadow-card overflow-hidden" aria-label="Client query volume" in:fade>
  <div class="p-4 sm:p-5 border-b border-line bg-panel flex justify-between items-center gap-4">
    <div>
      <h2 class="text-base font-semibold text-ink">Top clients</h2>
      <p class="text-sm text-faint mt-0.5">{clientsResponse ? `${clientsResponse.total_clients} clients · ${clientsResponse.total_queries.toLocaleString()} queries` : 'Client query volume'}</p>
    </div>
    <button onclick={fetchClients} class="inline-flex items-center gap-1.5 text-sm text-accent-2 font-semibold transition-colors px-3 py-1.5 rounded-md border border-accent/25 bg-accent-soft hover:bg-accent-fill hover:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-accent">
      <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
      Refresh
    </button>
  </div>
  {#if clientsResponse}
    <div class="grid border-b border-line bg-surface sm:grid-cols-3 sm:divide-x sm:divide-line">
      <div class="p-4">
        <div class="text-xs font-semibold uppercase tracking-[0.06em] text-muted">Clients</div>
        <div class="mt-2 text-2xl font-bold text-accent-2 tabular-nums">{clientsResponse.total_clients.toLocaleString()}</div>
      </div>
      <div class="border-t border-line p-4 sm:border-t-0">
        <div class="text-xs font-semibold uppercase tracking-[0.06em] text-muted">Queries</div>
        <div class="mt-2 text-2xl font-bold text-accent-2 tabular-nums">{clientsResponse.total_queries.toLocaleString()}</div>
      </div>
      <div class="border-t border-line p-4 sm:border-t-0">
        <div class="text-xs font-semibold uppercase tracking-[0.06em] text-muted">Top source</div>
        <div class="mt-2 truncate text-base font-semibold text-ink" title={clientsResponse.top_client ?? 'No client'}>
          {clientsResponse.top_client ?? 'No client'}
        </div>
        <div class="mt-1 text-xs font-semibold text-accent-2 tabular-nums">{clientsResponse.top_volume.toLocaleString()} queries</div>
      </div>
    </div>
  {/if}
  <div class="overflow-x-auto">
    <table class="min-w-full divide-y divide-line/60">
      <thead class="bg-accent-soft/45">
        <tr>
          <th scope="col" class="px-4 sm:px-6 py-3 text-left text-xs font-semibold text-muted uppercase tracking-wider">Client</th>
          <th scope="col" class="px-4 sm:px-6 py-3 text-right text-xs font-semibold text-muted uppercase tracking-wider">Total Queries</th>
          <th scope="col" class="px-4 sm:px-6 py-3 text-left text-xs font-semibold text-muted uppercase tracking-wider">Volume</th>
        </tr>
      </thead>
      <tbody class="divide-y divide-line/60">
        {#if loading && !clientsResponse}
          {#each Array(6) as _, i}
            <tr>
              <td class="px-4 sm:px-6 py-4"><div class="skeleton h-4 rounded w-40"></div></td>
              <td class="px-4 sm:px-6 py-4 text-right"><div class="skeleton h-4 rounded w-16 ml-auto"></div></td>
              <td class="px-4 sm:px-6 py-4"><div class="skeleton h-2 rounded-full w-full max-w-xs"></div></td>
            </tr>
          {/each}
        {:else if clientsResponse?.items.length === 0}
          <tr><td colspan="3" class="px-6 py-16 text-center">
            <div class="inline-flex flex-col items-center gap-3 text-faint">
              <div class="w-14 h-14 rounded-md bg-accent-soft border border-accent/20 text-accent-2 flex items-center justify-center">
                <svg class="w-7 h-7" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5"><path stroke-linecap="round" stroke-linejoin="round" d="M17 20h5v-2a4 4 0 00-3-3.87M9 20H4v-2a4 4 0 013-3.87m6-1.13a4 4 0 100-8 4 4 0 000 8z"/></svg>
              </div>
              <span class="text-sm font-medium">No client data found</span>
            </div>
          </td></tr>
        {:else if clientsResponse}
          {#each clientsResponse.items as client, i}
            <tr class="group hover:bg-accent-soft/60 transition-colors">
              <td class="px-4 sm:px-6 py-4 text-sm">
                <div class="flex items-center gap-3">
                  {#if i < 3}
                    <span class="w-7 h-7 rounded-md bg-accent-fill flex items-center justify-center text-xs font-bold text-white">{i + 1}</span>
                  {:else}
                    <span class="w-7 h-7 rounded-md bg-neutral-bg border border-line flex items-center justify-center text-xs font-bold text-muted">{i + 1}</span>
                  {/if}
                  <div class="min-w-0">
                    <div class="font-medium text-ink">
                      {#if client.hostname}
                        {client.hostname}
                      {:else}
                        {client.ips[0] ?? 'unknown'}
                      {/if}
                      {#if client.mac}
                        <span class="ml-2 text-xs text-faint font-normal font-mono">{client.mac}</span>
                      {/if}
                    </div>
                    <div class="mt-0.5 space-x-2">
                      {#each client.ips as ip}
                        <span class="text-xs text-faint font-mono">{ip}</span>
                      {/each}
                    </div>
                  </div>
                </div>
              </td>
              <td class="px-4 sm:px-6 py-4 whitespace-nowrap text-sm text-right font-semibold text-ink tabular-nums">{client.query_total.toLocaleString()}</td>
              <td class="px-4 sm:px-6 py-4 whitespace-nowrap text-sm w-full min-w-[150px] max-w-xs">
                <div class="w-full bg-line/60 rounded-full h-2 overflow-hidden">
                  <div class="bg-accent h-2 rounded-full transition-all duration-500" style="width: {(client.query_total / Math.max(clientsResponse.top_volume, 1)) * 100}%"></div>
                </div>
              </td>
            </tr>
          {/each}
        {/if}
      </tbody>
    </table>
  </div>
</section>
