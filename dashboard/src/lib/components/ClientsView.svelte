<script lang="ts">
  import { onMount } from 'svelte';
  import { fade } from 'svelte/transition';
  import type { ClientStatsResponse } from '../types/dashboard';
  import ErrorAlert from './ErrorAlert.svelte';

  let { onReady = () => {} } = $props<{ onReady?: () => void }>();

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
      onReady();
    }
  }

  onMount(() => {
    fetchClients();
  });
</script>

{#if error}
  <ErrorAlert message={error} />
{/if}

<div class="space-y-4" in:fade>
  {#if clientsResponse}
    <section class="stat-row" aria-label="Client summary">
      <div class="stat">
        <div class="stat-label">Clients</div>
        <div class="stat-value">{clientsResponse.total_clients.toLocaleString()}</div>
        <div class="stat-hint">Unique sources</div>
      </div>
      <div class="stat">
        <div class="stat-label">Queries</div>
        <div class="stat-value">{clientsResponse.total_queries.toLocaleString()}</div>
        <div class="stat-hint">All clients</div>
      </div>
      <div class="stat">
        <div class="stat-label">Top source</div>
        <div class="stat-value !text-base truncate" title={clientsResponse.top_client ?? 'No client'}>
          {clientsResponse.top_client ?? 'No client'}
        </div>
        <div class="stat-hint font-mono tabular-nums">{clientsResponse.top_volume.toLocaleString()} queries</div>
      </div>
    </section>
  {/if}

  <section class="panel overflow-hidden" aria-label="Client query volume">
    <div class="panel-head flex items-center justify-between gap-3 p-4">
      <div>
        <h2 class="text-sm font-semibold text-ink">Top clients</h2>
        <p class="mt-0.5 text-xs text-muted sm:text-sm">
          {clientsResponse ? `${clientsResponse.total_clients} ranked by volume` : 'Query volume by client'}
        </p>
      </div>
      <button onclick={fetchClients} class="btn btn-secondary">Refresh</button>
    </div>

    <div class="overflow-hidden">
      <table class="data-table">
        <colgroup>
          <col />
          <col class="w-[6.5rem]" />
          <col class="w-[28%] sm:w-[34%]" />
        </colgroup>
        <thead>
          <tr>
            <th scope="col">Client</th>
            <th scope="col" class="!text-right">Queries</th>
            <th scope="col">Share</th>
          </tr>
        </thead>
        <tbody>
          {#if loading && !clientsResponse}
            {#each Array(6) as _}
              <tr>
                <td><div class="skeleton h-4 w-40 rounded"></div></td>
                <td><div class="skeleton ml-auto h-4 w-16 rounded"></div></td>
                <td><div class="skeleton h-2 w-full rounded-full"></div></td>
              </tr>
            {/each}
          {:else if clientsResponse?.items.length === 0}
            <tr>
              <td colspan="3" class="!overflow-visible !whitespace-normal !py-16 text-center">
                <div class="empty-state">
                  <div class="empty-state-icon">
                    <svg class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.75"><path stroke-linecap="round" stroke-linejoin="round" d="M17 20h5v-2a4 4 0 00-3-3.87M9 20H4v-2a4 4 0 013-3.87m6-1.13a4 4 0 100-8 4 4 0 000 8z"/></svg>
                  </div>
                  <span class="text-sm font-medium">No client data found</span>
                </div>
              </td>
            </tr>
          {:else if clientsResponse}
            {#each clientsResponse.items as client, i}
              {@const pct = (client.query_total / Math.max(clientsResponse.top_volume, 1)) * 100}
              <tr>
                <td class="text-sm">
                  <div class="cell-clip-inline">
                    <span class="flex h-7 w-7 shrink-0 items-center justify-center rounded-md {i < 3 ? 'bg-accent-fill text-on-accent' : 'bg-panel text-muted'} font-mono text-xs font-semibold tabular-nums">
                      {i + 1}
                    </span>
                    <div class="cell-clip-text min-w-0">
                      <div class="font-medium text-ink">
                        {#if client.hostname}
                          {client.hostname}
                        {:else}
                          <span class="font-mono">{client.ips[0] ?? 'unknown'}</span>
                        {/if}
                        {#if client.mac}
                          <span class="ml-2 font-mono text-xs font-normal text-faint">{client.mac}</span>
                        {/if}
                      </div>
                      <div class="mt-0.5">
                        {#each client.ips as ip, ipIndex}
                          <span class="font-mono text-xs text-faint">{ipIndex > 0 ? ' · ' : ''}{ip}</span>
                        {/each}
                      </div>
                    </div>
                  </div>
                </td>
                <td class="text-right font-mono text-sm font-semibold text-ink tabular-nums">
                  {client.query_total.toLocaleString()}
                </td>
                <td class="text-sm">
                  <div class="flex items-center gap-2">
                    <div class="bar-track min-w-0 flex-1">
                      <div class="bar-fill" style="width: {pct}%"></div>
                    </div>
                    <span class="w-9 shrink-0 text-right font-mono text-[11px] text-faint tabular-nums">{pct.toFixed(0)}%</span>
                  </div>
                </td>
              </tr>
            {/each}
          {/if}
        </tbody>
      </table>
    </div>
  </section>
</div>
