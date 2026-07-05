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

<div class="space-y-5" in:fade>
  {#if clientsResponse}
    <section class="grid gap-3 sm:grid-cols-3" aria-label="Client summary">
      <div class="rounded-xl border border-line bg-surface p-4 shadow-soft">
        <div class="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted">Clients</div>
        <div class="mt-2 font-mono text-2xl font-semibold text-ink tabular-nums">{clientsResponse.total_clients.toLocaleString()}</div>
        <div class="mt-1 text-xs text-faint">Unique sources</div>
      </div>
      <div class="rounded-xl border border-line bg-surface p-4 shadow-soft">
        <div class="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted">Queries</div>
        <div class="mt-2 font-mono text-2xl font-semibold text-ink tabular-nums">{clientsResponse.total_queries.toLocaleString()}</div>
        <div class="mt-1 text-xs text-faint">Across all clients</div>
      </div>
      <div class="relative overflow-hidden rounded-xl bg-header p-4 text-header-text shadow-card">
        <div class="pointer-events-none absolute -right-6 -top-8 h-24 w-24 rounded-full bg-[radial-gradient(circle,color-mix(in_srgb,var(--ui-accent)_60%,transparent),transparent_70%)]" aria-hidden="true"></div>
        <div class="relative text-[11px] font-semibold uppercase tracking-[0.08em] text-accent-3">Top source</div>
        <div class="relative mt-2 truncate text-base font-semibold text-white" title={clientsResponse.top_client ?? 'No client'}>
          {clientsResponse.top_client ?? 'No client'}
        </div>
        <div class="relative mt-1 font-mono text-xs font-semibold text-accent-3 tabular-nums">{clientsResponse.top_volume.toLocaleString()} queries</div>
      </div>
    </section>
  {/if}

  <section class="overflow-hidden rounded-xl border border-line bg-surface shadow-card" aria-label="Client query volume">
    <div class="flex items-center justify-between gap-4 border-b border-line bg-panel p-4 sm:p-5">
      <div>
        <h2 class="text-base font-bold text-ink">Top clients</h2>
        <p class="mt-0.5 text-sm text-muted">{clientsResponse ? `${clientsResponse.total_clients} clients ranked by volume` : 'Client query volume'}</p>
      </div>
      <button onclick={fetchClients} class="inline-flex items-center gap-1.5 rounded-lg border border-accent/25 bg-accent-soft px-3 py-2 text-sm font-semibold text-accent-2 hover:bg-accent-fill hover:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-accent active:scale-[0.98]">
        <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
        Refresh
      </button>
    </div>

    <div class="overflow-x-auto">
      <table class="min-w-full divide-y divide-line/60">
        <thead class="bg-panel/95 backdrop-blur">
          <tr>
            <th scope="col" class="px-4 py-3 text-left text-[11px] font-semibold uppercase tracking-[0.05em] text-faint sm:px-6">Client</th>
            <th scope="col" class="px-4 py-3 text-right text-[11px] font-semibold uppercase tracking-[0.05em] text-faint sm:px-6">Queries</th>
            <th scope="col" class="px-4 py-3 text-left text-[11px] font-semibold uppercase tracking-[0.05em] text-faint sm:px-6">Volume</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-line/60">
          {#if loading && !clientsResponse}
            {#each Array(6) as _, i}
              <tr>
                <td class="px-4 py-4 sm:px-6"><div class="skeleton h-4 w-40 rounded"></div></td>
                <td class="px-4 py-4 text-right sm:px-6"><div class="skeleton ml-auto h-4 w-16 rounded"></div></td>
                <td class="px-4 py-4 sm:px-6"><div class="skeleton h-2 w-full max-w-xs rounded-full"></div></td>
              </tr>
            {/each}
          {:else if clientsResponse?.items.length === 0}
            <tr><td colspan="3" class="px-6 py-16 text-center">
              <div class="inline-flex flex-col items-center gap-3 text-faint">
                <div class="flex h-14 w-14 items-center justify-center rounded-xl border border-accent/20 bg-accent-soft text-accent-2">
                  <svg class="h-7 w-7" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5"><path stroke-linecap="round" stroke-linejoin="round" d="M17 20h5v-2a4 4 0 00-3-3.87M9 20H4v-2a4 4 0 013-3.87m6-1.13a4 4 0 100-8 4 4 0 000 8z"/></svg>
                </div>
                <span class="text-sm font-medium">No client data found</span>
              </div>
            </td></tr>
          {:else if clientsResponse}
            {#each clientsResponse.items as client, i}
              {@const pct = (client.query_total / Math.max(clientsResponse.top_volume, 1)) * 100}
              <tr class="group hover:bg-accent-soft/60">
                <td class="px-4 py-4 text-sm sm:px-6">
                  <div class="flex items-center gap-3">
                    {#if i < 3}
                      <span class="flex h-7 w-7 items-center justify-center rounded-lg bg-accent-fill text-xs font-bold text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.2)] tabular-nums">{i + 1}</span>
                    {:else}
                      <span class="flex h-7 w-7 items-center justify-center rounded-lg border border-line bg-neutral-bg text-xs font-bold text-muted tabular-nums">{i + 1}</span>
                    {/if}
                    <div class="min-w-0">
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
                      <div class="mt-0.5 space-x-2">
                        {#each client.ips as ip}
                          <span class="font-mono text-xs text-faint">{ip}</span>
                        {/each}
                      </div>
                    </div>
                  </div>
                </td>
                <td class="whitespace-nowrap px-4 py-4 text-right font-mono text-sm font-semibold text-ink tabular-nums sm:px-6">{client.query_total.toLocaleString()}</td>
                <td class="w-full min-w-[150px] max-w-xs whitespace-nowrap px-4 py-4 text-sm sm:px-6">
                  <div class="flex items-center gap-2">
                    <div class="h-2 flex-1 overflow-hidden rounded-full bg-line/60">
                      <div class="h-2 rounded-full bg-accent transition-all duration-500" style="width: {pct}%"></div>
                    </div>
                    <span class="w-10 shrink-0 text-right font-mono text-[11px] text-faint tabular-nums">{pct.toFixed(0)}%</span>
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
