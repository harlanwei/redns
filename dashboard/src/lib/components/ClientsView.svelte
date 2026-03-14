<script lang="ts">
  import { onMount } from 'svelte';
  import { fade } from 'svelte/transition';
  import type { ClientStatsResponse } from '../types/dashboard';
  import ErrorAlert from './ErrorAlert.svelte';

  let clientsResponse = $state<ClientStatsResponse | null>(null);
  let loading = $state(true);
  let error = $state<string | null>(null);
  let geoipData = $state<Record<string, { city: string | null; asn: string | null; isp: string | null; proxy: boolean | null; hosting: boolean | null }>>({});

  $effect(() => {
    if (clientsResponse) {
      const ipsToFetch = new Set<string>();
      for (const client of clientsResponse.items) {
        if (client.ip && !geoipData[client.ip]) {
          ipsToFetch.add(client.ip);
        }
      }
      for (const ip of ipsToFetch) {
        fetch(`/api/geoip?ip=${encodeURIComponent(ip)}`)
          .then((res) => res.json())
          .then((data) => {
            const newData = { ...geoipData };
            newData[ip] = data;
            geoipData = newData;
          })
          .catch((err) => console.error('Failed to fetch geoip for', ip, err));
      }
    }
  });

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

<div class="bg-white shadow-sm rounded-lg border border-gray-200 overflow-hidden" in:fade>
  <div class="p-4 sm:p-6 border-b border-gray-200 bg-gray-50/50 flex justify-between items-center">
    <h2 class="text-lg font-bold text-navy-900">Top Clients</h2>
    <button onclick={fetchClients} class="text-sm text-navy-600 hover:text-navy-800 font-medium transition-colors">Refresh</button>
  </div>
  <div class="overflow-x-auto">
    <table class="min-w-full divide-y divide-gray-200">
      <thead class="bg-gray-50">
        <tr>
          <th scope="col" class="px-4 sm:px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">IP Address</th>
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
              <td class="px-4 sm:px-6 py-4 whitespace-nowrap text-sm font-medium text-navy-900">
                {client.ip}
                {#if geoipData[client.ip]}
                  {#if geoipData[client.ip]?.city || geoipData[client.ip]?.asn || geoipData[client.ip]?.isp || geoipData[client.ip]?.proxy || geoipData[client.ip]?.hosting}
                    <div class="mt-1 flex flex-wrap gap-2 text-xs font-sans font-normal">
                      {#if geoipData[client.ip]?.city}
                        <span class="inline-flex items-center gap-1 px-1 py-0.5 rounded-md bg-blue-50 text-blue-700 border border-blue-100">
                          <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 11a3 3 0 11-6 0 3 3 0 016 0z"></path></svg>
                          {geoipData[client.ip]?.city}
                        </span>
                      {/if}
                      {#if geoipData[client.ip]?.asn}
                        <span class="inline-flex items-center gap-1 px-1 py-0.5 rounded-md bg-purple-50 text-purple-700 border border-purple-100">
                          <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9"></path></svg>
                          {geoipData[client.ip]?.asn}
                        </span>
                      {/if}
                      {#if geoipData[client.ip]?.isp}
                        <span class="inline-flex items-center gap-1 px-1 py-0.5 rounded-md bg-indigo-50 text-indigo-700 border border-indigo-100">
                          <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
                          {geoipData[client.ip]?.isp}
                        </span>
                      {/if}
                      {#if geoipData[client.ip]?.proxy}
                        <span class="inline-flex items-center gap-1 px-1 py-0.5 rounded-md bg-red-50 text-red-700 border border-red-100">
                          <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 11c0 3.517-1.009 6.799-2.753 9.571m-3.44-2.04l.054-.09A13.916 13.916 0 008 11a4 4 0 118 0c0 1.017-.07 2.019-.203 3m-2.118 6.844A21.88 21.88 0 0015.171 17m3.839 1.132c.645-2.266.99-4.659.99-7.132A8 8 0 008 4.07M3 15.364c.64-1.319 1-2.8 1-4.364 0-1.457.39-2.823 1.07-4"></path></svg>
                          Proxy
                        </span>
                      {/if}
                      {#if geoipData[client.ip]?.hosting}
                        <span class="inline-flex items-center gap-1 px-1 py-0.5 rounded-md bg-orange-50 text-orange-700 border border-orange-100">
                          <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01"></path></svg>
                          Hosting
                        </span>
                      {/if}
                    </div>
                  {/if}
                {/if}
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
