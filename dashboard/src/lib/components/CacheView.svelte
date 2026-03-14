<script lang="ts">
  import { onMount } from 'svelte';
  import { fade } from 'svelte/transition';
  import type { CacheSnapshot } from '../types/dashboard';
  import ErrorAlert from './ErrorAlert.svelte';

  let caches = $state<CacheSnapshot[]>([]);
  let loading = $state(true);
  let error = $state<string | null>(null);

  function getErrorMessage(err: unknown, fallback: string) {
    if (err instanceof Error && err.message) return err.message;
    return fallback;
  }

  function utilization(entries: number, capacity: number) {
    if (!capacity) return 0;
    return (entries / capacity) * 100;
  }

  function shardLabel(index: number) {
    return `Shard ${String(index + 1).padStart(2, '0')}`;
  }

  async function fetchCache() {
    loading = true;
    error = null;
    try {
      const res = await fetch('/api/cache');
      if (!res.ok) throw new Error('Failed to fetch cache metrics');
      const data = await res.json();
      caches = Array.isArray(data) ? data : [];
    } catch (err: unknown) {
      error = getErrorMessage(err, 'Failed to fetch cache metrics');
    } finally {
      loading = false;
    }
  }

  onMount(() => {
    fetchCache();
  });
</script>

{#if error}
  <ErrorAlert message={error} />
{/if}

<div class="space-y-6" in:fade>
  <div class="bg-white shadow-sm rounded-lg border border-gray-200 overflow-hidden">
    <div class="p-4 sm:p-6 border-b border-gray-200 bg-gray-50/50 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
      <div>
        <h2 class="text-lg font-bold text-navy-900">Cache Utilization</h2>
        <p class="text-sm text-gray-500">Sharded cache occupancy and balance.</p>
      </div>
      <button onclick={fetchCache} class="text-sm text-navy-600 hover:text-navy-800 font-medium transition-colors">Refresh</button>
    </div>

    {#if loading && caches.length === 0}
      <div class="px-6 py-12 text-center text-gray-500">Loading cache metrics...</div>
    {:else if caches.length === 0}
      <div class="px-6 py-12 text-center text-gray-500">No cache instances found.</div>
    {:else}
      <div class="p-4 sm:p-6 space-y-6">
        {#each caches as cache (cache.id)}
          {@const totalPct = utilization(cache.total_entries, cache.total_capacity)}
          {@const shardCount = Math.max(cache.shards.length, 1)}
          <div class="border border-gray-200 rounded-lg overflow-hidden">
            <div class="px-4 sm:px-6 py-4 bg-white border-b border-gray-200 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
              <div>
                <h3 class="text-base font-bold text-navy-900">Cache #{cache.id}</h3>
                <p class="text-xs text-gray-500">{cache.shards.length} shards</p>
              </div>
              <div class="text-left sm:text-right">
                <div class="text-xs uppercase tracking-wider text-gray-500 font-semibold">Utilization</div>
                <div class="text-2xl font-bold text-navy-900">{totalPct.toFixed(1)}%</div>
              </div>
            </div>

            <div class="p-4 sm:p-6 space-y-5 bg-gray-50/40">
              <div class="grid grid-cols-1 sm:grid-cols-3 gap-3">
                <div class="bg-white rounded-lg border border-gray-200 p-3">
                  <div class="text-xs uppercase tracking-wider text-gray-500 font-semibold">Entries</div>
                  <div class="text-xl font-bold text-navy-900">{cache.total_entries.toLocaleString()}</div>
                </div>
                <div class="bg-white rounded-lg border border-gray-200 p-3">
                  <div class="text-xs uppercase tracking-wider text-gray-500 font-semibold">Capacity</div>
                  <div class="text-xl font-bold text-navy-900">{cache.total_capacity.toLocaleString()}</div>
                </div>
                <div class="bg-white rounded-lg border border-gray-200 p-3">
                  <div class="text-xs uppercase tracking-wider text-gray-500 font-semibold">Avg / Shard</div>
                  <div class="text-xl font-bold text-navy-900">{Math.round(cache.total_entries / shardCount).toLocaleString()}</div>
                </div>
              </div>

              <div>
                <div class="flex justify-between text-xs text-gray-500 font-semibold uppercase tracking-wider">
                  <span>Total fill</span>
                  <span>{cache.total_entries.toLocaleString()} / {cache.total_capacity.toLocaleString()}</span>
                </div>
                <div class="mt-2 h-2 rounded-full bg-gray-200 overflow-hidden">
                  <div class="h-full bg-navy-500" style={`width: ${Math.min(totalPct, 100)}%;`}></div>
                </div>
              </div>

              <div>
                <div class="text-xs font-semibold text-gray-600 uppercase tracking-wider mb-3">Shard Utilization</div>
                <div class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-8 gap-3">
                  {#each cache.shards as shard (shard.index)}
                    {@const shardPct = utilization(shard.entries, shard.capacity)}
                    <div class="rounded-lg border border-gray-200 bg-white p-3">
                      <div class="flex items-center justify-between text-xs font-semibold text-gray-600">
                        <span>{shardLabel(shard.index)}</span>
                        <span class="text-navy-700">{shardPct.toFixed(0)}%</span>
                      </div>
                      <div class="mt-2 h-1.5 rounded-full bg-gray-200 overflow-hidden">
                        <div class="h-full bg-emerald-500" style={`width: ${Math.min(shardPct, 100)}%;`}></div>
                      </div>
                      <div class="mt-2 text-[11px] text-gray-500">{shard.entries.toLocaleString()} / {shard.capacity.toLocaleString()}</div>
                    </div>
                  {/each}
                </div>
              </div>
            </div>
          </div>
        {/each}
      </div>
    {/if}
  </div>
</div>
