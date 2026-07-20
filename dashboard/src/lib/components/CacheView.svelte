<script lang="ts">
  import { onMount } from 'svelte';
  import { fade } from 'svelte/transition';
  import type { CacheSnapshot } from '../types/dashboard';
  import ErrorAlert from './ErrorAlert.svelte';

  let caches = $state<CacheSnapshot[]>([]);
  let loading = $state(true);
  let error = $state<string | null>(null);
  let cacheTotalEntries = $derived(caches.reduce((sum, cache) => sum + cache.total_entries, 0));
  let cacheTotalCapacity = $derived(caches.reduce((sum, cache) => sum + cache.total_capacity, 0));
  let cacheTotalHits = $derived(caches.reduce((sum, cache) => sum + cache.hit_total, 0));
  let cacheTotalMisses = $derived(caches.reduce((sum, cache) => sum + cache.miss_total, 0));

  function getErrorMessage(err: unknown, fallback: string) {
    if (err instanceof Error && err.message) return err.message;
    return fallback;
  }

  function utilization(entries: number, capacity: number) {
    if (!capacity) return 0;
    return (entries / capacity) * 100;
  }

  function shardLabel(index: number) {
    return `S${String(index + 1).padStart(2, '0')}`;
  }

  function hitRate(hits: number, misses: number) {
    const total = hits + misses;
    return total > 0 ? (hits / total) * 100 : 0;
  }

  function fillClass(pct: number) {
    if (pct >= 90) return 'bar-fill bar-fill-danger';
    if (pct >= 75) return 'bar-fill bar-fill-warn';
    return 'bar-fill';
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

<div class="space-y-4" in:fade>
  {#if caches.length > 0}
    <section class="stat-row" aria-label="Cache summary">
      <div class="stat">
        <div class="stat-label">Instances</div>
        <div class="stat-value">{caches.length.toLocaleString()}</div>
        <div class="stat-hint">Cache pools</div>
      </div>
      <div class="stat">
        <div class="stat-label">Entries</div>
        <div class="stat-value">{cacheTotalEntries.toLocaleString()}</div>
        <div class="stat-hint">of {cacheTotalCapacity.toLocaleString()}</div>
      </div>
      <div class="stat">
        <div class="stat-label">Occupancy</div>
        <div class="stat-value">{utilization(cacheTotalEntries, cacheTotalCapacity).toFixed(1)}<span class="ml-0.5 text-sm font-medium text-faint">%</span></div>
        <div class="stat-hint">Total fill</div>
      </div>
      <div class="stat">
        <div class="stat-label">Hit rate</div>
        <div class="stat-value">{hitRate(cacheTotalHits, cacheTotalMisses).toFixed(1)}<span class="ml-0.5 text-sm font-medium text-faint">%</span></div>
        <div class="stat-hint">{cacheTotalHits.toLocaleString()} hits</div>
      </div>
    </section>
  {/if}

  <section class="panel overflow-hidden" aria-label="Cache utilization">
    <div class="panel-head flex items-center justify-between gap-3 p-4">
      <div>
        <h2 class="text-sm font-semibold text-ink">Utilization</h2>
        <p class="mt-0.5 text-xs text-muted sm:text-sm">Sharded occupancy across pools</p>
      </div>
      <button onclick={fetchCache} class="btn btn-secondary">Refresh</button>
    </div>

    {#if loading && caches.length === 0}
      <div class="space-y-4 p-4">
        <div class="skeleton h-28 w-full rounded-lg"></div>
        <div class="grid grid-cols-2 gap-3 sm:grid-cols-4">
          {#each Array(4) as _}<div class="skeleton h-16 rounded-lg"></div>{/each}
        </div>
      </div>
    {:else if caches.length === 0}
      <div class="px-6 py-16 text-center">
        <div class="empty-state">
          <div class="empty-state-icon">
            <svg class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.75"><path stroke-linecap="round" stroke-linejoin="round" d="M4 7c0-1.1 3.58-2 8-2s8 .9 8 2-3.58 2-8 2-8-.9-8-2zm0 0v10c0 1.1 3.58 2 8 2s8-.9 8-2V7"/></svg>
          </div>
          <span class="text-sm font-medium">No cache instances found</span>
        </div>
      </div>
    {:else}
      <div class="divide-y divide-line">
        {#each caches as cache (cache.id)}
          {@const totalPct = utilization(cache.total_entries, cache.total_capacity)}
          {@const shardCount = Math.max(cache.shards.length, 1)}
          <article class="p-4 sm:p-5">
            <div class="mb-4 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
              <div class="flex items-center gap-2.5">
                <span class="flex h-8 w-8 items-center justify-center rounded-lg bg-accent-fill font-mono text-xs font-bold text-on-accent tabular-nums">{cache.id}</span>
                <div>
                  <h3 class="text-sm font-semibold text-ink">Cache #{cache.id}</h3>
                  <p class="text-xs text-faint tabular-nums">{cache.shards.length} shards</p>
                </div>
              </div>
              <div class="font-mono text-xl font-semibold text-ink tabular-nums">
                {totalPct.toFixed(1)}<span class="text-sm font-medium text-faint">%</span>
              </div>
            </div>

            <div class="mb-4 grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-6">
              <div class="rounded-lg bg-panel/70 px-3 py-2.5">
                <div class="text-xs text-muted">Entries</div>
                <div class="mt-0.5 font-mono text-sm font-semibold tabular-nums">{cache.total_entries.toLocaleString()}</div>
              </div>
              <div class="rounded-lg bg-panel/70 px-3 py-2.5">
                <div class="text-xs text-muted">Capacity</div>
                <div class="mt-0.5 font-mono text-sm font-semibold tabular-nums">{cache.total_capacity.toLocaleString()}</div>
              </div>
              <div class="rounded-lg bg-panel/70 px-3 py-2.5">
                <div class="text-xs text-muted">Avg / shard</div>
                <div class="mt-0.5 font-mono text-sm font-semibold tabular-nums">{Math.round(cache.total_entries / shardCount).toLocaleString()}</div>
              </div>
              <div class="rounded-lg bg-panel/70 px-3 py-2.5">
                <div class="text-xs text-muted">Hits</div>
                <div class="mt-0.5 font-mono text-sm font-semibold text-success-text tabular-nums">{cache.hit_total.toLocaleString()}</div>
              </div>
              <div class="rounded-lg bg-panel/70 px-3 py-2.5">
                <div class="text-xs text-muted">Misses</div>
                <div class="mt-0.5 font-mono text-sm font-semibold text-warn-text tabular-nums">{cache.miss_total.toLocaleString()}</div>
              </div>
              <div class="rounded-lg bg-panel/70 px-3 py-2.5">
                <div class="text-xs text-muted">Hit rate</div>
                <div class="mt-0.5 font-mono text-sm font-semibold tabular-nums">{hitRate(cache.hit_total, cache.miss_total).toFixed(1)}%</div>
              </div>
            </div>

            <div class="mb-4">
              <div class="mb-1.5 flex justify-between text-xs text-muted">
                <span>Fill</span>
                <span class="font-mono tabular-nums">{cache.total_entries.toLocaleString()} / {cache.total_capacity.toLocaleString()}</span>
              </div>
              <div class="bar-track h-2">
                <div class="{fillClass(totalPct)}" style={`width: ${Math.min(totalPct, 100)}%;`}></div>
              </div>
            </div>

            <div>
              <div class="mb-2 text-xs font-medium text-muted">Shards</div>
              <div class="grid grid-cols-2 gap-2 sm:grid-cols-4 lg:grid-cols-8">
                {#each cache.shards as shard (shard.index)}
                  {@const shardPct = utilization(shard.entries, shard.capacity)}
                  <div class="rounded-lg border border-line bg-surface px-2.5 py-2">
                    <div class="flex items-center justify-between text-[11px] font-medium">
                      <span class="font-mono text-muted">{shardLabel(shard.index)}</span>
                      <span class="font-mono tabular-nums {shardPct >= 90 ? 'text-danger-text' : shardPct >= 75 ? 'text-warn-text' : 'text-accent-2'}">{shardPct.toFixed(0)}%</span>
                    </div>
                    <div class="bar-track mt-1.5 h-1">
                      <div class="{fillClass(shardPct)}" style={`width: ${Math.min(shardPct, 100)}%;`}></div>
                    </div>
                    <div class="mt-1 font-mono text-[10px] text-faint tabular-nums">{shard.entries}/{shard.capacity}</div>
                  </div>
                {/each}
              </div>
            </div>
          </article>
        {/each}
      </div>
    {/if}
  </section>
</div>
