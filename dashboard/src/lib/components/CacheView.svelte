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

  function hitRate(hits: number, misses: number) {
    const total = hits + misses;
    return total > 0 ? (hits / total) * 100 : 0;
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
  <div class="glass rounded-2xl border border-line/60 shadow-card overflow-hidden">
    <div class="p-4 sm:p-6 border-b border-line/60 glass-panel flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
      <div class="flex items-center gap-3">
        <div class="w-10 h-10 rounded-xl bg-grad-accent flex items-center justify-center shadow-glow shrink-0">
          <svg class="w-5 h-5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M4 7v10a2 2 0 002 2h12a2 2 0 002-2V7a2 2 0 00-2-2H6a2 2 0 00-2 2zM4 7l8 6 8-6" /></svg>
        </div>
        <div>
          <h2 class="text-lg font-bold text-ink">Cache Utilization</h2>
          <p class="text-sm text-faint mt-0.5">Sharded cache occupancy and balance.</p>
        </div>
      </div>
      <button onclick={fetchCache} class="inline-flex items-center gap-1.5 text-sm text-accent hover:text-accent-2 font-medium transition-colors px-3 py-1.5 rounded-lg hover:bg-accent-soft">
        <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
        Refresh
      </button>
    </div>

    {#if loading && caches.length === 0}
      <div class="p-4 sm:p-6 space-y-6">
        {#each Array(1) as _}
          <div class="border border-line/60 rounded-2xl overflow-hidden">
            <div class="px-4 sm:px-6 py-4 border-b border-line/60 flex justify-between">
              <div class="skeleton h-5 w-28 rounded"></div>
              <div class="skeleton h-8 w-20 rounded"></div>
            </div>
            <div class="p-4 sm:p-6 grid grid-cols-3 gap-3">
              {#each Array(6) as _}<div class="skeleton h-20 rounded-xl"></div>{/each}
            </div>
            <div class="p-4 sm:p-6">
              <div class="skeleton h-2 rounded-full w-full"></div>
            </div>
          </div>
        {/each}
      </div>
    {:else if caches.length === 0}
      <div class="px-6 py-16 text-center">
        <div class="inline-flex flex-col items-center gap-3 text-faint">
          <div class="w-14 h-14 rounded-2xl bg-panel border border-line flex items-center justify-center">
            <svg class="w-7 h-7" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5"><path stroke-linecap="round" stroke-linejoin="round" d="M4 7v10a2 2 0 002 2h12a2 2 0 002-2V7a2 2 0 00-2-2H6a2 2 0 00-2 2z"/></svg>
          </div>
          <span class="text-sm font-medium">No cache instances found</span>
        </div>
      </div>
    {:else}
      <div class="p-4 sm:p-6 space-y-6">
        {#each caches as cache (cache.id)}
          {@const totalPct = utilization(cache.total_entries, cache.total_capacity)}
          {@const shardCount = Math.max(cache.shards.length, 1)}
          <div class="border border-line/60 rounded-2xl overflow-hidden bg-surface/60">
            <div class="px-4 sm:px-6 py-4 border-b border-line/60 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
              <div class="flex items-center gap-3">
                <div class="w-9 h-9 rounded-lg bg-grad-accent flex items-center justify-center font-bold text-white text-sm shadow-sm">{cache.id}</div>
                <div>
                  <h3 class="text-base font-bold text-ink">Cache #{cache.id}</h3>
                  <p class="text-xs text-faint">{cache.shards.length} shards</p>
                </div>
              </div>
              <div class="flex items-center gap-4 text-left sm:text-right">
                <div>
                  <div class="text-xs uppercase tracking-wider text-faint font-semibold">Utilization</div>
                  <div class="text-2xl font-extrabold text-grad-accent tabular-nums leading-none">{totalPct.toFixed(1)}<span class="text-base text-faint font-bold">%</span></div>
                </div>
              </div>
            </div>

            <div class="p-4 sm:p-6 space-y-5 bg-panel/40">
              <div class="grid grid-cols-1 sm:grid-cols-3 gap-3">
                <div class="glass rounded-xl border border-line/60 p-3">
                  <div class="text-xs uppercase tracking-wider text-faint font-semibold">Entries</div>
                  <div class="text-xl font-bold text-ink tabular-nums">{cache.total_entries.toLocaleString()}</div>
                </div>
                <div class="glass rounded-xl border border-line/60 p-3">
                  <div class="text-xs uppercase tracking-wider text-faint font-semibold">Capacity</div>
                  <div class="text-xl font-bold text-ink tabular-nums">{cache.total_capacity.toLocaleString()}</div>
                </div>
                <div class="glass rounded-xl border border-line/60 p-3">
                  <div class="text-xs uppercase tracking-wider text-faint font-semibold">Avg / Shard</div>
                  <div class="text-xl font-bold text-ink tabular-nums">{Math.round(cache.total_entries / shardCount).toLocaleString()}</div>
                </div>
              </div>

              <div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
                <div class="relative glass rounded-xl border border-line/60 p-3 overflow-hidden">
                  <div class="absolute -top-6 -right-6 w-16 h-16 rounded-full opacity-20 blur-xl" style="background: var(--ui-success-grad);"></div>
                  <div class="relative text-xs uppercase tracking-wider text-faint font-semibold">Cache Hits</div>
                  <div class="relative text-xl font-bold text-success-text tabular-nums">{cache.hit_total.toLocaleString()}</div>
                </div>
                <div class="relative glass rounded-xl border border-line/60 p-3 overflow-hidden">
                  <div class="absolute -top-6 -right-6 w-16 h-16 rounded-full opacity-20 blur-xl" style="background: var(--ui-warn-grad);"></div>
                  <div class="relative text-xs uppercase tracking-wider text-faint font-semibold">Cache Misses</div>
                  <div class="relative text-xl font-bold text-warn-text tabular-nums">{cache.miss_total.toLocaleString()}</div>
                </div>
                <div class="relative glass rounded-xl border border-line/60 p-3 overflow-hidden">
                  <div class="absolute -top-6 -right-6 w-16 h-16 rounded-full opacity-20 blur-xl" style="background: var(--ui-accent-grad);"></div>
                  <div class="relative text-xs uppercase tracking-wider text-faint font-semibold">Hit Rate</div>
                  <div class="relative text-xl font-bold text-grad-accent tabular-nums">{hitRate(cache.hit_total, cache.miss_total).toFixed(1)}%</div>
                </div>
              </div>

              <div>
                <div class="flex justify-between text-xs text-faint font-semibold uppercase tracking-wider">
                  <span>Total fill</span>
                  <span class="tabular-nums">{cache.total_entries.toLocaleString()} / {cache.total_capacity.toLocaleString()}</span>
                </div>
                <div class="mt-2 h-2.5 rounded-full bg-line/60 overflow-hidden">
                  <div class="h-full bg-grad-accent rounded-full transition-all duration-500" style={`width: ${Math.min(totalPct, 100)}%;`}></div>
                </div>
              </div>

              <div>
                <div class="text-xs font-semibold text-muted uppercase tracking-wider mb-3">Shard Utilization</div>
                <div class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-8 gap-3">
                  {#each cache.shards as shard (shard.index)}
                    {@const shardPct = utilization(shard.entries, shard.capacity)}
                    <div class="glass rounded-xl border border-line/60 p-3">
                      <div class="flex items-center justify-between text-xs font-semibold text-muted">
                        <span>{shardLabel(shard.index)}</span>
                        <span class="text-accent tabular-nums">{shardPct.toFixed(0)}%</span>
                      </div>
                      <div class="mt-2 h-1.5 rounded-full bg-line/60 overflow-hidden">
                        <div class="h-full bg-success-grad rounded-full transition-all duration-500" style={`width: ${Math.min(shardPct, 100)}%; background: var(--ui-success-grad);`}></div>
                      </div>
                      <div class="mt-2 text-[11px] text-faint tabular-nums">{shard.entries.toLocaleString()} / {shard.capacity.toLocaleString()}</div>
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
