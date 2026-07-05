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
    return `Shard ${String(index + 1).padStart(2, '0')}`;
  }

  function hitRate(hits: number, misses: number) {
    const total = hits + misses;
    return total > 0 ? (hits / total) * 100 : 0;
  }

  // Color the fill bar by pressure: calm below 75%, warming toward capacity.
  function fillClass(pct: number) {
    if (pct >= 90) return 'bg-danger-2';
    if (pct >= 75) return 'bg-warn-2';
    return 'bg-accent';
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

<div class="space-y-5" in:fade>
  {#if caches.length > 0}
    <section class="grid gap-3 sm:grid-cols-2 lg:grid-cols-4" aria-label="Cache summary">
      <div class="rounded-xl border border-line bg-surface p-4 shadow-soft">
        <div class="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted">Instances</div>
        <div class="mt-2 font-mono text-2xl font-semibold text-ink tabular-nums">{caches.length.toLocaleString()}</div>
        <div class="mt-1 text-xs text-faint">Cache pools</div>
      </div>
      <div class="rounded-xl border border-line bg-surface p-4 shadow-soft">
        <div class="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted">Entries</div>
        <div class="mt-2 font-mono text-2xl font-semibold text-ink tabular-nums">{cacheTotalEntries.toLocaleString()}</div>
        <div class="mt-1 text-xs text-faint">of {cacheTotalCapacity.toLocaleString()} capacity</div>
      </div>
      <div class="rounded-xl border border-line bg-surface p-4 shadow-soft">
        <div class="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted">Occupancy</div>
        <div class="mt-2 font-mono text-2xl font-semibold text-ink tabular-nums">{utilization(cacheTotalEntries, cacheTotalCapacity).toFixed(1)}<span class="ml-0.5 text-sm font-medium text-faint">%</span></div>
        <div class="mt-1 text-xs text-faint">Total fill</div>
      </div>
      <div class="relative overflow-hidden rounded-xl bg-header p-4 text-header-text shadow-card">
        <div class="pointer-events-none absolute -right-6 -top-8 h-24 w-24 rounded-full bg-[radial-gradient(circle,color-mix(in_srgb,var(--ui-accent)_60%,transparent),transparent_70%)]" aria-hidden="true"></div>
        <div class="relative text-[11px] font-semibold uppercase tracking-[0.08em] text-accent-3">Hit rate</div>
        <div class="relative mt-2 font-mono text-2xl font-semibold text-white tabular-nums">{hitRate(cacheTotalHits, cacheTotalMisses).toFixed(1)}<span class="ml-0.5 text-sm font-medium text-header-muted">%</span></div>
        <div class="relative mt-1 text-xs text-header-muted">{cacheTotalHits.toLocaleString()} hits</div>
      </div>
    </section>
  {/if}

  <section class="overflow-hidden rounded-xl border border-line bg-surface shadow-card" aria-label="Cache utilization">
    <div class="flex flex-col gap-3 border-b border-line bg-panel p-4 sm:flex-row sm:items-center sm:justify-between sm:p-5">
      <div>
        <h2 class="text-base font-bold text-ink">Cache utilization</h2>
        <p class="mt-0.5 text-sm text-muted">Sharded occupancy and balance across pools.</p>
      </div>
      <button onclick={fetchCache} class="inline-flex items-center gap-1.5 rounded-lg border border-accent/25 bg-accent-soft px-3 py-2 text-sm font-semibold text-accent-2 hover:bg-accent-fill hover:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-accent active:scale-[0.98]">
        <svg class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
        Refresh
      </button>
    </div>

    {#if loading && caches.length === 0}
      <div class="space-y-6 p-4 sm:p-6">
        {#each Array(1) as _}
          <div class="overflow-hidden rounded-xl border border-line">
            <div class="flex justify-between border-b border-line/60 px-4 py-4 sm:px-6">
              <div class="skeleton h-9 w-40 rounded"></div>
              <div class="skeleton h-9 w-20 rounded"></div>
            </div>
            <div class="grid grid-cols-3 gap-3 p-4 sm:p-6">
              {#each Array(6) as _}<div class="skeleton h-20 rounded-lg"></div>{/each}
            </div>
          </div>
        {/each}
      </div>
    {:else if caches.length === 0}
      <div class="px-6 py-16 text-center">
        <div class="inline-flex flex-col items-center gap-3 text-faint">
          <div class="flex h-14 w-14 items-center justify-center rounded-xl border border-accent/20 bg-accent-soft text-accent-2">
            <svg class="h-7 w-7" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5"><path stroke-linecap="round" stroke-linejoin="round" d="M4 7c0-1.1 3.58-2 8-2s8 .9 8 2-3.58 2-8 2-8-.9-8-2zm0 0v10c0 1.1 3.58 2 8 2s8-.9 8-2V7"/></svg>
          </div>
          <span class="text-sm font-medium">No cache instances found</span>
        </div>
      </div>
    {:else}
      <div class="divide-y divide-line">
        {#each caches as cache (cache.id)}
          {@const totalPct = utilization(cache.total_entries, cache.total_capacity)}
          {@const shardCount = Math.max(cache.shards.length, 1)}
          <article class="bg-surface">
            <div class="flex flex-col gap-3 border-b border-line px-4 py-4 sm:flex-row sm:items-center sm:justify-between sm:px-6">
              <div class="flex items-center gap-3">
                <div class="flex h-9 w-9 items-center justify-center rounded-lg bg-accent-fill text-sm font-bold text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.2)] tabular-nums">{cache.id}</div>
                <div>
                  <h3 class="text-base font-bold text-ink">Cache #{cache.id}</h3>
                  <p class="text-xs text-faint tabular-nums">{cache.shards.length} shards</p>
                </div>
              </div>
              <div class="text-left sm:text-right">
                <div class="text-[11px] font-semibold uppercase tracking-[0.06em] text-faint">Utilization</div>
                <div class="font-mono text-2xl font-semibold leading-none text-ink tabular-nums">{totalPct.toFixed(1)}<span class="text-base font-medium text-faint">%</span></div>
              </div>
            </div>

            <div class="space-y-5 bg-panel/40 p-4 sm:p-6">
              <div class="grid grid-cols-1 overflow-hidden rounded-xl border border-line bg-surface sm:grid-cols-3 sm:divide-x sm:divide-line">
                <div class="p-3.5">
                  <div class="text-[11px] font-semibold uppercase tracking-[0.06em] text-faint">Entries</div>
                  <div class="mt-1 font-mono text-xl font-semibold text-ink tabular-nums">{cache.total_entries.toLocaleString()}</div>
                </div>
                <div class="border-t border-line p-3.5 sm:border-t-0">
                  <div class="text-[11px] font-semibold uppercase tracking-[0.06em] text-faint">Capacity</div>
                  <div class="mt-1 font-mono text-xl font-semibold text-ink tabular-nums">{cache.total_capacity.toLocaleString()}</div>
                </div>
                <div class="border-t border-line p-3.5 sm:border-t-0">
                  <div class="text-[11px] font-semibold uppercase tracking-[0.06em] text-faint">Avg / shard</div>
                  <div class="mt-1 font-mono text-xl font-semibold text-ink tabular-nums">{Math.round(cache.total_entries / shardCount).toLocaleString()}</div>
                </div>
              </div>

              <div class="grid grid-cols-1 overflow-hidden rounded-xl border border-line bg-surface sm:grid-cols-3 sm:divide-x sm:divide-line">
                <div class="p-3.5">
                  <div class="text-[11px] font-semibold uppercase tracking-[0.06em] text-faint">Cache hits</div>
                  <div class="mt-1 font-mono text-xl font-semibold text-success-text tabular-nums">{cache.hit_total.toLocaleString()}</div>
                </div>
                <div class="border-t border-line p-3.5 sm:border-t-0">
                  <div class="text-[11px] font-semibold uppercase tracking-[0.06em] text-faint">Cache misses</div>
                  <div class="mt-1 font-mono text-xl font-semibold text-warn-text tabular-nums">{cache.miss_total.toLocaleString()}</div>
                </div>
                <div class="border-t border-line p-3.5 sm:border-t-0">
                  <div class="text-[11px] font-semibold uppercase tracking-[0.06em] text-faint">Hit rate</div>
                  <div class="mt-1 font-mono text-xl font-semibold text-ink tabular-nums">{hitRate(cache.hit_total, cache.miss_total).toFixed(1)}%</div>
                </div>
              </div>

              <div>
                <div class="flex justify-between text-[11px] font-semibold uppercase tracking-[0.06em] text-faint">
                  <span>Total fill</span>
                  <span class="font-mono text-muted tabular-nums">{cache.total_entries.toLocaleString()} / {cache.total_capacity.toLocaleString()}</span>
                </div>
                <div class="mt-2 h-2.5 overflow-hidden rounded-full bg-line/60">
                  <div class="h-full rounded-full transition-all duration-500 {fillClass(totalPct)}" style={`width: ${Math.min(totalPct, 100)}%;`}></div>
                </div>
              </div>

              <div>
                <div class="mb-3 text-[11px] font-semibold uppercase tracking-[0.06em] text-muted">Shard utilization</div>
                <div class="grid grid-cols-2 gap-3 sm:grid-cols-4 lg:grid-cols-8">
                  {#each cache.shards as shard (shard.index)}
                    {@const shardPct = utilization(shard.entries, shard.capacity)}
                    <div class="rounded-lg border border-line bg-surface p-3">
                      <div class="flex items-center justify-between text-[11px] font-semibold text-muted">
                        <span>{shardLabel(shard.index)}</span>
                        <span class="font-mono tabular-nums {shardPct >= 90 ? 'text-danger-text' : shardPct >= 75 ? 'text-warn-text' : 'text-accent-2'}">{shardPct.toFixed(0)}%</span>
                      </div>
                      <div class="mt-2 h-1.5 overflow-hidden rounded-full bg-line/60">
                        <div class="h-full rounded-full transition-all duration-500 {fillClass(shardPct)}" style={`width: ${Math.min(shardPct, 100)}%;`}></div>
                      </div>
                      <div class="mt-2 font-mono text-[11px] text-faint tabular-nums">{shard.entries.toLocaleString()} / {shard.capacity.toLocaleString()}</div>
                    </div>
                  {/each}
                </div>
              </div>
            </div>
          </article>
        {/each}
      </div>
    {/if}
  </section>
</div>
