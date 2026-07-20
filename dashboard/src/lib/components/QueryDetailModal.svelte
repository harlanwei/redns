<script lang="ts">
  import { fade, fly } from 'svelte/transition';
  import type { DnsLogEntry } from '../types/dashboard';
  import { formatProtocol, parseAnswer, formatUpstream } from '../utils/dashboard';

  let { log, onClose } = $props<{
    log: DnsLogEntry | null;
    onClose: () => void;
  }>();

  let geoipData = $state<Record<string, { city: string | null; asn: string | null; isp: string | null; proxy: boolean | null; hosting: boolean | null }>>({});

  function portal(node: HTMLElement) {
    document.body.appendChild(node);
    return {
      destroy() {
        node.remove();
      },
    };
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === 'Escape') onClose();
  }

  $effect(() => {
    if (!log) {
      geoipData = {};
      return;
    }
    geoipData = {};
    const ipsToFetch = new Set<string>();
    if (log.client_ip) ipsToFetch.add(log.client_ip);
    for (const row of log.result_rows || []) {
      const parsed = parseAnswer(row);
      if (parsed.type === 'A' || parsed.type === 'AAAA') {
        ipsToFetch.add(parsed.value);
      }
    }
    for (const ip of ipsToFetch) {
      fetch(`/api/geoip?ip=${encodeURIComponent(ip)}`)
        .then((res) => res.json())
        .then((data) => {
          geoipData[ip] = data;
        })
        .catch((err) => console.error('Failed to fetch geoip for', ip, err));
    }
  });
</script>

<svelte:window onkeydown={handleKeydown} />

{#if log}
  <div
    class="fixed inset-0 z-50 flex items-end justify-center p-0 sm:items-center sm:p-4"
    role="dialog"
    aria-modal="true"
    aria-labelledby="modal-title"
    transition:fade={{ duration: 120 }}
    use:portal
  >
    <div class="absolute inset-0 bg-ink/50" aria-hidden="true" onclick={onClose}></div>

    <div
      class="relative z-10 flex max-h-[92dvh] w-full max-w-xl flex-col overflow-hidden rounded-t-2xl border border-line bg-surface shadow-lift sm:rounded-2xl"
      transition:fly={{ y: 16, duration: 180 }}
    >
      <div class="flex items-start justify-between gap-3 border-b border-line px-4 py-4 sm:px-5">
        <div class="min-w-0">
          <div class="flex flex-wrap items-center gap-2">
            <span class="chip chip-accent font-mono">{log.qtype}</span>
            <span class="chip {log.rcode?.toLowerCase() === 'noerror' ? 'chip-success' : 'chip-danger'}">{log.rcode}</span>
          </div>
          <h3 id="modal-title" class="mt-2 break-all text-base font-semibold tracking-tight text-ink sm:text-lg">
            {log.qname}
          </h3>
        </div>
        <button onclick={onClose} class="btn btn-ghost !px-2" aria-label="Close">
          <svg class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.75"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12" /></svg>
        </button>
      </div>

      <div class="overflow-y-auto px-4 py-4 sm:px-5">
        <dl class="grid grid-cols-1 gap-3 text-sm sm:grid-cols-2">
          <div class="rounded-lg bg-panel/70 px-3 py-2.5">
            <dt class="text-xs text-muted">Time</dt>
            <dd class="mt-0.5 font-medium text-ink">{new Date(log.ts_unix_ms).toLocaleString()}</dd>
          </div>
          <div class="rounded-lg bg-panel/70 px-3 py-2.5">
            <dt class="text-xs text-muted">Latency</dt>
            <dd class="mt-0.5 font-mono font-medium text-ink tabular-nums">{log.latency_ms}<span class="ml-0.5 text-xs font-normal text-faint">ms</span></dd>
          </div>
          <div class="rounded-lg bg-panel/70 px-3 py-2.5 sm:col-span-2">
            <dt class="text-xs text-muted">Client</dt>
            <dd class="mt-0.5 font-medium text-ink">
              {log.client_ip}
              <span class="text-xs font-normal text-faint">({formatProtocol(log.protocol || '')})</span>
            </dd>
            {#if geoipData[log.client_ip || '']}
              {#if geoipData[log.client_ip || '']?.city || geoipData[log.client_ip || '']?.asn || geoipData[log.client_ip || '']?.isp || geoipData[log.client_ip || '']?.proxy || geoipData[log.client_ip || '']?.hosting}
                <div class="mt-2 flex flex-wrap gap-1.5">
                  {#if geoipData[log.client_ip || '']?.city}
                    <span class="chip chip-info">{geoipData[log.client_ip || '']?.city}</span>
                  {/if}
                  {#if geoipData[log.client_ip || '']?.asn}
                    <span class="chip chip-neutral">{geoipData[log.client_ip || '']?.asn}</span>
                  {/if}
                  {#if geoipData[log.client_ip || '']?.isp}
                    <span class="chip chip-info">{geoipData[log.client_ip || '']?.isp}</span>
                  {/if}
                  {#if geoipData[log.client_ip || '']?.proxy}
                    <span class="chip chip-danger">Proxy</span>
                  {/if}
                  {#if geoipData[log.client_ip || '']?.hosting}
                    <span class="chip chip-warn">Hosting</span>
                  {/if}
                </div>
              {/if}
            {/if}
          </div>
          {#if (log.upstreams?.length ?? 0) > 0}
            <div class="rounded-lg bg-panel/70 px-3 py-2.5 sm:col-span-2">
              <dt class="text-xs text-muted">Upstream</dt>
              <dd class="mt-1.5 flex flex-wrap gap-1.5">
                {#each log.upstreams ?? [] as upstream}
                  {#if upstream === '__C__'}
                    <span class="chip chip-info">System Cache</span>
                  {:else}
                    <span class="chip chip-neutral">{formatUpstream(upstream)}</span>
                  {/if}
                {/each}
              </dd>
            </div>
          {/if}
        </dl>

        <div class="mt-4 overflow-hidden rounded-xl border border-line">
          <table class="data-table">
            <colgroup>
              <col class="w-[4.5rem]" />
              <col />
              <col class="w-[4.5rem]" />
            </colgroup>
            <thead>
              <tr>
                <th scope="col">Type</th>
                <th scope="col">Value</th>
                <th scope="col" class="!text-right">TTL</th>
              </tr>
            </thead>
            <tbody>
              {#each log.result_rows || [] as row}
                {@const parsed = parseAnswer(row)}
                <tr>
                  <td class="text-sm font-medium text-muted">{parsed.type}</td>
                  <td class="font-mono text-sm text-ink" title={parsed.value}>
                    <span class="cell-clip">{parsed.value}</span>
                    {#if (parsed.type === 'A' || parsed.type === 'AAAA') && geoipData[parsed.value]}
                      {#if geoipData[parsed.value]?.city || geoipData[parsed.value]?.asn || geoipData[parsed.value]?.isp || geoipData[parsed.value]?.proxy || geoipData[parsed.value]?.hosting}
                        <div class="mt-1.5 flex flex-wrap gap-1.5 font-sans !whitespace-normal">
                          {#if geoipData[parsed.value]?.city}
                            <span class="chip chip-info">{geoipData[parsed.value]?.city}</span>
                          {/if}
                          {#if geoipData[parsed.value]?.asn}
                            <span class="chip chip-neutral">{geoipData[parsed.value]?.asn}</span>
                          {/if}
                          {#if geoipData[parsed.value]?.isp}
                            <span class="chip chip-info">{geoipData[parsed.value]?.isp}</span>
                          {/if}
                          {#if geoipData[parsed.value]?.proxy}
                            <span class="chip chip-danger">Proxy</span>
                          {/if}
                          {#if geoipData[parsed.value]?.hosting}
                            <span class="chip chip-warn">Hosting</span>
                          {/if}
                        </div>
                      {/if}
                    {/if}
                  </td>
                  <td class="text-right text-sm font-medium text-muted tabular-nums">
                    {#if parsed.ttl !== undefined}
                      {parsed.ttl}<span class="ml-0.5 text-xs text-faint">s</span>
                    {:else}
                      <span class="text-faint">—</span>
                    {/if}
                  </td>
                </tr>
              {/each}
              {#if (log.result_rows || []).length === 0}
                <tr>
                  <td colspan="3" class="!overflow-visible !whitespace-normal py-8 text-center text-sm text-faint">No answers recorded</td>
                </tr>
              {/if}
            </tbody>
          </table>
        </div>
      </div>

      <div class="border-t border-line px-4 py-3 sm:px-5 sm:text-right">
        <button type="button" class="btn btn-secondary w-full sm:w-auto" onclick={onClose}>Close</button>
      </div>
    </div>
  </div>
{/if}
