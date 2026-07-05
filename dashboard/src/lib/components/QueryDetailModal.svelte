<script lang="ts">
  import { fade, slide } from 'svelte/transition';
  import type { DnsLogEntry } from '../types/dashboard';
  import { formatProtocol, parseAnswer, formatUpstream } from '../utils/dashboard';

  let { log, onClose } = $props<{
    log: DnsLogEntry | null;
    onClose: () => void;
  }>();

  let geoipData = $state<Record<string, { city: string | null; asn: string | null; isp: string | null; proxy: boolean | null; hosting: boolean | null }>>({});

  // Portal the dialog to <body> on mount. `position: fixed` resolves against
  // the nearest ancestor with a transform/filter/backdrop-filter/etc., and the
  // dashboard wraps the views in `animate-rise` (whose fill-mode holds a
  // transform) — without portaling, the overlay gets caged inside that wrapper
  // instead of covering the viewport. Restoring the node on destroy keeps
  // Svelte's transition teardown correct.
  function portal(node: HTMLElement) {
    document.body.appendChild(node);
    return {
      destroy() {
        node.remove();
      },
    };
  }

  // Whenever the selected log changes, refresh geoip data for its client IP and
  // any A/AAAA answer values.
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

{#if log}
  <div class="fixed inset-0 z-50 overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true" transition:fade={{ duration: 150 }} use:portal>
    <div class="flex items-end justify-center min-h-screen pt-4 px-2 pb-4 text-left sm:block sm:p-0 sm:text-center">
      <div class="fixed inset-0 z-0 bg-ink/70" aria-hidden="true" onclick={onClose}></div>
      <span class="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>
      <div class="relative z-10 inline-block w-full align-bottom bg-surface rounded-md text-left overflow-hidden shadow-lift transform transition-all sm:my-8 sm:align-middle sm:max-w-2xl sm:w-full border border-line" transition:slide={{ duration: 200 }}>
        <div class="bg-header text-header-text px-4 pt-5 pb-5 sm:px-6 relative overflow-hidden">
          <div class="relative sm:flex sm:items-start">
            <div class="mt-3 text-left sm:mt-0 w-full">
              <h3 class="text-xl leading-6 font-bold flex items-center gap-2" id="modal-title">
                Query Results
              </h3>
              <div class="mt-2 text-sm text-header-muted flex gap-2 items-center">
                <span class="inline-flex items-center px-2 py-0.5 rounded-md text-xs font-bold bg-white/10 text-white border border-white/15">
                  {log?.qtype}
                </span>
                <span class="font-mono text-white/90 truncate max-w-sm">{log?.qname}</span>
              </div>
            </div>
          </div>
        </div>

        <div class="bg-surface px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
          <div class="flex flex-col gap-3 mb-4 text-sm bg-panel p-4 rounded-md border border-line">
            <div class="flex flex-col sm:flex-row sm:justify-between sm:items-center border-b border-line/60 pb-2">
              <div class="text-muted font-medium mb-1 sm:mb-0">Time</div>
              <div class="font-medium text-ink text-left sm:text-right">{new Date(log.ts_unix_ms).toLocaleString()}</div>
            </div>
            <div class="flex flex-col sm:flex-row sm:justify-between sm:items-center border-b border-line/60 pb-2">
              <div class="text-muted font-medium mb-1 sm:mb-0">Client</div>
              <div class="text-left sm:text-right">
                <div class="font-medium text-ink">{log?.client_ip} <span class="text-faint text-xs font-normal">({formatProtocol(log?.protocol || '')})</span></div>
                {#if geoipData[log?.client_ip || '']}
                  {#if geoipData[log?.client_ip || '']?.city || geoipData[log?.client_ip || '']?.asn || geoipData[log?.client_ip || '']?.isp || geoipData[log?.client_ip || '']?.proxy || geoipData[log?.client_ip || '']?.hosting}
                    <div class="mt-1 flex flex-wrap sm:justify-end gap-2 text-xs font-sans">
                      {#if geoipData[log?.client_ip || '']?.city}
                        <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-info-bg text-info-text border border-info/20">
                          <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 11a3 3 0 11-6 0 3 3 0 016 0z"></path></svg>
                          {geoipData[log?.client_ip || '']?.city}
                        </span>
                      {/if}
                      {#if geoipData[log?.client_ip || '']?.asn}
                        <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-neutral-bg text-neutral-text border border-line">
                          <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9"></path></svg>
                          {geoipData[log?.client_ip || '']?.asn}
                        </span>
                      {/if}
                      {#if geoipData[log?.client_ip || '']?.isp}
                        <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-info-bg text-info-text border border-info/20">
                          <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
                          {geoipData[log?.client_ip || '']?.isp}
                        </span>
                      {/if}
                      {#if geoipData[log?.client_ip || '']?.proxy}
                        <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-danger-bg text-danger-text border border-danger/20">
                          <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 11c0 3.517-1.009 6.799-2.753 9.571m-3.44-2.04l.054-.09A13.916 13.916 0 008 11a4 4 0 118 0c0 1.017-.07 2.019-.203 3m-2.118 6.844A21.88 21.88 0 0015.171 17m3.839 1.132c.645-2.266.99-4.659.99-7.132A8 8 0 008 4.07M3 15.364c.64-1.319 1-2.8 1-4.364 0-1.457.39-2.823 1.07-4"></path></svg>
                          Proxy
                        </span>
                      {/if}
                      {#if geoipData[log?.client_ip || '']?.hosting}
                        <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-warn-bg text-warn-text border border-warn/20">
                          <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01"></path></svg>
                          Hosting
                        </span>
                      {/if}
                    </div>
                  {/if}
                {/if}
              </div>
            </div>
            <div class="flex flex-col sm:flex-row sm:justify-between sm:items-center border-b border-line/60 pb-2">
              <div class="text-muted font-medium mb-1 sm:mb-0">Status</div>
              <div class="text-left sm:text-right">
                <span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-md text-xs font-semibold {log?.rcode?.toLowerCase() === 'noerror' ? 'bg-success-bg text-success-text' : 'bg-danger-bg text-danger-text'}">
                  <span class="w-1.5 h-1.5 rounded-full {log?.rcode?.toLowerCase() === 'noerror' ? 'bg-success-2' : 'bg-danger-2'}"></span>
                  {log?.rcode}
                </span>
              </div>
            </div>
            <div class="flex flex-col sm:flex-row sm:justify-between sm:items-center border-b border-line/60 pb-2">
              <div class="text-muted font-medium mb-1 sm:mb-0">Latency</div>
              <div class="font-medium text-ink text-left sm:text-right">{log?.latency_ms}<span class="text-faint text-xs font-normal ml-0.5">ms</span></div>
            </div>
            {#if (log?.upstreams?.length ?? 0) > 0}
              <div class="flex flex-col sm:flex-row sm:justify-between sm:items-start">
                <div class="text-muted font-medium mb-1 sm:mb-0 mt-1">Upstream</div>
                <div class="flex flex-wrap gap-1">
                  {#each log?.upstreams ?? [] as upstream}
                    {#if upstream === '__C__'}
                      <span class="inline-flex items-center px-2 py-0.5 rounded-md text-xs font-semibold bg-info-bg text-info-text ring-1 ring-inset ring-info/20">System Cache</span>
                    {:else}
                      <span class="inline-flex items-center px-2 py-0.5 rounded-md text-xs font-semibold bg-neutral-bg text-neutral-text ring-1 ring-inset ring-line-2">{formatUpstream(upstream)}</span>
                    {/if}
                  {/each}
                </div>
              </div>
            {/if}
          </div>

          <div class="mt-4 border border-line/60 rounded-md overflow-hidden">
            <table class="min-w-full divide-y divide-line/60">
              <thead class="bg-panel">
                <tr>
                  <th scope="col" class="px-6 py-3 text-left text-xs font-semibold text-muted uppercase tracking-wider w-1/6">Type</th>
                  <th scope="col" class="px-6 py-3 text-left text-xs font-semibold text-muted uppercase tracking-wider">Value</th>
                  <th scope="col" class="px-6 py-3 text-right text-xs font-semibold text-muted uppercase tracking-wider w-1/6">TTL</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-line/60">
                {#each log?.result_rows || [] as row}
                  {@const parsed = parseAnswer(row)}
                  <tr class="hover:bg-accent-soft/50">
                    <td class="px-6 py-3 whitespace-nowrap text-left text-sm text-muted font-medium">{parsed.type}</td>
                    <td class="px-6 py-3 text-left text-sm text-ink font-mono break-all">
                      {parsed.value}
                      {#if (parsed.type === 'A' || parsed.type === 'AAAA') && geoipData[parsed.value]}
                        {#if geoipData[parsed.value]?.city || geoipData[parsed.value]?.asn || geoipData[parsed.value]?.isp || geoipData[parsed.value]?.proxy || geoipData[parsed.value]?.hosting}
                          <div class="mt-1 flex flex-wrap gap-2 text-xs font-sans">
                            {#if geoipData[parsed.value]?.city}
                              <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-info-bg text-info-text border border-info/20">
                                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 11a3 3 0 11-6 0 3 3 0 016 0z"></path></svg>
                                {geoipData[parsed.value]?.city}
                              </span>
                            {/if}
                            {#if geoipData[parsed.value]?.asn}
                              <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-neutral-bg text-neutral-text border border-line">
                                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9"></path></svg>
                                {geoipData[parsed.value]?.asn}
                              </span>
                            {/if}
                            {#if geoipData[parsed.value]?.isp}
                              <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-info-bg text-info-text border border-info/20">
                                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
                                {geoipData[parsed.value]?.isp}
                              </span>
                            {/if}
                            {#if geoipData[parsed.value]?.proxy}
                              <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-danger-bg text-danger-text border border-danger/20">
                                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 11c0 3.517-1.009 6.799-2.753 9.571m-3.44-2.04l.054-.09A13.916 13.916 0 008 11a4 4 0 118 0c0 1.017-.07 2.019-.203 3m-2.118 6.844A21.88 21.88 0 0015.171 17m3.839 1.132c.645-2.266.99-4.659.99-7.132A8 8 0 008 4.07M3 15.364c.64-1.319 1-2.8 1-4.364 0-1.457.39-2.823 1.07-4"></path></svg>
                                Proxy
                              </span>
                            {/if}
                            {#if geoipData[parsed.value]?.hosting}
                              <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-warn-bg text-warn-text border border-warn/20">
                                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01"></path></svg>
                                Hosting
                              </span>
                            {/if}
                          </div>
                        {/if}
                      {/if}
                    </td>
                    <td class="px-6 py-3 text-right whitespace-nowrap text-sm text-muted font-medium tabular-nums">
                      {#if parsed.ttl !== undefined}
                        {parsed.ttl}<span class="text-faint text-xs ml-0.5">s</span>
                      {:else}
                        <span class="text-faint">—</span>
                      {/if}
                    </td>
                  </tr>
                {/each}
                {#if (log?.result_rows || []).length === 0}
                  <tr><td colspan="3" class="px-6 py-8 text-sm text-faint text-center italic">No answers recorded</td></tr>
                {/if}
              </tbody>
            </table>
          </div>
        </div>
        <div class="bg-panel px-4 py-3 sm:px-6 sm:flex sm:flex-row-reverse border-t border-line">
          <button type="button" class="mt-3 w-full inline-flex justify-center rounded-md border border-line shadow-soft px-4 py-2 bg-surface text-base font-medium text-ink hover:bg-panel focus:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-surface focus-visible:ring-accent sm:mt-0 sm:ml-3 sm:w-auto sm:text-sm transition-colors" onclick={onClose}>Close</button>
        </div>
      </div>
    </div>
  </div>
{/if}
