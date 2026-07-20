<script module lang="ts">
  let nextId = 0;
</script>

<script lang="ts">
  let { size = 'h-5 w-5' }: { size?: string } = $props();
  const uid = `redns-logo-${nextId++}`;
</script>

<!--
  ReDNS mark — "resolution route": a lowercase r drawn as one query path,
  from the root node (top of the stem) to the resolved record (end of the leg).
-->
<svg
  class="{size} logo-draw"
  viewBox="0 0 24 24"
  fill="none"
  aria-hidden="true"
>
  <defs>
    <linearGradient id="{uid}-route" gradientUnits="userSpaceOnUse" x1="12" y1="3.5" x2="12" y2="21">
      <stop offset="0" style="stop-color: var(--logo-route-hi)" />
      <stop offset="0.6" style="stop-color: var(--logo-route-mid)" />
      <stop offset="1" style="stop-color: var(--logo-route-lo)" />
    </linearGradient>
    <radialGradient id="{uid}-node" cx="0.35" cy="0.3" r="0.85">
      <stop offset="0" style="stop-color: var(--logo-node-hi)" />
      <stop offset="0.55" style="stop-color: var(--logo-node-mid)" />
      <stop offset="1" style="stop-color: var(--logo-node-lo)" />
    </radialGradient>
  </defs>

  <g class="logo-glow" stroke="currentColor" stroke-width="4.5" opacity="0.15" stroke-linecap="round" stroke-linejoin="round">
    <path class="logo-stem" pathLength="1" d="M8 5.5v13" />
    <path class="logo-bowl" pathLength="1" d="M8 5.5h4.5a3.25 3.25 0 0 1 0 6.5H8" />
    <path class="logo-leg" pathLength="1" d="M12.5 12l5 6.5" />
  </g>

  <g class="logo-main" stroke="url(#{uid}-route)" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
    <path class="logo-stem" pathLength="1" d="M8 5.5v13" />
    <path class="logo-bowl" pathLength="1" d="M8 5.5h4.5a3.25 3.25 0 0 1 0 6.5H8" />
    <path class="logo-leg" pathLength="1" d="M12.5 12l5 6.5" />
  </g>

  <g class="logo-node">
    <circle cx="8" cy="5.5" r="2.8" fill="currentColor" opacity="0.12" />
    <circle cx="8" cy="5.5" r="1.9" fill="url(#{uid}-node)" />
  </g>
  <g class="logo-node logo-node-end">
    <circle class="logo-halo-end" cx="17.5" cy="18.5" r="3.3" fill="currentColor" />
    <circle cx="17.5" cy="18.5" r="2.9" fill="currentColor" opacity="0.16" />
    <circle cx="17.5" cy="18.5" r="1.9" fill="url(#{uid}-node)" />
  </g>
</svg>

<style>
  .logo-draw {
    --logo-route-hi: #ffffff;
    --logo-route-mid: #eef2f6;
    --logo-route-lo: #ccd7e2;
    --logo-node-hi: #ffffff;
    --logo-node-mid: #eaeff5;
    --logo-node-lo: #b4c2d1;
    --logo-emboss: rgba(0, 0, 0, 0.6);
  }
  :global(.dark) .logo-draw {
    --logo-route-hi: #10141a;
    --logo-route-mid: #232a33;
    --logo-route-lo: #3a434e;
    --logo-node-hi: #4a5460;
    --logo-node-mid: #242b34;
    --logo-node-lo: #0c0f14;
    --logo-emboss: rgba(0, 0, 0, 0.28);
  }

  .logo-draw path {
    stroke-dasharray: 1;
    animation: logo-trace 0.45s cubic-bezier(0.33, 1, 0.68, 1) both;
  }
  .logo-stem { animation-delay: 0.05s; }
  .logo-bowl { animation-delay: 0.22s; }
  .logo-leg { animation-delay: 0.5s; }

  .logo-main {
    filter: drop-shadow(0 0.6px 0.8px var(--logo-emboss));
  }

  .logo-glow {
    transition: opacity 0.25s ease;
  }
  .logo-tile:hover .logo-glow {
    opacity: 0.28;
  }

  .logo-node {
    transform-box: fill-box;
    transform-origin: center;
    animation: logo-node-in 0.32s cubic-bezier(0.34, 1.56, 0.64, 1) both;
  }
  .logo-node:not(.logo-node-end) { animation-delay: 0s; }
  .logo-node-end { animation-delay: 0.72s; }

  .logo-halo-end {
    opacity: 0;
    transform-box: fill-box;
    transform-origin: center;
  }
  .logo-tile:hover .logo-halo-end {
    animation: logo-ripple 1.2s cubic-bezier(0.16, 1, 0.3, 1) infinite;
  }

  @keyframes logo-trace {
    from { stroke-dashoffset: 1; }
    to { stroke-dashoffset: 0; }
  }
  @keyframes logo-node-in {
    from { transform: scale(0); }
    to { transform: scale(1); }
  }
  @keyframes logo-ripple {
    0% { opacity: 0.45; transform: scale(0.5); }
    100% { opacity: 0; transform: scale(1.65); }
  }
</style>
