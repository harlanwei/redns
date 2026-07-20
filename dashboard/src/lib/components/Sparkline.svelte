<script module lang="ts">
  let uidCounter = 0;
</script>

<script lang="ts">
  /**
   * Tiny dependency-free sparkline. Draws a gradient-filled area + stroke line
   * with a live dot on the most recent sample.
   */
  let { values, width = 100, height = 30, stroke = 'var(--ui-accent)' } = $props<{
    values: number[];
    width?: number;
    height?: number;
    stroke?: string;
  }>();

  const uid = ++uidCounter;
  const PAD = 2;

  let points = $derived.by(() => {
    if (values.length < 2) return [];
    const min = Math.min(...values);
    const max = Math.max(...values);
    const span = max - min || 1;
    return values.map((v, i) => {
      const x = (i / (values.length - 1)) * (width - PAD * 2) + PAD;
      const y = height - PAD - ((v - min) / span) * (height - PAD * 2);
      return { x, y };
    });
  });

  let linePath = $derived(
    points.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' '),
  );
  let areaPath = $derived(
    points.length
      ? `${linePath} L${points[points.length - 1].x.toFixed(1)},${height} L${points[0].x.toFixed(1)},${height} Z`
      : '',
  );
  let last = $derived(points[points.length - 1]);
</script>

<svg {width} {height} viewBox="0 0 {width} {height}" class="overflow-visible" aria-hidden="true">
  <defs>
    <linearGradient id="spark-g{uid}" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color={stroke} stop-opacity="0.30" />
      <stop offset="100%" stop-color={stroke} stop-opacity="0" />
    </linearGradient>
  </defs>
  {#if areaPath}
    <path d={areaPath} fill="url(#spark-g{uid})" />
    <path
      d={linePath}
      fill="none"
      stroke={stroke}
      stroke-width="1.5"
      stroke-linecap="round"
      stroke-linejoin="round"
    />
    <circle cx={last.x} cy={last.y} r="2.4" fill={stroke} class="spark-dot" />
  {/if}
</svg>
