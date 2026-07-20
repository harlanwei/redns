/**
 * Latency heatmap: map a latency value to a green→amber→red color relative to
 * the distribution of latencies on the current page, so outliers pop without
 * reading numbers. Uses a log scale because DNS latencies span orders of
 * magnitude.
 */

export type LatencyBounds = { lo: number; hi: number };

/** Compute robust bounds (p15..p95) from a set of latency samples. */
export function latencyBounds(values: number[]): LatencyBounds {
  if (values.length === 0) return { lo: 0, hi: 1 };
  const sorted = [...values].sort((a, b) => a - b);
  const pick = (p: number) => sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))];
  return { lo: pick(0.15), hi: Math.max(pick(0.95), pick(0.15) + 1) };
}

/**
 * Return an HSL color for `ms` within the given bounds. `dark` adjusts
 * lightness for contrast against the dark surface.
 */
export function latencyColor(ms: number, bounds: LatencyBounds, dark: boolean): string {
  const lo = Math.log1p(Math.max(0, bounds.lo));
  const hi = Math.log1p(Math.max(0, bounds.hi));
  const span = hi - lo;
  const t = span <= 0 ? 0 : Math.min(1, Math.max(0, (Math.log1p(Math.max(0, ms)) - lo) / span));
  // Hue sweeps 150 (green) → 45 (amber) → 8 (red).
  const hue = t < 0.5 ? 150 - (150 - 45) * (t / 0.5) : 45 - (45 - 8) * ((t - 0.5) / 0.5);
  const light = dark ? 62 : 38;
  return `hsl(${hue.toFixed(0)} 72% ${light}%)`;
}
