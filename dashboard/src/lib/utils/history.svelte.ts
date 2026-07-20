/**
 * Module-level rolling history of headline metrics, used to draw sparklines in
 * the stat cards. Kept at module scope (not per-component) so history survives
 * tab switches and auto-refresh ticks accumulate a real trend.
 */
const MAX_POINTS = 40;

export const metricHistory = $state<Record<string, number[]>>({});

/** Append a sample to a metric's rolling history (capped at MAX_POINTS). */
export function pushMetric(key: string, value: number) {
  if (!Number.isFinite(value)) return;
  let arr = metricHistory[key];
  if (!arr) arr = metricHistory[key] = [];
  arr.push(value);
  if (arr.length > MAX_POINTS) arr.shift();
}
