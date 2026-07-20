<script lang="ts">
  import { onMount } from 'svelte';

  /**
   * Tweens between numeric values with an ease-out curve so stat cards count
   * up/down instead of snapping on refresh. Respects prefers-reduced-motion.
   */
  let { value, decimals = 0, duration = 550 } = $props<{
    value: number;
    decimals?: number;
    duration?: number;
  }>();

  let display = $state(0);
  let current = 0;
  let raf: number | undefined;

  const reduceMotion =
    typeof window !== 'undefined' &&
    window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;

  function format(n: number): string {
    return n.toLocaleString(undefined, {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    });
  }

  function animateTo(target: number) {
    if (reduceMotion) {
      current = target;
      display = target;
      return;
    }
    const from = current;
    if (from === target) {
      display = target;
      return;
    }
    if (raf) cancelAnimationFrame(raf);
    const start = performance.now();
    const step = (now: number) => {
      const t = Math.min(1, (now - start) / duration);
      const eased = 1 - Math.pow(1 - t, 3);
      current = from + (target - from) * eased;
      display = current;
      if (t < 1) raf = requestAnimationFrame(step);
    };
    raf = requestAnimationFrame(step);
  }

  $effect(() => {
    animateTo(value);
  });

  onMount(() => () => {
    if (raf) cancelAnimationFrame(raf);
  });
</script>

<span class="tabular-nums">{format(display)}</span>
