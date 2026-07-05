<script lang="ts">
  import { fade } from 'svelte/transition';

  let { message } = $props<{ message: string }>();
</script>

<div class="mb-6 flex items-start gap-3 overflow-hidden rounded-xl border border-danger/25 bg-danger-bg p-4 shadow-soft" role="alert" transition:fade>
  <span class="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-danger/10 text-danger">
    <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
      <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clip-rule="evenodd" />
    </svg>
  </span>
  <div class="min-w-0 pt-1">
    <p class="text-sm font-semibold text-danger-text">{message}</p>
  </div>
</div>
