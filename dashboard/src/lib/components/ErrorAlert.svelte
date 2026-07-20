<script lang="ts">
  import { fade } from 'svelte/transition';

  let { message } = $props<{ message: string }>();
</script>

<div class="mb-4 flex items-start gap-3 rounded-xl border border-danger/20 bg-danger-bg p-3.5" role="alert" transition:fade>
  <span class="mt-0.5 flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-danger/10 text-danger">
    <svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
      <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clip-rule="evenodd" />
    </svg>
  </span>
  <p class="pt-1 text-sm font-medium text-danger-text">{message}</p>
</div>
