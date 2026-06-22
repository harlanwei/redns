<script lang="ts">
  import { fade } from 'svelte/transition';

  let { message } = $props<{ message: string }>();
</script>

<div class="relative bg-danger-bg mb-6 rounded-2xl shadow-card overflow-hidden" transition:fade>
  <div class="absolute inset-y-0 left-0 w-1" style="background: var(--ui-danger-grad);"></div>
  <div class="flex p-4 pl-5">
    <div class="flex-shrink-0">
      <svg class="h-5 w-5 text-danger" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
        <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clip-rule="evenodd" />
      </svg>
    </div>
    <div class="ml-3">
      <p class="text-sm font-medium text-danger-text">{message}</p>
    </div>
  </div>
</div>
