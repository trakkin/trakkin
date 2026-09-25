<script lang="ts">
import type { Snippet } from "svelte";

type Props = {
  title: string;
  description?: string;
  actions?: Snippet;
  below?: Snippet;
};

let { title, description, actions, below }: Props = $props();
</script>

<header class="shrink-0 bg-transparent px-4 py-3.5 shadow-[inset_0_-1px_0_var(--border-strong)] sm:px-6">
  <div class="flex min-h-9 min-w-0 flex-wrap items-center justify-between gap-x-4 gap-y-3">
    <div class="min-w-0 flex-[1_1_12rem]">
      <h1 class="text-title font-[640] wrap-anywhere text-text-primary">{title}</h1>
      {#if description}
        <p class="mt-0.5 text-label wrap-anywhere text-text-muted">{description}</p>
      {/if}
    </div>
    {#if actions}
      <div class="ml-auto flex max-w-full shrink-0 flex-wrap items-center justify-end gap-2">{@render actions()}</div>
    {/if}
  </div>
  {#if below}
    <div class="mt-3">{@render below()}</div>
  {/if}
</header>
