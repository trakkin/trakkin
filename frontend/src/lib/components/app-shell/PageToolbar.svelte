<script lang="ts">
import type { Snippet } from "svelte";

type Props = {
  label: string;
  children: Snippet;
};

let { label, children }: Props = $props();
</script>

<section
  aria-label={label}
  class="shrink-0 border-b border-border-strong bg-surface-raised/35 px-4 py-3 sm:px-6"
>
  {@render children()}
</section>
