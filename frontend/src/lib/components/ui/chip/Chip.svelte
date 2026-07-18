<script lang="ts">
import type { Snippet } from "svelte";
import type { HTMLAttributes } from "svelte/elements";
import { type ChipTone, chipToneDots } from "./styles";

type Props = Omit<HTMLAttributes<HTMLSpanElement>, "children"> & {
  children: Snippet;
  dot?: boolean;
  tone?: ChipTone;
};

let { children, class: className, dot = false, tone = "neutral", ...restProps }: Props = $props();
</script>

<span
  {...restProps}
  class={[
    'inline-flex h-6.5 shrink-0 items-center gap-0.5 rounded-full border border-border-subtle bg-transparent text-xs font-[510] text-text-secondary',
    dot ? 'pr-2.5 pl-1.25' : 'px-2.5',
    className
  ]}
>
  {#if dot}
    <span
      aria-hidden="true"
      class="flex size-4 shrink-0 items-center justify-center"
    >
      <span class="size-1.5 rounded-full {chipToneDots[tone]}"></span>
    </span>
  {/if}
  {@render children()}
</span>
