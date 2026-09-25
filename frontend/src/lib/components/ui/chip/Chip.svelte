<script lang="ts">
import type { Snippet } from "svelte";
import type { HTMLAttributes } from "svelte/elements";
import { type ChipTone, chipTones } from "./styles";

type Props = Omit<HTMLAttributes<HTMLSpanElement>, "children"> & {
  children: Snippet;
  status?: boolean;
  tone?: ChipTone;
};

let { children, class: className, status = false, tone = "neutral", ...restProps }: Props = $props();
</script>

<span
  {...restProps}
  class={[
    "inline-flex h-6 shrink-0 items-center rounded-control border px-2 text-meta font-[560]",
    chipTones[status || tone !== "neutral" ? tone : "neutral"],
    className
  ]}
>
  {@render children()}
</span>
