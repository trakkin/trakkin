<script lang="ts">
import type { Snippet } from "svelte";
import type { HTMLAttributes } from "svelte/elements";
import { type PanelTone, panelBase, panelTones } from "./styles";

type Props = Omit<HTMLAttributes<HTMLDivElement>, "children"> & {
  children: Snippet;
  element?: "div" | "section";
  tone?: PanelTone;
};

let { children, class: className, element = "div", tone = "default", ...restProps }: Props = $props();
</script>

<svelte:element
  {...restProps}
  class={[panelBase, panelTones[tone], className]}
  this={element}
>
  {@render children()}
</svelte:element>
