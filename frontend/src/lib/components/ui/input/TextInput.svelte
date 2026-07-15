<script lang="ts">
import type { HTMLInputAttributes } from "svelte/elements";
import { type ControlSize, controlBase, controlSizes } from "./styles";

type Props = Omit<HTMLInputAttributes, "class" | "size" | "value"> & {
  class?: string;
  controlSize?: ControlSize;
  value?: HTMLInputAttributes["value"];
};

let { class: className, controlSize = "md", value = $bindable(), ...restProps }: Props = $props();
</script>

<input
  {...restProps}
  class={[controlBase, controlSizes[controlSize], className]}
  bind:value
>
