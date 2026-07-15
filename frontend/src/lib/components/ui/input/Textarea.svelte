<script lang="ts">
import type { HTMLTextareaAttributes } from "svelte/elements";
import { type ControlSize, controlBase, textareaSizes } from "./styles";

type Props = Omit<HTMLTextareaAttributes, "class" | "value"> & {
  class?: string;
  controlSize?: ControlSize;
  value?: string;
};

let { class: className, controlSize = "md", rows = 4, value = $bindable(), ...restProps }: Props = $props();
</script>

<textarea
  {...restProps}
  class={[controlBase, 'resize-y', textareaSizes[controlSize], className]}
  {rows}
  bind:value
></textarea>
