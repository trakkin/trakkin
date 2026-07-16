<script lang="ts">
import { Switch as SwitchPrimitive, type WithoutChildrenOrChild } from "bits-ui";

type Props = WithoutChildrenOrChild<SwitchPrimitive.RootProps>;

let { checked = $bindable(false), class: className, ...restProps }: Props = $props();
</script>

<SwitchPrimitive.Root
  {...restProps}
  class={[
    'group inline-flex h-4 w-7 shrink-0 items-center rounded-full border border-border-strong bg-white/6 p-px outline-none transition-[background-color,border-color,box-shadow] duration-(--motion-fast) ease-standard hover:border-text-muted focus-visible:border-accent/60 focus-visible:ring-2 focus-visible:ring-accent/10 data-[state=checked]:border-accent/40 data-[state=checked]:bg-accent/75 data-disabled:pointer-events-none data-disabled:opacity-35',
    className
  ]}
  bind:checked
>
  <SwitchPrimitive.Thumb
    class="block size-3 rounded-full bg-text-secondary transition-[background-color,translate,scale] duration-(--motion-standard) ease-kinetic group-active:scale-85 data-[state=checked]:translate-x-3.5 data-[state=checked]:bg-accent-contrast"
  />
</SwitchPrimitive.Root>
