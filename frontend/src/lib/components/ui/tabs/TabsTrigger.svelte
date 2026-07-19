<script lang="ts">
import { Tabs as TabsPrimitive } from "bits-ui";

type Props = TabsPrimitive.TriggerProps;

let { children, class: className, ...restProps }: Props = $props();
</script>

<TabsPrimitive.Trigger
  {...restProps}
  class={[
		'inline-flex h-7 items-center justify-center rounded-md px-2.5 text-label font-[510] text-text-muted outline-none transition-[background-color,color,box-shadow] duration-(--motion-standard) ease-standard hover:bg-white/3 hover:text-text-secondary focus-visible:ring-2 focus-visible:ring-accent/10 data-[state=active]:bg-white/5 data-[state=active]:text-text-secondary data-[orientation=vertical]:w-full data-[orientation=vertical]:justify-start data-disabled:pointer-events-none data-disabled:opacity-35',
		className
	]}
>
  {@render children?.()}
</TabsPrimitive.Trigger>
