<script lang="ts">
import { Tabs as TabsPrimitive } from "bits-ui";

type Props = TabsPrimitive.RootProps;

let { value = $bindable(), children, class: className, ...restProps }: Props = $props();
</script>

<TabsPrimitive.Root
  {...restProps}
  class={[
		'w-full data-[orientation=vertical]:grid data-[orientation=vertical]:grid-cols-[auto_minmax(0,1fr)]',
		className
	]}
  bind:value
>
  {@render children?.()}
</TabsPrimitive.Root>
