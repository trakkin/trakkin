<script lang="ts">
import { Tabs as TabsPrimitive } from "bits-ui";

type Props = TabsPrimitive.ListProps;

let { children, class: className, ...restProps }: Props = $props();
</script>

<TabsPrimitive.List
  {...restProps}
  class={[
		'inline-flex min-h-8 items-center gap-0.5 data-[orientation=vertical]:min-w-40 data-[orientation=vertical]:flex-col data-[orientation=vertical]:items-stretch',
		className
	]}
>
  {@render children?.()}
</TabsPrimitive.List>
