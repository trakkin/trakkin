<script lang="ts">
import { Tabs as TabsPrimitive } from "bits-ui";

type Props = TabsPrimitive.ContentProps;

let { children, class: className, ...restProps }: Props = $props();
</script>

<TabsPrimitive.Content
  {...restProps}
  class={['min-w-0 pt-3 outline-none data-[orientation=vertical]:pt-0 data-[orientation=vertical]:pl-3', className]}
>
  {@render children?.()}
</TabsPrimitive.Content>
