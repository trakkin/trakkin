<script lang="ts">
import { Tabs as TabsPrimitive } from "bits-ui";
import { provideTabsVariant } from "./context.ts";
import type { TabsVariant } from "./styles.ts";

type Props = TabsPrimitive.RootProps & {
  variant?: TabsVariant;
};

let { value = $bindable(), children, class: className, variant = "underline", ...restProps }: Props = $props();

provideTabsVariant(() => variant);
</script>

<TabsPrimitive.Root
  {...restProps}
  class={[
    "w-full data-[orientation=vertical]:grid data-[orientation=vertical]:grid-cols-[auto_minmax(0,1fr)]",
    className,
  ]}
  bind:value
>
  {@render children?.()}
</TabsPrimitive.Root>
