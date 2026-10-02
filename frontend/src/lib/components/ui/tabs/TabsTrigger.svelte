<script lang="ts">
import { Tabs as TabsPrimitive } from "bits-ui";
import { useTabsVariant } from "./context.ts";
import { tabsTriggerClasses } from "./styles.ts";

type Props = TabsPrimitive.TriggerProps;

let { children, class: className, ...restProps }: Props = $props();

const variant = useTabsVariant();
</script>

<TabsPrimitive.Trigger
  {...restProps}
  class={[tabsTriggerClasses(variant()), className]}
>
  {@render children?.()}
</TabsPrimitive.Trigger>
