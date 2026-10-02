<script lang="ts">
import { Tabs as TabsPrimitive } from "bits-ui";
import { useTabsVariant } from "./context.ts";
import { tabsListClasses } from "./styles.ts";

type Props = TabsPrimitive.ListProps;

let { children, class: className, ...restProps }: Props = $props();

const variant = useTabsVariant();
</script>

<TabsPrimitive.List
  {...restProps}
  class={[tabsListClasses(variant()), className]}
>
  {@render children?.()}
</TabsPrimitive.List>
