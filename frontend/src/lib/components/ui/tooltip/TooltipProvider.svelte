<script lang="ts">
import { Tooltip as TooltipPrimitive } from "bits-ui";

type Props = TooltipPrimitive.ProviderProps;

let {
  children,
  delayDuration = 500,
  disableHoverableContent = true,
  skipDelayDuration = 100,
  ...restProps
}: Props = $props();
</script>

<TooltipPrimitive.Provider
  {...restProps}
  {delayDuration}
  {disableHoverableContent}
  {skipDelayDuration}
>
  {@render children?.()}
</TooltipPrimitive.Provider>
