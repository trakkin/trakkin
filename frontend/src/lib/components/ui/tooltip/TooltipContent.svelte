<script lang="ts">
import { Tooltip as TooltipPrimitive } from "bits-ui";

type Props = TooltipPrimitive.ContentProps;

let { children, class: className, sideOffset = 6, ...restProps }: Props = $props();
</script>

<TooltipPrimitive.Portal>
  <TooltipPrimitive.Content
    {...restProps}
    class={[
      'z-50 max-w-[min(16rem,var(--bits-tooltip-content-available-width),calc(100vw-1rem))] origin-(--bits-tooltip-content-transform-origin) rounded-overlay border border-border-subtle bg-surface-raised px-2 py-1.5 text-meta font-[510] text-text-secondary shadow-[0_4px_12px_rgb(0_0_0/28%)] outline-none transition-[opacity,transform] duration-(--motion-emphasis) ease-kinetic data-starting-style:translate-y-1 data-starting-style:opacity-0 data-ending-style:translate-y-0.5 data-ending-style:opacity-0',
      className
    ]}
    collisionPadding={8}
    hideWhenDetached
    {sideOffset}
    sticky="always"
    strategy="fixed"
    updatePositionStrategy="always"
  >
    {@render children?.()}
  </TooltipPrimitive.Content>
</TooltipPrimitive.Portal>
