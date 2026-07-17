<script lang="ts">
import { DropdownMenu as DropdownMenuPrimitive } from "bits-ui";

type Props = DropdownMenuPrimitive.ContentProps;

let { children, class: className, collisionPadding = 8, sideOffset = 4, ...restProps }: Props = $props();
</script>

<DropdownMenuPrimitive.Portal>
  <DropdownMenuPrimitive.Content
    {...restProps}
    class={[
      'z-50 min-w-40 max-w-[calc(100vw-1rem)] origin-(--bits-dropdown-menu-content-transform-origin) rounded-md border border-border-subtle bg-surface-raised p-1 text-label text-text-secondary shadow-[0_8px_24px_rgb(0_0_0/28%)] outline-none transition-[opacity,transform] duration-(--motion-emphasis) ease-kinetic data-starting-style:translate-y-1 data-starting-style:opacity-0 data-ending-style:translate-y-1 data-ending-style:opacity-0',
      className
    ]}
    {collisionPadding}
    {sideOffset}
    strategy="fixed"
  >
    {@render children?.()}
  </DropdownMenuPrimitive.Content>
</DropdownMenuPrimitive.Portal>
