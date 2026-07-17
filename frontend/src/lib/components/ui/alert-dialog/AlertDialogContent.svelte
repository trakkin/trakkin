<script lang="ts">
import { AlertDialog as AlertDialogPrimitive } from "bits-ui";

type Props = AlertDialogPrimitive.ContentProps;

let { children, class: className, ...restProps }: Props = $props();
</script>

<AlertDialogPrimitive.Portal>
  <AlertDialogPrimitive.Overlay
    class="fixed inset-0 z-50 bg-black/60 backdrop-blur-[1px] transition-opacity duration-(--motion-emphasis) ease-kinetic data-starting-style:opacity-0 data-ending-style:opacity-0"
  />
  <AlertDialogPrimitive.Content
    {...restProps}
    class={[
      'fixed top-1/2 left-1/2 z-50 w-[min(28rem,calc(100vw-2rem))] -translate-x-1/2 -translate-y-1/2 rounded-md border border-border-strong bg-surface-raised p-5 text-text-primary shadow-[0_20px_60px_rgb(0_0_0/45%)] outline-none transition-[opacity,transform] duration-(--motion-emphasis) ease-kinetic data-starting-style:translate-y-[calc(-50%+0.5rem)] data-starting-style:scale-[0.98] data-starting-style:opacity-0 data-ending-style:translate-y-[calc(-50%+0.5rem)] data-ending-style:scale-[0.98] data-ending-style:opacity-0',
      className
    ]}
  >
    {@render children?.()}
  </AlertDialogPrimitive.Content>
</AlertDialogPrimitive.Portal>
