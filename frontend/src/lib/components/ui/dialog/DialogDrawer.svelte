<script lang="ts">
import { Dialog as DialogPrimitive } from "bits-ui";

type Props = DialogPrimitive.ContentProps;

let { children, class: className, ...restProps }: Props = $props();
</script>

<DialogPrimitive.Portal>
  <DialogPrimitive.Overlay
    class="fixed inset-0 z-50 bg-black/55 backdrop-blur-[1px] transition-opacity duration-(--motion-emphasis) ease-kinetic data-starting-style:opacity-0 data-ending-style:opacity-0"
  />
  <DialogPrimitive.Content
    {...restProps}
    class={[
      "fixed inset-y-0 right-0 z-50 flex w-full flex-col border-l border-border-strong bg-surface-raised text-text-primary shadow-[-20px_0_60px_rgb(0_0_0/38%)] outline-none transition-transform duration-(--motion-emphasis) ease-kinetic sm:max-w-2xl data-starting-style:translate-x-full data-ending-style:translate-x-full",
      className,
    ]}
  >
    {@render children?.()}
  </DialogPrimitive.Content>
</DialogPrimitive.Portal>
