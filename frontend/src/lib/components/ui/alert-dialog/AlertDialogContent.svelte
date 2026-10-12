<script lang="ts">
import { AlertDialog as AlertDialogPrimitive } from "bits-ui";
import type { Snippet } from "svelte";

type Props = AlertDialogPrimitive.ContentProps & {
  header?: Snippet;
  footer?: Snippet;
};

let { children, header, footer, class: className, ...restProps }: Props = $props();
</script>

<AlertDialogPrimitive.Portal>
  <AlertDialogPrimitive.Overlay
    class="fixed inset-0 z-50 bg-black/55 backdrop-blur-[1px] transition-opacity duration-(--motion-emphasis) ease-kinetic data-starting-style:opacity-0 data-ending-style:opacity-0"
  />
  <AlertDialogPrimitive.Content
    {...restProps}
    class={[
  "fixed top-1/2 left-1/2 z-50 flex max-h-[calc(100dvh-2rem)] w-[min(30rem,calc(100%-2rem))] -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-overlay border border-border-strong bg-surface-raised text-text-primary shadow-[0_20px_60px_rgb(0_0_0/38%)] outline-none transition-[opacity,transform] duration-(--motion-emphasis) ease-kinetic data-starting-style:translate-y-[calc(-50%+0.25rem)] data-starting-style:opacity-0 data-ending-style:translate-y-[calc(-50%+0.25rem)] data-ending-style:opacity-0",
  className,
]}
  >
    {#if header}
      <header class="shrink-0 border-b border-border-subtle bg-surface-panel px-5 py-4">
        {@render header()}
      </header>
    {/if}
    <div class="min-h-0 overflow-y-auto px-5 py-4">
      {@render children?.()}
    </div>
    {#if footer}
      <footer
        class="flex shrink-0 flex-wrap items-center justify-end gap-2 border-t border-border-subtle bg-surface-panel px-5 py-3"
      >
        {@render footer()}
      </footer>
    {/if}
  </AlertDialogPrimitive.Content>
</AlertDialogPrimitive.Portal>
