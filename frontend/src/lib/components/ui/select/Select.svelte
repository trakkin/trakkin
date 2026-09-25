<script lang="ts">
import { Check, ChevronsUpDown } from "@lucide/svelte";
import { Select as SelectPrimitive } from "bits-ui";
import { controlBase, controlSizes } from "../input/styles";
import type { SelectProps } from "./types";

let {
  allowDeselect = false,
  ariaInvalid = false,
  ariaLabel,
  autocomplete,
  class: className,
  contentClass,
  controlSize = "md",
  disabled = false,
  items,
  loop = true,
  name,
  onOpenChange,
  onValueChange,
  open = $bindable(false),
  placeholder = "Select an option",
  required = false,
  triggerClass,
  value = $bindable(""),
}: SelectProps = $props();
</script>

<div class={className}>
  <SelectPrimitive.Root
    {allowDeselect}
    {autocomplete}
    {disabled}
    {items}
    {loop}
    {name}
    {onOpenChange}
    {onValueChange}
    {required}
    type="single"
    bind:open
    bind:value
  >
    <SelectPrimitive.Trigger
      aria-invalid={ariaInvalid}
      aria-label={ariaLabel}
      class={[
        controlBase,
        'group inline-flex items-center justify-between gap-2 data-placeholder:text-text-disabled',
        controlSizes[controlSize],
        triggerClass
      ]}
    >
      <SelectPrimitive.Value
        class="min-w-0 flex-1 truncate text-left"
        {placeholder}
      />
      <ChevronsUpDown
        aria-hidden="true"
        class="shrink-0 text-text-muted transition-transform duration-(--motion-emphasis) ease-kinetic group-data-[state=open]:rotate-180"
        size={15}
        strokeWidth={2}
      />
    </SelectPrimitive.Trigger>

    <SelectPrimitive.Portal>
      <SelectPrimitive.Content
        class={[
          'z-50 min-w-(--bits-select-anchor-width) max-w-[calc(100vw-1rem)] origin-(--bits-select-content-transform-origin) rounded-overlay border border-border-subtle bg-surface-raised p-1 text-label text-text-secondary shadow-[0_8px_24px_rgb(0_0_0/28%)] outline-none transition-[opacity,transform] duration-(--motion-emphasis) ease-kinetic data-starting-style:translate-y-1 data-starting-style:opacity-0 data-ending-style:translate-y-1 data-ending-style:opacity-0',
          contentClass
        ]}
        collisionPadding={8}
        hideWhenDetached
        sideOffset={6}
        sticky="always"
        strategy="fixed"
        updatePositionStrategy="always"
      >
        <SelectPrimitive.Viewport
          class="max-h-[min(16rem,var(--bits-select-content-available-height))] overflow-y-auto"
        >
          {#each items as item (item.value)}
            <SelectPrimitive.Item
              class="flex h-7 select-none items-center gap-2 rounded-control px-2 text-label font-[450] outline-none transition-colors duration-(--motion-fast) ease-standard data-highlighted:bg-white/5 data-selected:text-text-primary data-disabled:pointer-events-none data-disabled:text-text-disabled data-disabled:opacity-45"
              disabled={item.disabled}
              label={item.label}
              value={item.value}
            >
              {#snippet children({ selected })}
                <span class="min-w-0 flex-1 truncate">{item.label}</span>
                {#if selected}
                  <Check
                    aria-hidden="true"
                    class="shrink-0 text-accent"
                    size={14}
                    strokeWidth={2.25}
                  />
                {/if}
              {/snippet}
            </SelectPrimitive.Item>
          {/each}
        </SelectPrimitive.Viewport>
      </SelectPrimitive.Content>
    </SelectPrimitive.Portal>
  </SelectPrimitive.Root>
</div>
