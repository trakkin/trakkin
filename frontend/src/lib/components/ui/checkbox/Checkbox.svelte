<script lang="ts">
import { Check, Minus } from "@lucide/svelte";
import { Checkbox as CheckboxPrimitive, type WithoutChildrenOrChild } from "bits-ui";

type Props = WithoutChildrenOrChild<CheckboxPrimitive.RootProps>;

let { checked = $bindable(false), indeterminate = $bindable(false), class: className, ...restProps }: Props = $props();
</script>

<CheckboxPrimitive.Root
  {...restProps}
  class={[
    'inline-flex size-4 shrink-0 items-center justify-center rounded-sm border border-border-strong bg-white/3 text-accent-contrast outline-none transition-[background-color,border-color,box-shadow] duration-(--motion-standard) ease-standard hover:border-text-muted focus-visible:border-accent/60 focus-visible:ring-2 focus-visible:ring-accent/10 data-[state=checked]:border-accent/50 data-[state=checked]:bg-accent/85 data-[state=indeterminate]:border-accent/50 data-[state=indeterminate]:bg-accent/85 data-disabled:pointer-events-none data-disabled:opacity-35 data-readonly:cursor-default',
    className
  ]}
  bind:checked
  bind:indeterminate
>
  {#snippet children({ checked: isChecked, indeterminate: isIndeterminate })}
    <span
      class="flex items-center justify-center transition-[opacity,scale] duration-(--motion-standard) ease-kinetic {isChecked || isIndeterminate ? 'scale-100 opacity-100' : 'scale-50 opacity-0'}"
    >
      {#if isIndeterminate}
        <Minus
          aria-hidden="true"
          class="translate-y-[0.5px]"
          size={11}
          strokeWidth={3}
        />
      {:else}
        <Check
          aria-hidden="true"
          class="translate-y-[0.5px]"
          size={11}
          strokeWidth={3}
        />
      {/if}
    </span>
  {/snippet}
</CheckboxPrimitive.Root>
