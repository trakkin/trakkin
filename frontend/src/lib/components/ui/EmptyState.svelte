<script lang="ts">
import type { IconProps } from "@lucide/svelte";
import type { Component, Snippet } from "svelte";
import type { HTMLAttributes } from "svelte/elements";

type Props = Omit<HTMLAttributes<HTMLElement>, "children"> & {
  title: string;
  description?: string;
  icon: Component<Omit<IconProps, "icon" | "iconNode">>;
  actions?: Snippet;
};

let { title, description, icon: Icon, actions, class: className, ...restProps }: Props = $props();
</script>

<section
  {...restProps}
  class={["grid min-h-48 w-full place-items-center px-4 py-8 text-center", className]}
>
  <div class="flex min-w-0 max-w-md flex-col items-center">
    <Icon
      aria-hidden="true"
      class="text-text-disabled"
      size={24}
    />
    <h2 class="mt-3 text-heading font-semibold wrap-anywhere text-text-primary">{title}</h2>
    {#if description}
      <p class="mt-1 text-label wrap-anywhere text-text-muted">{description}</p>
    {/if}
    {#if actions}
      <div class="mt-4 flex flex-wrap justify-center gap-2">{@render actions()}</div>
    {/if}
  </div>
</section>
