<script lang="ts">
import { ImageOff } from "@lucide/svelte";
import type { Snippet } from "svelte";
import type { HTMLAttributes } from "svelte/elements";

type Props = Omit<HTMLAttributes<HTMLDivElement>, "children"> & {
  alt?: string;
  children?: Snippet;
  shape?: "landscape" | "poster" | "square";
  src?: string;
};

let { alt = "", children, class: className, shape = "poster", src, ...restProps }: Props = $props();
let failedSrc = $state<string>();

const shapeClasses = {
  landscape: "aspect-video",
  poster: "aspect-2/3",
  square: "aspect-square",
};
</script>

<div
  {...restProps}
  class={[
    "relative isolate overflow-hidden rounded-md border border-border-subtle bg-surface-raised shadow-[0_8px_24px_rgb(0_0_0/18%)]",
    shapeClasses[shape],
    className,
  ]}
>
  {#if src && src !== failedSrc}
    <img
      {alt}
      class="absolute inset-0 size-full object-cover"
      decoding="async"
      loading="lazy"
      onerror={() => (failedSrc = src)}
      {src}
    >
  {:else}
    <div
      class="absolute inset-0 grid place-items-center bg-[linear-gradient(145deg,var(--surface-raised),var(--surface-panel))] text-text-disabled"
    >
      <ImageOff
        aria-hidden="true"
        size={20}
      />
    </div>
  {/if}
  <div
    aria-hidden="true"
    class="pointer-events-none absolute inset-0 bg-[linear-gradient(to_top,rgb(0_0_0/68%)_0%,transparent_42%)]"
  ></div>
  {#if children}
    {@render children()}
  {/if}
</div>
