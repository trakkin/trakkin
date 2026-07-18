<script lang="ts">
import { LoaderCircle } from "@lucide/svelte";

type Props = {
  class?: string;
  size?: number;
};

let { class: className, size = 16 }: Props = $props();
</script>

<span class={["inline-flex shrink-0 animate-spin", className]}>
  <LoaderCircle
    aria-hidden="true"
    {size}
  />
</span>
