<script lang="ts">
import { AlertDialog as AlertDialogPrimitive } from "bits-ui";
import { type ButtonSize, buttonBase, buttonSizes, buttonVariants } from "../button/styles";

type Props = AlertDialogPrimitive.CancelProps & {
  size?: ButtonSize;
};

let { children, class: className, size = "md", ...restProps }: Props = $props();
</script>

<AlertDialogPrimitive.Cancel
  {...restProps}
  class={[buttonBase, buttonSizes[size], buttonVariants.secondary, className]}
>
  {@render children?.()}
</AlertDialogPrimitive.Cancel>
