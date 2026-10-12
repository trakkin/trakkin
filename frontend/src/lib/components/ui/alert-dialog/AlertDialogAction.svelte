<script lang="ts">
import { AlertDialog as AlertDialogPrimitive } from "bits-ui";
import { type ButtonSize, type ButtonVariant, buttonBase, buttonSizes, buttonVariants } from "../button/styles";

type Props = AlertDialogPrimitive.ActionProps & {
  size?: ButtonSize;
  variant?: ButtonVariant;
};

let { children, class: className, size = "md", variant = "danger", ...restProps }: Props = $props();
</script>

<AlertDialogPrimitive.Action
  {...restProps}
  class={[buttonBase, buttonSizes[size], buttonVariants[variant], className]}
>
  {@render children?.()}
</AlertDialogPrimitive.Action>
