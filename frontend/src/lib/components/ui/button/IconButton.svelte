<script lang="ts">
import { Button as ButtonPrimitive, type ButtonRootProps } from "bits-ui";
import { type ButtonSize, type ButtonVariant, buttonBase, buttonVariants, iconButtonSizes } from "./styles";

type Props = ButtonRootProps & {
  label: string;
  size?: ButtonSize;
  variant?: ButtonVariant;
};

let { children, class: className, label, size = "md", variant = "ghost", ...restProps }: Props = $props();
</script>

<ButtonPrimitive.Root
  {...restProps}
  aria-label={label}
  class={[buttonBase, iconButtonSizes[size], buttonVariants[variant], className]}
>
  {@render children?.()}
</ButtonPrimitive.Root>
