<script lang="ts">
import { Button as ButtonPrimitive, type ButtonRootProps } from "bits-ui";
import { type ButtonSize, type ButtonVariant, buttonBase, buttonSizes, buttonVariants } from "./styles";

type Props = ButtonRootProps & {
  size?: ButtonSize;
  variant?: ButtonVariant;
};

let { children, class: className, size = "md", variant = "secondary", ...restProps }: Props = $props();
let elementProps: ButtonRootProps = $derived(
  "href" in restProps ? restProps : { ...restProps, type: restProps.type ?? "button" },
);
</script>

<ButtonPrimitive.Root
  {...elementProps}
  class={[buttonBase, buttonSizes[size], buttonVariants[variant], className]}
>
  {@render children?.()}
</ButtonPrimitive.Root>
