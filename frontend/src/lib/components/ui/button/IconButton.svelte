<script lang="ts">
import { Button as ButtonPrimitive, type ButtonRootProps } from "bits-ui";
import { type ButtonSize, type ButtonVariant, buttonBase, buttonVariants, iconButtonSizes } from "./styles";

type Props = ButtonRootProps & {
  label: string;
  size?: ButtonSize;
  variant?: ButtonVariant;
};

let {
  children,
  class: className,
  label,
  size = "md",
  title = label,
  variant = "ghost",
  ...restProps
}: Props = $props();
let elementProps: ButtonRootProps = $derived(
  "href" in restProps ? restProps : { ...restProps, type: restProps.type ?? "button" },
);
</script>

<ButtonPrimitive.Root
  {...elementProps}
  aria-label={label}
  class={[buttonBase, iconButtonSizes[size], buttonVariants[variant], className]}
  {title}
>
  {@render children?.()}
</ButtonPrimitive.Root>
