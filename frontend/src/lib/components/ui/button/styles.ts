export type ButtonSize = "sm" | "md" | "lg";
export type ButtonVariant = "primary" | "secondary" | "ghost" | "danger" | "success";

export const buttonBase =
  "relative inline-flex shrink-0 select-none items-center justify-center gap-1.5 overflow-hidden rounded-control border text-label font-[510] outline-none transition-[background-color,border-color,color,filter] duration-(--motion-standard) ease-standard focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-surface-panel active:brightness-95 disabled:pointer-events-none disabled:opacity-35";

export const buttonSizes: Record<ButtonSize, string> = {
  lg: "h-(--control-height-lg) px-4",
  md: "h-(--control-height-md) px-3",
  sm: "h-(--control-height-sm) px-2.5",
};

export const iconButtonSizes: Record<ButtonSize, string> = {
  lg: "size-(--control-height-lg) p-0",
  md: "size-(--control-height-md) p-0",
  sm: "size-(--control-height-sm) p-0",
};

export const buttonVariants: Record<ButtonVariant, string> = {
  danger: "border-danger/25 bg-danger/6 text-danger hover:border-danger/40 hover:bg-danger/10",
  ghost: "border-transparent bg-transparent text-text-secondary hover:bg-white/4 hover:text-text-primary",
  primary: "border-accent/25 bg-accent/10 text-accent hover:border-accent/40 hover:bg-accent/15",
  secondary: "border-border-subtle bg-white/3 text-text-secondary hover:border-border-strong hover:bg-white/5",
  success: "border-success/25 bg-success/8 text-success hover:border-success/40 hover:bg-success/12",
};
