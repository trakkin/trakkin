export type ButtonSize = "sm" | "md" | "lg";
export type ButtonVariant = "primary" | "secondary" | "ghost" | "danger";

export const buttonBase =
  "relative inline-flex shrink-0 select-none items-center justify-center gap-1.5 overflow-hidden rounded-md border text-label font-[510] outline-none transition-[background-color,border-color,color,filter] duration-(--motion-standard) ease-standard active:brightness-95 disabled:pointer-events-none disabled:opacity-35";

export const buttonSizes: Record<ButtonSize, string> = {
  lg: "h-9 px-4",
  md: "h-8 px-3",
  sm: "h-7 px-2.5",
};

export const iconButtonSizes: Record<ButtonSize, string> = {
  lg: "size-8 p-0",
  md: "size-7 p-0",
  sm: "size-6 p-0",
};

export const buttonVariants: Record<ButtonVariant, string> = {
  danger: "border-danger/25 bg-danger/6 text-danger hover:border-danger/40 hover:bg-danger/10",
  ghost: "border-transparent bg-transparent text-text-secondary hover:bg-white/4 hover:text-text-primary",
  primary: "border-accent/25 bg-accent/10 text-accent hover:border-accent/40 hover:bg-accent/15",
  secondary: "border-border-subtle bg-white/3 text-text-secondary hover:border-border-strong hover:bg-white/5",
};
