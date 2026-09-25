export type ChipTone = "neutral" | "accent" | "success" | "warning" | "danger" | "info";

export const chipTones: Record<ChipTone, string> = {
  neutral: "border-border-subtle bg-white/2 text-text-secondary",
  accent: "border-accent/20 bg-accent/6 text-accent",
  success: "border-success/20 bg-success/6 text-success",
  warning: "border-warning/20 bg-warning/6 text-warning",
  danger: "border-danger/20 bg-danger/6 text-danger",
  info: "border-info/20 bg-info/6 text-info",
};
