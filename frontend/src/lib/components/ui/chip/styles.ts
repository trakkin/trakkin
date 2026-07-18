export type ChipTone = "neutral" | "accent" | "success" | "warning" | "danger" | "info";

export const chipToneDots: Record<ChipTone, string> = {
  neutral: "bg-text-disabled",
  accent: "bg-accent",
  success: "bg-success",
  warning: "bg-warning",
  danger: "bg-danger",
  info: "bg-info",
};
