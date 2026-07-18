export type PanelTone = "accent" | "danger" | "default" | "dashed" | "warning";

export const panelBase = "rounded-panel border";

export const panelTones: Record<PanelTone, string> = {
  accent: "border-accent/20 bg-accent/4",
  danger: "border-danger/20 bg-danger/5",
  default: "border-border-subtle bg-white/2",
  dashed: "border-dashed border-border-strong bg-white/2",
  warning: "border-warning/20 bg-warning/5",
};
