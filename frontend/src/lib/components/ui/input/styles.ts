export type ControlSize = "sm" | "md" | "lg";

export const controlBase =
  "w-full min-w-0 rounded-control border border-border-subtle bg-white/3 text-label font-[450] text-text-secondary caret-accent outline-none transition-[background-color,border-color,box-shadow] duration-(--motion-standard) ease-standard placeholder:text-text-disabled hover:border-border-strong hover:bg-white/4 focus-visible:border-accent focus-visible:bg-white/4 focus-visible:ring-2 focus-visible:ring-accent/20 disabled:cursor-not-allowed disabled:bg-white/2 disabled:text-text-disabled disabled:opacity-55 aria-invalid:border-danger/60 aria-invalid:focus-visible:border-danger/70 aria-invalid:focus-visible:ring-danger/20";

export const controlSizes: Record<ControlSize, string> = {
  lg: "h-(--control-height-lg) px-3",
  md: "h-(--control-height-md) px-2.5",
  sm: "h-(--control-height-sm) px-2",
};

export const textareaSizes: Record<ControlSize, string> = {
  lg: "min-h-28 px-3 py-2.5",
  md: "min-h-24 px-2.5 py-2",
  sm: "min-h-20 px-2 py-1.5",
};
