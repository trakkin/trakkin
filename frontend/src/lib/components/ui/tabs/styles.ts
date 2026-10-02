export type TabsVariant = "segmented" | "underline";

export function tabsListClasses(variant: TabsVariant = "underline"): string[] {
  return [
    "inline-flex items-center data-[orientation=vertical]:min-w-40 data-[orientation=vertical]:flex-col data-[orientation=vertical]:items-stretch",
    variant === "underline"
      ? "min-h-9 gap-4 data-[orientation=vertical]:gap-1"
      : "min-h-8 gap-0.5 rounded-control border border-border-subtle bg-white/2 p-0.5",
  ];
}

export function tabsTriggerClasses(variant: TabsVariant = "underline"): string[] {
  return [
    "inline-flex items-center justify-center text-label text-text-muted outline-none transition-[background-color,color,box-shadow] duration-(--motion-standard) ease-standard hover:text-text-secondary focus-visible:ring-2 focus-visible:ring-accent/20 data-[state=active]:text-text-primary data-[orientation=vertical]:w-full data-[orientation=vertical]:justify-start data-disabled:pointer-events-none data-disabled:opacity-35",
    variant === "underline"
      ? "relative h-9 px-0.5 font-[560] after:absolute after:right-0 after:bottom-0 after:left-0 after:h-0.5 after:rounded-t-full after:bg-accent after:opacity-0 after:content-[''] data-[state=active]:after:opacity-100 data-[orientation=vertical]:h-8 data-[orientation=vertical]:px-2 data-[orientation=vertical]:after:top-0 data-[orientation=vertical]:after:right-0 data-[orientation=vertical]:after:bottom-0 data-[orientation=vertical]:after:left-auto data-[orientation=vertical]:after:h-auto data-[orientation=vertical]:after:w-0.5 data-[orientation=vertical]:after:rounded-l-full"
      : "h-7 rounded-control px-2.5 font-[510] hover:bg-white/3 data-[state=active]:bg-white/5 data-[state=active]:shadow-[inset_0_0_0_1px_rgb(255_255_255/4%)]",
  ];
}
