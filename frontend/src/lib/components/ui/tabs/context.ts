import { getContext, setContext } from "svelte";
import type { TabsVariant } from "./styles.ts";

const tabsVariantContext = Symbol("tabs-variant");

type TabsVariantGetter = () => TabsVariant;

export function provideTabsVariant(getVariant: TabsVariantGetter): void {
  setContext(tabsVariantContext, getVariant);
}

export function useTabsVariant(): TabsVariantGetter {
  return getContext<TabsVariantGetter>(tabsVariantContext) ?? (() => "underline");
}
