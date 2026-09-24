import { defineVersionedLocalStorage } from "$lib/storage/versionedLocalStorage";

export type CatalogViewMode = "grid" | "list";

export const catalogViewPreference = defineVersionedLocalStorage<CatalogViewMode>({
  name: "catalog-view",
  version: 1,
  decode: decodeCatalogViewMode,
});

export function decodeCatalogViewMode(value: unknown): CatalogViewMode | undefined {
  return value === "grid" || value === "list" ? value : undefined;
}
