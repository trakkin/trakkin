import { defineVersionedLocalStorage } from "$lib/storage/versionedLocalStorage";

export const activeWorkspacePreference = defineVersionedLocalStorage<string>({
  name: "active-workspace",
  version: 1,
  decode: (value) => (typeof value === "string" && value.trim() === value && value.length > 0 ? value : undefined),
});
