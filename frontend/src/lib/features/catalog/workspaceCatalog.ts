import type { CatalogPage } from "$lib/api/catalog";
import {
  loadWorkspaceSourceDirectory,
  type WorkspaceSourceDirectory,
  type WorkspaceSourceDirectoryEntry,
} from "$lib/features/workspaces/workspaceSourceDirectory";

export type WorkspaceCatalogSource = WorkspaceSourceDirectoryEntry & {
  catalog: CatalogPage | null;
  error: string;
};

export type WorkspaceCatalog = {
  workspace: WorkspaceSourceDirectory["workspace"];
  sources: WorkspaceCatalogSource[];
  issues: WorkspaceSourceDirectory["issues"];
};

export async function loadWorkspaceCatalog(workspaceId: string, signal?: AbortSignal): Promise<WorkspaceCatalog> {
  const directory = await loadWorkspaceSourceDirectory(workspaceId, signal);
  return {
    workspace: directory.workspace,
    issues: directory.issues,
    sources: directory.sources.map((entry) => ({
      ...entry,
      catalog: null,
      error: entry.source.availability === "available" ? "" : "Source unavailable",
    })),
  };
}
