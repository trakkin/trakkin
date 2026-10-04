import { ApiRequestError } from "$lib/api/client";
import { listTasks } from "$lib/api/operations";

export type CatalogPublicationIssue = "scan-active" | "unpublished";

export async function catalogPublicationIssue(
  error: unknown,
  workspaceId: string,
  sourceId: string,
  signal?: AbortSignal,
): Promise<CatalogPublicationIssue | null> {
  if (!(error instanceof ApiRequestError) || error.code !== "stream_unavailable") return null;
  try {
    const tasks = await listTasks(workspaceId, signal);
    const scanActive = tasks.some(
      (task) =>
        !task.terminal &&
        task.sourceId === sourceId &&
        (task.kind === "catalogRead" || (task.kind === "catalogReset" && task.stream === "catalog")),
    );
    return scanActive ? "scan-active" : "unpublished";
  } catch {
    return null;
  }
}
