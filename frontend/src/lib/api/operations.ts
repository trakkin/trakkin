import { request } from "./client.ts";
import type { components, operations } from "./generated/schema.ts";
import { pollUntilTerminal } from "./taskPolling.ts";

export type TaskRun = components["schemas"]["TaskResponse"];

type ListTasksResponse = operations["list_tasks"]["responses"][200]["content"]["application/json"];
type StartSourceReadRequest = operations["start_source_read"]["requestBody"]["content"]["application/json"];
type StartSourceResetRequest = operations["start_source_reset"]["requestBody"]["content"]["application/json"];

export function pollCatalog(workspaceId: string, sourceId: string, signal?: AbortSignal): Promise<TaskRun> {
  const body: StartSourceReadRequest = { stream: "catalog", mode: "full" };
  return request(`/api/v1/workspaces/${workspaceId}/sources/${sourceId}/reads`, { method: "POST", body, signal });
}

export function getTask(workspaceId: string, taskId: string, signal?: AbortSignal): Promise<TaskRun> {
  return request(`/api/v1/workspaces/${workspaceId}/tasks/${taskId}`, { signal });
}

export function listTasks(workspaceId: string, signal?: AbortSignal): Promise<ListTasksResponse> {
  return request(`/api/v1/workspaces/${workspaceId}/tasks`, { signal });
}

export function cancelTask(workspaceId: string, taskId: string): Promise<TaskRun> {
  return request(`/api/v1/workspaces/${workspaceId}/tasks/${taskId}/cancellation`, { method: "POST" });
}

export async function resetAndRefetch(workspaceId: string, sourceId: string, signal?: AbortSignal): Promise<TaskRun> {
  const body: StartSourceResetRequest = { stream: "catalog" };
  const reset = await request<TaskRun>(`/api/v1/workspaces/${workspaceId}/sources/${sourceId}/resets`, {
    method: "POST",
    body,
    signal,
  });
  const resetResult = await waitForTask(workspaceId, reset, signal);
  if (resetResult.status !== "succeeded") return resetResult;
  return pollCatalog(workspaceId, sourceId, signal);
}

export function waitForTask(workspaceId: string, task: TaskRun, signal?: AbortSignal): Promise<TaskRun> {
  return pollUntilTerminal(task, (taskId, signal) => getTask(workspaceId, taskId, signal), signal);
}
