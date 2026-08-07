import { request } from "./client.ts";
import type { components, operations } from "./generated/schema.ts";

export type SynchronizationPreview = components["schemas"]["SyncPreviewResponse"];
export type SynchronizationPreviewEntry = components["schemas"]["SyncPreviewEntryResponse"];
export type SynchronizationRunAdmission = components["schemas"]["StartSyncRunResponse"];
export type SynchronizationSchedule = components["schemas"]["SyncScheduleResponse"];

type CreateSynchronizationPreviewRequest =
  operations["create_sync_preview"]["requestBody"]["content"]["application/json"];
type CreateSynchronizationPreviewResponse =
  operations["create_sync_preview"]["responses"][201]["content"]["application/json"];
type StartSynchronizationRunRequest = operations["start_sync_run"]["requestBody"]["content"]["application/json"];
type StartSynchronizationRunResponse = operations["start_sync_run"]["responses"][201]["content"]["application/json"];
type EnableSynchronizationScheduleRequest =
  operations["enable_sync_schedule"]["requestBody"]["content"]["application/json"];
type EnableSynchronizationScheduleResponse =
  operations["enable_sync_schedule"]["responses"][200]["content"]["application/json"];
type GetSynchronizationScheduleResponse =
  operations["get_sync_schedule"]["responses"][200]["content"]["application/json"];
type PauseSynchronizationScheduleResponse =
  operations["pause_sync_schedule"]["responses"][200]["content"]["application/json"];

export function createSynchronizationPreview(
  workspaceId: string,
  policyId: string,
  signal?: AbortSignal,
): Promise<CreateSynchronizationPreviewResponse> {
  const body: CreateSynchronizationPreviewRequest = { policyId };
  return request(`/api/v1/workspaces/${workspaceId}/sync-previews`, { method: "POST", body, signal });
}

export function startSynchronizationRun(
  workspaceId: string,
  previewId: string,
  previewHash: string,
  signal?: AbortSignal,
): Promise<StartSynchronizationRunResponse> {
  const body: StartSynchronizationRunRequest = { previewHash };
  return request(`/api/v1/workspaces/${workspaceId}/sync-previews/${previewId}/runs`, {
    method: "POST",
    body,
    signal,
  });
}

export function getSynchronizationSchedule(
  workspaceId: string,
  policyId: string,
  signal?: AbortSignal,
): Promise<GetSynchronizationScheduleResponse> {
  return request(`/api/v1/workspaces/${workspaceId}/sync-policies/${policyId}/schedule`, { signal });
}

export function enableSynchronizationSchedule(
  workspaceId: string,
  policyId: string,
  intervalSeconds: number,
  signal?: AbortSignal,
): Promise<EnableSynchronizationScheduleResponse> {
  const body: EnableSynchronizationScheduleRequest = { intervalSeconds };
  return request(`/api/v1/workspaces/${workspaceId}/sync-policies/${policyId}/schedule`, {
    method: "PUT",
    body,
    signal,
  });
}

export function pauseSynchronizationSchedule(
  workspaceId: string,
  policyId: string,
  signal?: AbortSignal,
): Promise<PauseSynchronizationScheduleResponse> {
  return request(`/api/v1/workspaces/${workspaceId}/sync-policies/${policyId}/schedule/pause`, {
    method: "POST",
    signal,
  });
}
