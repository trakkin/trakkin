import { request } from "./client.ts";
import type { components, operations } from "./generated/schema.ts";

export type Workspace = components["schemas"]["WorkspaceResponse"];
export type WorkspaceSource = components["schemas"]["WorkspaceSourceResponse"];
export type MappingRootPolicy = components["schemas"]["MappingRootPolicyResponse"];

type ListWorkspacesResponse = operations["list_workspaces"]["responses"][200]["content"]["application/json"];
type CreateWorkspaceRequest = operations["create_workspace"]["requestBody"]["content"]["application/json"];
type UpdateMappingRootPolicyRequest = components["schemas"]["UpdateMappingRootPolicyRequest"];

export function listWorkspaces(signal?: AbortSignal): Promise<ListWorkspacesResponse> {
  return request("/api/v1/workspaces", { signal });
}

export function getWorkspace(workspaceId: string, signal?: AbortSignal): Promise<Workspace> {
  return request(`/api/v1/workspaces/${workspaceId}`, { signal });
}

export function createWorkspace(name: string): Promise<Workspace> {
  const body: CreateWorkspaceRequest = { name };
  return request("/api/v1/workspaces", { method: "POST", body });
}

export function attachSource(workspaceId: string, sourceId: string): Promise<Workspace> {
  return request(`/api/v1/workspaces/${workspaceId}/sources/${sourceId}`, { method: "PUT" });
}

export function detachSource(workspaceId: string, sourceId: string): Promise<Workspace> {
  return request(`/api/v1/workspaces/${workspaceId}/sources/${sourceId}`, { method: "DELETE" });
}

export function getMappingRootPolicy(
  workspaceId: string,
  sourceId: string,
  signal?: AbortSignal,
): Promise<MappingRootPolicy> {
  return request(`/api/v1/workspaces/${workspaceId}/sources/${sourceId}/mapping-root-policy`, { signal });
}

export function updateMappingRootPolicy(
  workspaceId: string,
  sourceId: string,
  namespaces: string[] | null,
  signal?: AbortSignal,
): Promise<MappingRootPolicy> {
  const body: UpdateMappingRootPolicyRequest = { namespaces };
  return request(`/api/v1/workspaces/${workspaceId}/sources/${sourceId}/mapping-root-policy`, {
    method: "PUT",
    body,
    signal,
  });
}
