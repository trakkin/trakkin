import { request } from "./client.ts";
import type { components, operations } from "./generated/schema.ts";

export type Workspace = components["schemas"]["WorkspaceResponse"];
export type WorkspaceSource = components["schemas"]["WorkspaceSourceResponse"];

type ListWorkspacesResponse = operations["list_workspaces"]["responses"][200]["content"]["application/json"];
type CreateWorkspaceRequest = operations["create_workspace"]["requestBody"]["content"]["application/json"];

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
