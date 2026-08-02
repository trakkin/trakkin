import { request } from "./client.ts";
import type { components, operations } from "./generated/schema.ts";

export type SynchronizationPolicy = components["schemas"]["SyncPolicyResponse"];
export type SynchronizationPolicyDocument = components["schemas"]["SyncPolicyDocumentPayload"];
export type SynchronizationPolicyPage = components["schemas"]["SyncPolicyPageResponse"];

type CreateSynchronizationPolicyRequest =
  operations["create_sync_policy"]["requestBody"]["content"]["application/json"];
type CreateSynchronizationPolicyResponse =
  operations["create_sync_policy"]["responses"][201]["content"]["application/json"];
type ListSynchronizationPoliciesResponse =
  operations["list_sync_policies"]["responses"][200]["content"]["application/json"];

export function listSynchronizationPolicies(
  workspaceId: string,
  limit = 100,
  cursor?: string,
  signal?: AbortSignal,
): Promise<ListSynchronizationPoliciesResponse> {
  const query = new URLSearchParams({ limit: String(limit) });
  if (cursor) query.set("cursor", cursor);
  return request(`/api/v1/workspaces/${workspaceId}/sync-policies?${query}`, { signal });
}

export function createSynchronizationPolicy(
  workspaceId: string,
  document: SynchronizationPolicyDocument,
  signal?: AbortSignal,
): Promise<CreateSynchronizationPolicyResponse> {
  const body: CreateSynchronizationPolicyRequest = { document };
  return request(`/api/v1/workspaces/${workspaceId}/sync-policies`, { method: "POST", body, signal });
}
