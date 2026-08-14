import { request } from "./client.ts";
import type { components, operations } from "./generated/schema.ts";

export type ActivityAction = components["schemas"]["ActivityActionResponse"];
export type ActivityActionDetail = components["schemas"]["ActivityActionDetailResponse"];
export type ActivityPage = components["schemas"]["ActivityPageResponse"];
export type ActivityRun = components["schemas"]["ActivityRunResponse"];
export type ActivityQuery = NonNullable<operations["list_activity"]["parameters"]["query"]>;

type ListActivityResponse = operations["list_activity"]["responses"][200]["content"]["application/json"];
type GetActivityActionResponse = operations["get_activity_action"]["responses"][200]["content"]["application/json"];

export function listActivity(
  workspaceId: string,
  query: ActivityQuery = {},
  signal?: AbortSignal,
): Promise<ListActivityResponse> {
  const parameters = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== "") parameters.set(key, String(value));
  }
  const suffix = parameters.size ? `?${parameters}` : "";
  return request(`/api/v1/workspaces/${workspaceId}/activity${suffix}`, { signal });
}

export function getActivityAction(
  workspaceId: string,
  actionId: string,
  signal?: AbortSignal,
): Promise<GetActivityActionResponse> {
  return request(`/api/v1/workspaces/${workspaceId}/activity/actions/${actionId}`, { signal });
}
