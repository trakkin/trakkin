import { request } from "./client.ts";
import type { components } from "./generated/schema.ts";

export type MappingState = components["schemas"]["MappingStateResponse"];
export type MappingResult = components["schemas"]["MappingResultResponse"];
export type MappingPage = components["schemas"]["MappingPageResponse"];
export type SaveMappingInput = components["schemas"]["SaveMappingRequest"];
export type MappingSource = components["schemas"]["MappingSourceResponse"];
export type MappingSources = components["schemas"]["MappingSourcesResponse"];
export type SaveMappingSourceInput = components["schemas"]["SaveMappingSourceRequest"];

export type MappingQueryOptions = {
  expectedFingerprint?: string;
  search?: string;
  claimId?: string;
  endpoint?: string;
  limit?: number;
  cursor?: string;
};

export const MAPPING_PAGE_LIMIT = 50;

export function getMappingState(signal?: AbortSignal): Promise<MappingState> {
  return request("/api/v1/mappings/state", { signal });
}

export function queryMappings(options: MappingQueryOptions, signal?: AbortSignal): Promise<MappingPage> {
  const parameters = new URLSearchParams();
  for (const field of ["expectedFingerprint", "search", "claimId", "endpoint", "cursor"] as const) {
    if (options[field]) parameters.set(field, options[field]);
  }
  parameters.set("limit", String(options.limit ?? MAPPING_PAGE_LIMIT));
  return request(`/api/v1/mappings?${parameters}`, { signal });
}

export function previewMapping(record: string, signal?: AbortSignal): Promise<MappingResult> {
  return request("/api/v1/mappings/preview", { method: "POST", body: { record }, signal });
}

export function saveMapping(body: SaveMappingInput, signal?: AbortSignal): Promise<MappingState> {
  return request("/api/v1/mappings", { method: "POST", body, signal });
}

export function decideMapping(
  claimId: string,
  expectedFingerprint: string,
  rejected: boolean,
  signal?: AbortSignal,
): Promise<MappingState> {
  return request(`/api/v1/mappings/${encodeURIComponent(claimId)}/decision`, {
    method: "POST",
    body: { expectedFingerprint, rejected },
    signal,
  });
}

export function deleteMapping(
  claimId: string,
  expectedFingerprint: string,
  signal?: AbortSignal,
): Promise<MappingState> {
  return request(`/api/v1/mappings/${encodeURIComponent(claimId)}`, {
    method: "DELETE",
    body: { expectedFingerprint },
    signal,
  });
}

export function getMappingSources(signal?: AbortSignal): Promise<MappingSources> {
  return request("/api/v1/mappings/sources", { signal });
}

export function saveMappingSource(
  body: SaveMappingSourceInput,
  id?: string,
  signal?: AbortSignal,
): Promise<MappingState> {
  return request(id ? `/api/v1/mappings/sources/${encodeURIComponent(id)}` : "/api/v1/mappings/sources", {
    method: id ? "PUT" : "POST",
    body,
    signal,
  });
}

export function deleteMappingSource(
  id: string,
  expectedFingerprint: string,
  signal?: AbortSignal,
): Promise<MappingState> {
  return request(`/api/v1/mappings/sources/${encodeURIComponent(id)}`, {
    method: "DELETE",
    body: { expectedFingerprint },
    signal,
  });
}

export function refreshMappingSource(
  id: string,
  expectedFingerprint: string,
  signal?: AbortSignal,
): Promise<MappingState> {
  return request(`/api/v1/mappings/sources/${encodeURIComponent(id)}/refresh`, {
    method: "POST",
    body: { expectedFingerprint },
    signal,
  });
}

export function revertMapping(
  claimId: string,
  expectedFingerprint: string,
  signal?: AbortSignal,
): Promise<MappingState> {
  return request(`/api/v1/mappings/${encodeURIComponent(claimId)}/revert`, {
    method: "POST",
    body: { expectedFingerprint },
    signal,
  });
}
