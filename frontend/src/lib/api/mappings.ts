import { request } from "./client.ts";
import type { components } from "./generated/schema.ts";

export type MappingSource = components["schemas"]["MappingSourceResponse"];
export type MappingState = components["schemas"]["MappingStateResponse"];
export type MappingResult = components["schemas"]["MappingResultResponse"];
export type MappingPage = components["schemas"]["MappingPageResponse"];

export type CreateUploadedMappingSourceInput = {
  sourceKey: string;
  displayName: string;
  enabled: boolean;
};

export type CreateRemoteMappingSourceInput = components["schemas"]["CreateRemoteMappingSourceRequest"];
export type UpdateMappingSourceInput = components["schemas"]["UpdateMappingSourceRequest"];
export type ReorderMappingSourcesInput = components["schemas"]["ReorderMappingSourcesRequest"];
export type WriteLocalMappingInput = components["schemas"]["WriteLocalMappingRequest"];

export type MappingQueryOptions = {
  expectedChainFingerprint?: string;
  search?: string;
  sourceKey?: string;
  includeShadowed?: boolean;
  limit?: number;
  cursor?: string;
};

export const MAPPING_PAGE_LIMIT = 50;

export function getMappingState(signal?: AbortSignal): Promise<MappingState> {
  return request("/api/v1/mapping-sources", { signal });
}

export function createUploadedMappingSource(
  input: CreateUploadedMappingSourceInput,
  content: string,
  signal?: AbortSignal,
): Promise<MappingState> {
  const parameters = new URLSearchParams({
    sourceKey: input.sourceKey,
    displayName: input.displayName,
    enabled: String(input.enabled),
  });
  return request(`/api/v1/mapping-sources/uploads?${parameters}`, {
    method: "POST",
    rawBody: content,
    headers: { "content-type": "text/plain; charset=utf-8" },
    signal,
  });
}

export function createRemoteMappingSource(
  input: CreateRemoteMappingSourceInput,
  signal?: AbortSignal,
): Promise<MappingState> {
  return request("/api/v1/mapping-sources/remotes", { method: "POST", body: input, signal });
}

export function updateMappingSource(
  sourceId: string,
  input: UpdateMappingSourceInput,
  signal?: AbortSignal,
): Promise<MappingState> {
  return request(`/api/v1/mapping-sources/${encodeURIComponent(sourceId)}`, {
    method: "PATCH",
    body: input,
    signal,
  });
}

export function createLocalMapping(input: WriteLocalMappingInput, signal?: AbortSignal): Promise<MappingState> {
  return request("/api/v1/mappings/local", { method: "POST", body: input, signal });
}

export function updateLocalMapping(
  statementId: string,
  input: WriteLocalMappingInput,
  signal?: AbortSignal,
): Promise<MappingState> {
  return request(`/api/v1/mappings/local/${encodeURIComponent(statementId)}`, {
    method: "PUT",
    body: input,
    signal,
  });
}

export function removeLocalMapping(statementId: string, signal?: AbortSignal): Promise<MappingState> {
  return request(`/api/v1/mappings/local/${encodeURIComponent(statementId)}`, { method: "DELETE", signal });
}

export function reorderMappingSources(input: ReorderMappingSourcesInput, signal?: AbortSignal): Promise<MappingState> {
  return request("/api/v1/mapping-sources/order", { method: "PUT", body: input, signal });
}

export function removeMappingSource(sourceId: string, signal?: AbortSignal): Promise<MappingState> {
  return request(`/api/v1/mapping-sources/${encodeURIComponent(sourceId)}`, {
    method: "DELETE",
    signal,
  });
}

export function refreshRemoteMappingSource(sourceId: string, signal?: AbortSignal): Promise<MappingState> {
  return request(`/api/v1/mapping-sources/${encodeURIComponent(sourceId)}/refresh`, {
    method: "POST",
    signal,
  });
}

export function queryMappings(options: MappingQueryOptions, signal?: AbortSignal): Promise<MappingPage> {
  const parameters = new URLSearchParams();
  if (options.expectedChainFingerprint) {
    parameters.set("expectedChainFingerprint", options.expectedChainFingerprint);
  }
  if (options.search) parameters.set("search", options.search);
  if (options.sourceKey) parameters.set("sourceKey", options.sourceKey);
  if (options.includeShadowed) parameters.set("includeShadowed", "true");
  parameters.set("limit", String(options.limit ?? MAPPING_PAGE_LIMIT));
  if (options.cursor) parameters.set("cursor", options.cursor);
  return request(`/api/v1/mappings?${parameters}`, { signal });
}
