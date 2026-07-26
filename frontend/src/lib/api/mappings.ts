import { ApiRequestError, request } from "./client.ts";
import type { components, operations } from "./generated/schema.ts";

export type MappingSummary = components["schemas"]["MappingSummaryResponse"];
export type MappingDetail = components["schemas"]["MappingDetailResponse"];
export type MappingRevisionSummary = components["schemas"]["MappingRevisionSummaryResponse"];
export type MappingActivation = components["schemas"]["MappingActivationResponse"];
export type MappingHeadMoved = components["schemas"]["MappingHeadMovedResponse"];
export type MappingResolutionPreview = components["schemas"]["MappingResolutionPreviewResponse"];
export type MappingResolutionPreviewRequest = components["schemas"]["MappingResolutionPreviewRequest"];
export type MappingRow = MappingDetail["document"]["mappings"][number];

type ListMappingsResponse = operations["list_mappings"]["responses"][200]["content"]["application/json"];
type GetMappingResponse = operations["get_mapping"]["responses"][200]["content"]["application/json"];
type ListMappingRevisionsResponse =
  operations["list_mapping_revisions"]["responses"][200]["content"]["application/json"];
type GetMappingRevisionResponse = operations["get_mapping_revision"]["responses"][200]["content"]["application/json"];
type CreateMappingRequest = operations["create_mapping"]["requestBody"]["content"]["application/json"];
type CreateMappingResponse = operations["create_mapping"]["responses"][201]["content"]["application/json"];
type CreateMappingRevisionRequest = operations["create_mapping_revision"]["requestBody"]["content"]["application/json"];
type CreateMappingRevisionResponse =
  operations["create_mapping_revision"]["responses"][201]["content"]["application/json"];
type ActivateMappingRevisionRequest =
  operations["activate_mapping_revision"]["requestBody"]["content"]["application/json"];
type ActivateMappingRevisionResponse =
  operations["activate_mapping_revision"]["responses"][200]["content"]["application/json"];
type RetireMappingResponse = operations["retire_mapping"]["responses"][200]["content"]["application/json"];
type PreviewMappingResolutionResponse =
  operations["preview_mapping_resolution"]["responses"][200]["content"]["application/json"];

export class MappingHeadMovedError extends Error {
  constructor(readonly conflict: MappingHeadMoved) {
    super(conflict.message);
    this.name = "MappingHeadMovedError";
  }
}

export function listMappingRegistry(
  workspaceId: string,
  options: { cursor?: string; limit?: number } = {},
  signal?: AbortSignal,
): Promise<ListMappingsResponse> {
  const parameters = new URLSearchParams();
  parameters.set("limit", String(options.limit ?? 25));
  if (options.cursor) parameters.set("cursor", options.cursor);
  return request(`/api/v1/workspaces/${workspaceId}/mappings?${parameters}`, { signal });
}

export function getMappingDetail(
  workspaceId: string,
  mappingId: string,
  signal?: AbortSignal,
): Promise<GetMappingResponse> {
  return request(`/api/v1/workspaces/${workspaceId}/mappings/${mappingId}`, { signal });
}

export function listMappingHistory(
  workspaceId: string,
  mappingId: string,
  options: { cursor?: string; limit?: number } = {},
  signal?: AbortSignal,
): Promise<ListMappingRevisionsResponse> {
  const parameters = new URLSearchParams();
  parameters.set("limit", String(options.limit ?? 25));
  if (options.cursor) parameters.set("cursor", options.cursor);
  return request(`/api/v1/workspaces/${workspaceId}/mappings/${mappingId}/revisions?${parameters}`, { signal });
}

export function getMappingRevisionDetail(
  workspaceId: string,
  mappingId: string,
  contentHash: string,
  signal?: AbortSignal,
): Promise<GetMappingRevisionResponse> {
  return request(
    `/api/v1/workspaces/${workspaceId}/mappings/${mappingId}/revisions/${encodeURIComponent(contentHash)}`,
    { signal },
  );
}

export function createMappingDocument(workspaceId: string, mappings: MappingRow[]): Promise<CreateMappingResponse> {
  const body: CreateMappingRequest = { schema: "trakkin.mapping/v1", mappings };
  return request(`/api/v1/workspaces/${workspaceId}/mappings`, { method: "POST", body });
}

export async function createMappingRevision(
  workspaceId: string,
  mappingId: string,
  baseContentHash: string,
  mappings: MappingRow[],
): Promise<CreateMappingRevisionResponse> {
  const body: CreateMappingRevisionRequest = { baseContentHash, mappings };
  try {
    return await request(`/api/v1/workspaces/${workspaceId}/mappings/${mappingId}/revisions`, {
      method: "POST",
      body,
    });
  } catch (error) {
    if (error instanceof ApiRequestError && isHeadMoved(error)) {
      throw new MappingHeadMovedError(error.body as MappingHeadMoved);
    }
    throw error;
  }
}

export function activateMappingRevision(
  workspaceId: string,
  mappingId: string,
  contentHash: string,
): Promise<ActivateMappingRevisionResponse> {
  const body: ActivateMappingRevisionRequest = { contentHash };
  return request(`/api/v1/workspaces/${workspaceId}/mappings/${mappingId}/activation`, {
    method: "POST",
    body,
  });
}

export function retireMapping(workspaceId: string, mappingId: string): Promise<RetireMappingResponse> {
  return request(`/api/v1/workspaces/${workspaceId}/mappings/${mappingId}/retirement`, { method: "POST" });
}

export function previewMappingResolution(
  workspaceId: string,
  mappingId: string,
  contentHash: string,
  body: MappingResolutionPreviewRequest,
  signal?: AbortSignal,
): Promise<PreviewMappingResolutionResponse> {
  return request(
    `/api/v1/workspaces/${workspaceId}/mappings/${mappingId}/revisions/${encodeURIComponent(contentHash)}/resolution-preview`,
    { method: "POST", body, signal },
  );
}

function isHeadMoved(error: ApiRequestError): boolean {
  const body = error.body as Partial<MappingHeadMoved>;
  return (
    error.status === 409 &&
    error.code === "head_moved" &&
    typeof body.currentHead === "string" &&
    Array.isArray(body.added) &&
    Array.isArray(body.removed)
  );
}
