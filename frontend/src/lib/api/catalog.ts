import { request } from "./client.ts";
import type { components, operations } from "./generated/schema.ts";

export type CatalogAsset = components["schemas"]["BinaryAssetReferenceResponse"];
export type CatalogEntry = components["schemas"]["CatalogEntryResponse"];
export type CatalogItem = components["schemas"]["CatalogItemResponse"];
export type CatalogPage = components["schemas"]["CatalogPageResponse"];
export type CatalogRelation = components["schemas"]["CatalogRelationResponse"];
export type ProviderKey = components["schemas"]["KeyResponse"];
export type Term = components["schemas"]["TermResponse"];

export type ExternalReference = components["schemas"]["PortableReferenceResponse"] & { portable: boolean };
export type MappingInterface = {
  formatRevision: number;
  term: Term | null;
  semanticRole: Term | null;
  subjectScope: Term | null;
  componentInterface: Term | null;
  selectionModes: Term[];
  contractHash: number[];
};

export type CatalogPageOptions = {
  cursor?: string;
  limit?: number;
  parent?: ProviderKey;
  search?: string;
};

type GetCatalogResponse = operations["get_catalog"]["responses"][200]["content"]["application/json"];

export const CATALOG_PAGE_LIMIT = 60;

export function getCatalog(
  workspaceId: string,
  sourceId: string,
  options: CatalogPageOptions,
  signal?: AbortSignal,
): Promise<GetCatalogResponse> {
  const parameters = new URLSearchParams();
  if (options.cursor) parameters.set("cursor", options.cursor);
  parameters.set("limit", String(options.limit ?? CATALOG_PAGE_LIMIT));
  if (options.search) parameters.set("search", options.search);
  if (options.parent) {
    parameters.set("parentNamespace", options.parent.namespace);
    parameters.set("parentValue", hexBytes(options.parent.value));
  }
  return request(`/api/v1/workspaces/${workspaceId}/sources/${sourceId}/catalog?${parameters}`, { signal });
}

export function formatTerm(term: Term | null): string {
  return term ? term.name.replaceAll(/[-_]/g, " ") : "Unknown";
}

export function formatProviderKey(key: ProviderKey | null): string {
  if (!key) return "Unkeyed item";
  const bytes = hexBytes(key.value);
  const text = new TextDecoder(undefined, { fatal: true });
  try {
    const decoded = text.decode(Uint8Array.from(key.value));
    if (decoded && [...decoded].every((character) => !/\p{C}/u.test(character))) return decoded;
  } catch {}
  return `${key.namespace}:${bytes.slice(0, 16)}${bytes.length > 16 ? "..." : ""}`;
}

export function formatPortableReference(reference: components["schemas"]["PortableReferenceResponse"]): string {
  const value = reference.value
    .map((byte) =>
      isPortableReferenceRawByte(byte)
        ? String.fromCharCode(byte)
        : `%${byte.toString(16).toUpperCase().padStart(2, "0")}`,
    )
    .join("");
  return `${reference.namespace}:${value}`;
}

function isPortableReferenceRawByte(byte: number): boolean {
  return (
    (byte >= 0x30 && byte <= 0x39) ||
    (byte >= 0x41 && byte <= 0x5a) ||
    (byte >= 0x61 && byte <= 0x7a) ||
    byte === 0x2d ||
    byte === 0x2e ||
    byte === 0x2f ||
    byte === 0x5f ||
    byte === 0x7e
  );
}

function hexBytes(bytes: number[]): string {
  return bytes.map((value) => value.toString(16).padStart(2, "0")).join("");
}
