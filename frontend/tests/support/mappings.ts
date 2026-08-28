import { timestamp } from "./api.ts";

export function mappingSummary(
  mappingId: string,
  headContentHash: string,
  portableRevision: number,
  rowCount: number,
  activation: { activeContentHash: string | null; generation: number; updatedAt: string } | null,
) {
  return {
    mappingId,
    headContentHash,
    portableRevision,
    rowCount,
    createdAt: timestamp,
    updatedAt: timestamp,
    activation,
  };
}

export function mappingRevision(contentHash: string, portableRevision: number, rowCount: number) {
  return {
    contentHash,
    portableRevision,
    rowCount,
    origin: portableRevision === 1 ? "created" : "updated",
    createdAt: timestamp,
  };
}

export function mappingDetail(
  summary: {
    mappingId: string;
    headContentHash: string;
    activation: { activeContentHash: string | null; generation: number; updatedAt: string } | null;
  },
  contentHash: string,
  revision: number,
  mappings: string[][],
) {
  return {
    mappingId: summary.mappingId,
    contentHash,
    document: { schema: "trakkin.mapping/v1", revision, mappings },
    origin: revision === 3 ? "local" : "import",
    parents: revision === 3 ? [{ contentHash: `sha256:${"b".repeat(64)}`, verified: true }] : [],
    package: null,
    activation: summary.activation,
    recordCreatedAt: timestamp,
    recordUpdatedAt: timestamp,
    revisionCreatedAt: timestamp,
  };
}
