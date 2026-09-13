import { bytes, sourceId, workspaceId } from "./api.ts";

export function catalogPage(entries: ReturnType<typeof catalogEntry>[]) {
  return {
    publicationId: "66666666-6666-6666-6666-666666666666",
    nextCursor: null,
    entries,
  };
}

export function catalogEntry(
  displayName: string,
  itemKey: string,
  relationKey: string,
  hasChildren: boolean,
  asset: boolean,
  referenceRoles: { recommended?: boolean; effective?: boolean } = {},
) {
  const itemValue = bytes(itemKey);
  const reference = {
    namespace: "example.media",
    value: itemValue,
    providerRecommended: referenceRoles.recommended ?? true,
    effectiveMappingRoot: referenceRoles.effective ?? true,
  };
  return {
    hasChildren,
    relation: {
      key: { namespace: "fixture", value: bytes(relationKey) },
      parentKey: null,
      providerItemKey: { namespace: "fixture", value: itemValue },
      kind: { namespace: "fixture", name: hasChildren ? "series" : "episode" },
      order: [1],
      attributes: {},
    },
    item: {
      key: { namespace: "fixture", value: itemValue },
      kind: { namespace: "fixture", name: hasChildren ? "show" : "episode" },
      displayName,
      attributes: {},
      portableReferenceCandidates: [reference],
      binaryAssets: asset
        ? [
            {
              key: { namespace: "fixture", value: bytes("poster") },
              kind: { namespace: "fixture", name: "poster" },
              href: `/api/v1/workspaces/${workspaceId}/sources/${sourceId}/catalog/66666666-6666-6666-6666-666666666666/assets/fixture/6d656469612f6e6f72746873746172/fixture/706f73746572`,
            },
          ]
        : [],
    },
  };
}

export function rootCatalog() {
  return catalogPage([catalogEntry("Northstar", "media/northstar", "root", true, true)]);
}

export function childCatalog() {
  return {
    publicationId: "66666666-6666-6666-6666-666666666666",
    nextCursor: null,
    entries: [catalogEntry("First light", "episode/1", "child", false, false)],
  };
}
