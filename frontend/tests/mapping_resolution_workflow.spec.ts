import { expect, test } from "@playwright/test";
import {
  connection,
  connectionId,
  expectNoHorizontalOverflow,
  fallback,
  json,
  mockApi,
  source,
  timestamp,
  workspace,
  workspaceId,
} from "./support/api.ts";
import { mappingDetail, mappingRevision, mappingSummary } from "./support/mappings.ts";

test("previews exact portable mapping revisions", async ({ page }) => {
  const mappingId = "00000000-0000-0000-0000-000000000201";
  const contentHash = `sha256:${"4".repeat(64)}`;
  const historicalHash = `sha256:${"3".repeat(64)}`;
  const summary = mappingSummary(mappingId, contentHash, 2, 2, null);
  const mappings = [
    ["example.media://item/one", "example.alt://item/one"],
    ["example.alt://item/one", "example.third://item/one"],
  ];
  const historicalMappings = [["example.media://item/historical", "example.alt://item/historical"]];
  let previewRequests = 0;
  let deferredPreviewRequested = false;
  let deferredPreviewCompleted = false;
  let releaseDeferredPreview: () => void = () => {};
  const deferredPreviewGate = new Promise<void>((resolve) => {
    releaseDeferredPreview = resolve;
  });

  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const decodedPath = decodeURIComponent(url.pathname);
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}`) return json(route, workspace);
    if (url.pathname === "/api/v1/connections") return json(route, [connection]);
    if (url.pathname === `/api/v1/connections/${connectionId}/sources`) return json(route, [source]);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks`) return json(route, []);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings`) {
      return json(route, { mappings: [summary], nextCursor: null });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings/${mappingId}`) {
      return json(route, mappingDetail(summary, contentHash, 2, mappings));
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings/${mappingId}/revisions`) {
      return json(route, {
        mappingId,
        headContentHash: contentHash,
        revisions: [mappingRevision(contentHash, 2, 2), mappingRevision(historicalHash, 1, 1)],
        nextCursor: null,
      });
    }
    if (decodedPath === `/api/v1/workspaces/${workspaceId}/mappings/${mappingId}/revisions/${historicalHash}`) {
      return json(route, mappingDetail(summary, historicalHash, 1, historicalMappings));
    }
    if (
      decodedPath ===
        `/api/v1/workspaces/${workspaceId}/mappings/${mappingId}/revisions/${contentHash}/resolution-preview` &&
      method === "POST"
    ) {
      const body = (await route.request().postDataJSON()) as { mode: "direct" | "transitive" };
      previewRequests += 1;
      if (previewRequests === 3) {
        deferredPreviewRequested = true;
        await deferredPreviewGate;
        deferredPreviewCompleted = true;
      }
      return json(route, mappingResolutionPreview(mappingId, contentHash, body.mode));
    }
    return fallback(route);
  });

  await page.goto(`/w/${workspaceId}/mappings`);
  await page.getByRole("button", { name: `Inspect mapping ${mappingId}` }).click();
  const preview = page.getByRole("region", { name: "Resolution preview" });
  await preview.getByRole("button", { name: "Preview" }).click();
  await expect(preview.getByText("complete", { exact: true })).toBeVisible();
  await expect(preview.getByRole("region", { name: "Resolved correspondences" })).toContainText(
    "example.alt://item/one",
  );

  await preview.getByRole("button", { name: "Transitive" }).click();
  await preview
    .getByRole("textbox", { name: "Requested portable endpoints" })
    .fill("example.media://item/one\nexample.media://missing");
  await preview.getByRole("textbox", { name: "Finite resolution window" }).fill("episode:1..24");
  await preview.getByRole("button", { name: "Preview" }).click();
  await expect(preview.getByText("partial", { exact: true })).toBeVisible();
  await expect(preview.getByRole("region", { name: "Resolved correspondences" })).toContainText(
    "example.third://item/one",
  );
  await expect(preview.getByRole("region", { name: "Resolution findings" })).toContainText("unresolved");
  await expect(preview.getByRole("region", { name: "Resolution findings" })).toContainText("ambiguous");

  await preview.getByRole("button", { name: "Preview" }).click();
  await expect.poll(() => deferredPreviewRequested).toBe(true);
  await page
    .getByRole("region", { name: "Mapping history" })
    .getByRole("button", { name: "Inspect revision 1" })
    .click();
  await expect(preview.getByRole("textbox", { name: "Requested portable endpoints" })).toHaveValue(
    "example.media://item/historical\nexample.alt://item/historical",
  );
  await expect(preview.getByRole("button", { name: "Direct" })).toHaveAttribute("aria-pressed", "true");
  releaseDeferredPreview();
  await expect.poll(() => deferredPreviewCompleted).toBe(true);
  await page.evaluate(
    () => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))),
  );
  await expect(preview.getByText("partial", { exact: true })).toHaveCount(0);
  await expect(preview.getByRole("region", { name: "Resolved correspondences" })).toHaveCount(0);
  await expect(preview.getByRole("button", { name: "Preview" })).toBeEnabled();
  await expectNoHorizontalOverflow(page);
});

function mappingResolutionPreview(mappingId: string, contentHash: string, mode: "direct" | "transitive") {
  const proof = {
    catalogPublications: [],
    coordinatePublications: [],
    endpointHash: `sha256:${"5".repeat(64)}`,
    key: `${mode}:proof`,
    kind: mode === "direct" ? "direct" : "derived",
    parentProofs: [],
    revisionHash: contentHash,
    rowHash: `sha256:${"6".repeat(64)}`,
    transform: { numerator: "1", denominator: "1" },
  };
  const transitive = mode === "transitive";
  return {
    mappingId,
    contentHash,
    mode,
    resolvedAt: timestamp,
    completeness: transitive ? "partial" : "complete",
    correspondences: [
      {
        endpoints: transitive
          ? ["example.media://item/one", "example.third://item/one"]
          : ["example.media://item/one", "example.alt://item/one"],
        segment: null,
        proof,
        supportingProofs: [proof],
        supportCount: 1,
      },
    ],
    findings: transitive
      ? [
          { endpoint: "example.media://missing", kind: "unresolved", span: null, proofPaths: [] },
          { endpoint: "example.media://ambiguous", kind: "ambiguous", span: null, proofPaths: [] },
        ]
      : [],
    publishedEvidence: [],
    lookupEvidence: [],
    semanticWork: {
      endpointHits: transitive ? 3 : 2,
      candidateRows: transitive ? 2 : 1,
      traversalHops: transitive ? 2 : 0,
      partitions: 1,
      lookupReferences: 0,
      lookupCalls: 0,
      lookupBytes: 0,
    },
    transportLookupCalls: 0,
    nextWorkKey: null,
  };
}
