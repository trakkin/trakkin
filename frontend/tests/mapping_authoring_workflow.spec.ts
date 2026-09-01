import { expect, test } from "@playwright/test";
import {
  apiError,
  connection,
  connectionId,
  expectNoHorizontalOverflow,
  fallback,
  json,
  mockApi,
  source,
  sourceId,
  timestamp,
  workspace,
  workspaceId,
} from "./support/api.ts";
import { catalogEntry, catalogPage } from "./support/catalog.ts";
import { mappingDetail, mappingRevision, mappingSummary } from "./support/mappings.ts";

test("authors and activates portable mapping revisions", async ({ page }) => {
  const mappingId = "00000000-0000-0000-0000-000000000101";
  const firstHash = `sha256:${"1".repeat(64)}`;
  const externalHash = `sha256:${"2".repeat(64)}`;
  const savedHash = `sha256:${"3".repeat(64)}`;
  const initialRows = [["example.alt:item/one", "example.media:item/one"]];
  const externalRows = [...initialRows, ["example.alt:external", "example.media:external"]];
  let summary: ReturnType<typeof mappingSummary> | null = null;
  let headRows = initialRows;
  let revisions: Array<ReturnType<typeof mappingRevision>> = [];
  let updateAttempts = 0;
  let activationRequests = 0;
  let staleSaveRequested = false;
  let releaseStaleSave: () => void = () => {};
  const staleSaveGate = new Promise<void>((resolve) => {
    releaseStaleSave = resolve;
  });
  let staleCatalogPageRequested = false;
  let releaseStaleCatalogPage: () => void = () => {};
  const staleCatalogPageGate = new Promise<void>((resolve) => {
    releaseStaleCatalogPage = resolve;
  });
  let rootCatalogRequests = 0;
  let mismatchedPublicationRequested = false;

  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const decodedPath = decodeURIComponent(url.pathname);
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}`) return json(route, workspace);
    if (url.pathname === "/api/v1/connections") return json(route, [connection]);
    if (url.pathname === `/api/v1/connections/${connectionId}/sources`) return json(route, [source]);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks`) return json(route, []);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/sources/${sourceId}/catalog`) {
      if (url.searchParams.get("cursor") === "stale-page") {
        staleCatalogPageRequested = true;
        await staleCatalogPageGate;
        return json(route, catalogPage([catalogEntry("Old result", "media/old-result", "old", false, false)]));
      }
      if (url.searchParams.get("cursor") === "publication-page") {
        mismatchedPublicationRequested = true;
        return json(route, {
          ...catalogPage([catalogEntry("Mismatched publication", "media/mismatched", "mismatched", false, false)]),
          publicationId: "77777777-7777-7777-7777-777777777777",
        });
      }
      if (url.searchParams.get("search") === "fresh") {
        return json(route, catalogPage([catalogEntry("Fresh result", "media/fresh-result", "fresh", false, false)]));
      }
      rootCatalogRequests += 1;
      if (rootCatalogRequests >= 3) {
        return json(route, {
          ...catalogPage([catalogEntry("Current publication", "media/current", "current", false, false)]),
          publicationId: "77777777-7777-7777-7777-777777777777",
        });
      }
      return json(route, {
        ...catalogPage([catalogEntry("Quiet signal", "media/quiet-signal", "quiet", false, false)]),
        nextCursor: rootCatalogRequests === 1 ? "stale-page" : "publication-page",
      });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings` && method === "GET") {
      return json(route, { mappings: summary ? [summary] : [], nextCursor: null });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings` && method === "POST") {
      staleSaveRequested = true;
      await staleSaveGate;
      summary = mappingSummary(mappingId, firstHash, 1, 1, null);
      revisions = [mappingRevision(firstHash, 1, 1)];
      return json(route, mappingDetail(summary, firstHash, 1, initialRows), 201);
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings/${mappingId}` && method === "GET") {
      if (!summary) return fallback(route);
      return json(route, mappingDetail(summary, summary.headContentHash, summary.portableRevision, headRows));
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings/${mappingId}/revisions` && method === "GET") {
      return json(route, {
        mappingId,
        headContentHash: summary?.headContentHash ?? firstHash,
        revisions,
        nextCursor: null,
      });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings/${mappingId}/revisions` && method === "POST") {
      updateAttempts += 1;
      if (updateAttempts === 1) {
        summary = mappingSummary(mappingId, externalHash, 2, 2, null);
        headRows = externalRows;
        revisions = [mappingRevision(externalHash, 2, 2), mappingRevision(firstHash, 1, 1)];
        return json(
          route,
          {
            code: "head_moved",
            message: "Mapping head moved since the selected base revision",
            fieldProblems: [],
            currentHead: externalHash,
            added: [["example.alt:draft", "example.media:draft"]],
            removed: [["example.alt:external", "example.media:external"]],
          },
          409,
        );
      }
      summary = mappingSummary(mappingId, savedHash, 3, 1, null);
      headRows = [["example.alt:draft", "example.media:draft"]];
      revisions = [
        mappingRevision(savedHash, 3, 1),
        mappingRevision(externalHash, 2, 2),
        mappingRevision(firstHash, 1, 1),
      ];
      return json(route, mappingDetail(summary, savedHash, 3, headRows), 201);
    }
    if (
      decodedPath.startsWith(`/api/v1/workspaces/${workspaceId}/mappings/${mappingId}/revisions/`) &&
      method === "GET"
    ) {
      if (!summary) return fallback(route);
      const contentHash = decodedPath.slice(decodedPath.lastIndexOf("/") + 1);
      const revision = revisions.find((candidate) => candidate.contentHash === contentHash);
      const rows = contentHash === savedHash ? headRows : contentHash === externalHash ? externalRows : initialRows;
      return revision
        ? json(route, mappingDetail(summary, contentHash, revision.portableRevision, rows))
        : fallback(route);
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings/${mappingId}/activation` && method === "POST") {
      activationRequests += 1;
      if (activationRequests === 3) {
        return apiError(route, 404, "mapping_not_found", "Mapping no longer exists");
      }
      const body = await route.request().postDataJSON();
      const generation = (summary?.activation?.generation ?? 0) + 1;
      const activation = { activeContentHash: body.contentHash, generation, updatedAt: timestamp };
      if (summary) summary = { ...summary, activation };
      return json(route, activation);
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings/${mappingId}/retirement` && method === "POST") {
      const generation = (summary?.activation?.generation ?? 0) + 1;
      const activation = { activeContentHash: null, generation, updatedAt: timestamp };
      if (summary) summary = { ...summary, activation };
      return json(route, activation);
    }
    return fallback(route);
  });

  await page.goto(`/w/${workspaceId}/mappings`);
  await page.getByRole("button", { name: "New mapping" }).click();
  await page.getByRole("button", { name: "Catalog references" }).click();
  await expect(page.getByText("Quiet signal", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Load more" }).click();
  await expect.poll(() => staleCatalogPageRequested).toBe(true);
  await page.getByLabel("Search catalog references").fill("Fresh");
  await expect(page.getByText("Fresh result", { exact: true })).toBeVisible();
  releaseStaleCatalogPage();
  await expect(page.getByText("Old result", { exact: true })).not.toBeVisible();
  await expect(page.getByText("Fresh result", { exact: true })).toBeVisible();
  await page.getByLabel("Search catalog references").fill("");
  await expect(page.getByText("Quiet signal", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Load more" }).click();
  await expect.poll(() => mismatchedPublicationRequested).toBe(true);
  await expect(page.getByText("Current publication", { exact: true })).toBeVisible();
  await expect(page.getByText("Mismatched publication", { exact: true })).not.toBeVisible();
  await page.getByLabel("Search catalog references").fill("Fresh");
  await expect(page.getByText("Fresh result", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Add example.media:media/fresh-result to mapping row 1" }).click();
  await expect(page.getByRole("textbox", { name: "Portable references for row 1" })).toHaveValue(
    "example.media:media/fresh-result",
  );
  await page
    .getByRole("textbox", { name: "Portable references for row 1" })
    .fill("example.media:item/one\nexample.alt:item/one");
  await page.getByRole("button", { name: "Create mapping" }).click();
  await expect.poll(() => staleSaveRequested).toBe(true);
  await page.keyboard.press("Escape");
  await expect(page.getByRole("heading", { name: "New mapping" })).not.toBeVisible();
  await page.getByRole("button", { name: "New mapping" }).click();
  await expect(page.getByRole("heading", { name: "New mapping" })).toBeVisible();
  const staleSaveResponse = page.waitForResponse(
    (response) =>
      new URL(response.url()).pathname === `/api/v1/workspaces/${workspaceId}/mappings` &&
      response.request().method() === "POST",
  );
  releaseStaleSave();
  await staleSaveResponse;
  await page.evaluate(
    () => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))),
  );
  await expect(page.getByRole("heading", { name: "New mapping" })).toBeVisible();
  await page.getByLabel("Close mapping editor").click();

  await page.reload();
  await expect(page.getByText(mappingId, { exact: true })).toBeVisible();
  await page.getByRole("button", { name: `Inspect mapping ${mappingId}` }).click();
  await expect(page.getByRole("region", { name: "Portable mapping document" })).toContainText('"revision": 1');

  await page.getByRole("button", { name: "New revision" }).click();
  await page
    .getByRole("textbox", { name: "Portable references for row 1" })
    .fill("example.media:draft\nexample.alt:draft");
  await page.getByRole("button", { name: "Save revision" }).click();
  await expect(page.getByRole("alert")).toContainText("Mapping head moved");
  await expect(page.getByRole("alert")).toContainText("1 added · 1 removed");
  await page.getByRole("button", { name: "Rebase draft" }).click();
  await page.getByRole("button", { name: "Save revision" }).click();
  await expect(page.getByRole("region", { name: "Portable mapping document" })).toContainText('"revision": 3');

  await page.getByRole("button", { name: "Activate head" }).click();
  await expect(page.getByRole("button", { name: "Active head" })).toBeDisabled();
  await page
    .getByRole("region", { name: "Mapping history" })
    .getByRole("button", { name: "Inspect revision 1" })
    .click();
  await page.getByRole("button", { name: "Roll back to revision" }).click();
  await expect(page.getByRole("button", { name: "Active revision" })).toBeDisabled();
  await page.getByRole("button", { name: "Retire" }).click();
  await expect(page.getByText("Retired", { exact: true })).toBeVisible();

  await page
    .getByRole("region", { name: "Mapping history" })
    .getByRole("button", { name: "Inspect revision 3" })
    .click();
  await page.getByRole("button", { name: "Activate head" }).click();
  await expect(page.getByRole("alert")).toContainText("Mapping no longer exists");
  await expectNoHorizontalOverflow(page);
});
