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
  timestamp,
  workspace,
  workspaceId,
} from "./support/api.ts";
import { mappingDetail } from "./support/mappings.ts";

test("inspects paged portable mapping history", async ({ page }) => {
  let mappingRegistryRequests = 0;
  let expansionAttempts = 0;
  let staleExpansionRequested = false;
  let releaseStaleExpansion: () => void = () => {};
  const staleExpansionGate = new Promise<void>((resolve) => {
    releaseStaleExpansion = resolve;
  });
  let staleRevisionRequested = false;
  let releaseStaleRevision: () => void = () => {};
  const staleRevisionGate = new Promise<void>((resolve) => {
    releaseStaleRevision = resolve;
  });
  let staleRetirementRequested = false;
  let selectedActivationRequests = 0;
  let releaseStaleRetirement: () => void = () => {};
  const staleRetirementGate = new Promise<void>((resolve) => {
    releaseStaleRetirement = resolve;
  });
  const mappingSummaries = Array.from({ length: 26 }, (_, index) => {
    const ordinal = index + 1;
    const headContentHash = `sha256:${ordinal.toString(16).padStart(64, "0")}`;
    return {
      mappingId: `00000000-0000-0000-0000-${ordinal.toString().padStart(12, "0")}`,
      headContentHash,
      portableRevision: ordinal === 1 ? 3 : 1,
      rowCount: ordinal * 2,
      createdAt: timestamp,
      updatedAt: timestamp,
      activation:
        ordinal === 1
          ? { activeContentHash: headContentHash, generation: 4, updatedAt: timestamp }
          : ordinal === 2
            ? { activeContentHash: null, generation: 5, updatedAt: timestamp }
            : null,
    };
  });
  const current = mappingSummaries[0];
  const selected = mappingSummaries[2];
  const historicalHash = `sha256:${"f".repeat(64)}`;
  const revisions = [
    {
      contentHash: current.headContentHash,
      portableRevision: 3,
      rowCount: 2,
      origin: "updated",
      createdAt: timestamp,
    },
    {
      contentHash: historicalHash,
      portableRevision: 2,
      rowCount: 1,
      origin: "imported",
      createdAt: "2029-12-01T00:00:00Z",
    },
  ];

  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const decodedPath = decodeURIComponent(url.pathname);
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings`) {
      mappingRegistryRequests += 1;
      if (mappingRegistryRequests === 1) {
        return apiError(route, 503, "mappings_unavailable", "Mapping registry is temporarily unavailable");
      }
      return url.searchParams.get("cursor") === "mapping-page-2"
        ? json(route, { mappings: mappingSummaries.slice(25), nextCursor: null })
        : json(route, { mappings: mappingSummaries.slice(0, 25), nextCursor: "mapping-page-2" });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings/${mappingSummaries[1].mappingId}`) {
      expansionAttempts += 1;
      if (expansionAttempts === 1) {
        staleExpansionRequested = true;
        await staleExpansionGate;
      }
      return apiError(route, 404, "mapping_not_found", "Mapping no longer exists");
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings/${mappingSummaries[1].mappingId}/revisions`) {
      return json(route, {
        mappingId: mappingSummaries[1].mappingId,
        headContentHash: mappingSummaries[1].headContentHash,
        revisions: [],
        nextCursor: null,
      });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings/${current.mappingId}/revisions`) {
      return json(route, {
        mappingId: current.mappingId,
        headContentHash: current.headContentHash,
        revisions,
        nextCursor: null,
      });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings/${selected.mappingId}/revisions`) {
      return json(route, {
        mappingId: selected.mappingId,
        headContentHash: selected.headContentHash,
        revisions: [],
        nextCursor: null,
      });
    }
    if (decodedPath === `/api/v1/workspaces/${workspaceId}/mappings/${current.mappingId}/revisions/${historicalHash}`) {
      staleRevisionRequested = true;
      await staleRevisionGate;
      return json(
        route,
        mappingDetail(current, historicalHash, 2, [["catalog.example://title/101", "metadata.example://movie/101"]]),
      );
    }
    if (
      decodedPath ===
      `/api/v1/workspaces/${workspaceId}/mappings/${current.mappingId}/revisions/${current.headContentHash}`
    ) {
      return json(
        route,
        mappingDetail(current, current.headContentHash, 3, [
          ["catalog.example://title/101", "metadata.example://movie/101"],
          ["registry.example://series/202", "metadata.example://show/202"],
        ]),
      );
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings/${current.mappingId}`) {
      return json(
        route,
        mappingDetail(current, current.headContentHash, 3, [
          ["catalog.example://title/101", "metadata.example://movie/101"],
          ["registry.example://series/202", "metadata.example://show/202"],
        ]),
      );
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings/${selected.mappingId}`) {
      return json(
        route,
        mappingDetail(selected, selected.headContentHash, 1, [
          ["catalog.example://title/303", "metadata.example://movie/303"],
        ]),
      );
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings/${current.mappingId}/retirement`) {
      staleRetirementRequested = true;
      await staleRetirementGate;
      return apiError(route, 404, "mapping_not_found", "Mapping no longer exists");
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/mappings/${selected.mappingId}/activation`) {
      selectedActivationRequests += 1;
      return json(route, {
        activeContentHash: selected.headContentHash,
        generation: 1,
        updatedAt: timestamp,
      });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}`) return json(route, workspace);
    if (url.pathname === "/api/v1/connections") return json(route, [connection]);
    if (url.pathname === `/api/v1/connections/${connectionId}/sources`) return json(route, [source]);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks`) return json(route, []);
    return fallback(route);
  });

  await page.goto(`/w/${workspaceId}/mappings`);
  await expect(page.getByRole("heading", { name: "Mappings" })).toBeVisible();
  const registryError = page.getByRole("alert").filter({ hasText: "Mapping registry is temporarily unavailable" });
  await expect(registryError).toBeVisible();
  await expect(page.getByRole("button", { name: "New mapping" })).toBeDisabled();
  await expect(page.getByLabel("Refresh mappings")).toBeDisabled();
  await registryError.getByRole("button", { name: "Retry" }).click();
  await expect.poll(() => mappingRegistryRequests).toBe(2);
  await expect(page.getByText("Active head", { exact: true })).toBeVisible();
  await expect(page.getByText("Retired", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "New mapping" })).toBeEnabled();
  await expect(page.getByLabel("Refresh mappings")).toBeEnabled();
  await expectNoHorizontalOverflow(page);
  const navigationToggle = page.getByRole("button", { name: "Open navigation" }).filter({ visible: true });
  if (await navigationToggle.count()) {
    await navigationToggle.click();
    await expect(page.getByRole("menuitem", { name: "Mappings" })).toHaveAttribute("aria-current", "page");
    await page.keyboard.press("Escape");
  } else {
    await expect(page.getByRole("link", { name: "Mappings" })).toHaveAttribute("href", `/w/${workspaceId}/mappings`);
  }

  await page.getByRole("button", { name: `Inspect mapping ${mappingSummaries[1].mappingId}` }).click();
  await expect.poll(() => staleExpansionRequested).toBe(true);

  await page.getByRole("button", { name: `Inspect mapping ${current.mappingId}` }).click();
  await expect(page.getByRole("region", { name: "Portable mapping document" })).toContainText(
    '"schema": "trakkin.mapping/v1"',
  );
  const staleExpansionResponse = page.waitForResponse(
    (response) =>
      decodeURIComponent(new URL(response.url()).pathname) ===
        `/api/v1/workspaces/${workspaceId}/mappings/${mappingSummaries[1].mappingId}` && response.status() === 404,
  );
  releaseStaleExpansion();
  await staleExpansionResponse;
  await page.evaluate(
    () => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))),
  );
  await expect(page.getByRole("button", { name: `Collapse mapping ${current.mappingId}` })).toBeVisible();
  await expect(page.getByRole("alert")).not.toBeVisible();

  await page.getByRole("button", { name: `Inspect mapping ${mappingSummaries[1].mappingId}` }).click();
  await expect(page.getByRole("alert")).toContainText("Mapping no longer exists");
  await page.getByRole("button", { name: "Dismiss mapping error" }).click();

  await page.getByRole("button", { name: `Inspect mapping ${current.mappingId}` }).click();
  await expect(page.getByRole("region", { name: "Portable mapping document" })).toContainText(
    '"schema": "trakkin.mapping/v1"',
  );
  const historyButtons = page.getByRole("region", { name: "Mapping history" }).getByRole("button");
  await expect(historyButtons).toHaveCount(2);
  await expect(historyButtons.nth(0)).toHaveAccessibleName("Inspect revision 3");
  await expect(historyButtons.nth(1)).toHaveAccessibleName("Inspect revision 2");
  await historyButtons.nth(1).click();
  await expect.poll(() => staleRevisionRequested).toBe(true);
  await historyButtons.nth(0).click();
  await expect(page.getByRole("region", { name: "Portable mapping document" })).toContainText('"revision": 3');
  releaseStaleRevision();
  await expect(page.getByRole("region", { name: "Portable mapping document" })).not.toContainText('"revision": 2');
  await expect(page.getByRole("region", { name: "Portable mapping document" })).toContainText('"revision": 3');

  await page.getByRole("button", { name: "Retire" }).click();
  await expect.poll(() => staleRetirementRequested).toBe(true);
  await page.getByRole("button", { name: `Inspect mapping ${selected.mappingId}` }).click();
  await expect(page.getByRole("region", { name: "Portable mapping document" })).toContainText(
    "catalog.example://title/303",
  );
  await expect(page.getByRole("button", { name: "Activate head" })).toBeEnabled();
  await page.getByRole("button", { name: "Activate head" }).click();
  await expect.poll(() => selectedActivationRequests).toBe(1);
  await expect(page.getByRole("button", { name: "Active head" })).toBeDisabled();
  const staleRetirementResponse = page.waitForResponse(
    (response) =>
      new URL(response.url()).pathname === `/api/v1/workspaces/${workspaceId}/mappings/${current.mappingId}/retirement`,
  );
  releaseStaleRetirement();
  await staleRetirementResponse;
  await page.evaluate(
    () => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))),
  );
  await expect(page.getByRole("button", { name: "Active head" })).toBeDisabled();
  await expect(page.getByRole("alert")).not.toBeVisible();
  await expectNoHorizontalOverflow(page);

  await page.getByRole("button", { name: "Next mappings page" }).click();
  await expect(page.getByText(mappingSummaries[25].mappingId, { exact: true })).toBeVisible();
  await expect(page.getByText("Page 2", { exact: true })).toBeVisible();
});
