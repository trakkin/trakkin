import { expect, test } from "@playwright/test";
import { expectNoHorizontalOverflow, fallback, json, mockApi, timestamp, workspace } from "./support/api.ts";

const localSource = {
  id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  sourceKey: "local",
  displayName: "Local mappings",
  kind: "local",
  remoteUrl: null,
  priority: 0,
  enabled: true,
  contentHash: "a".repeat(64),
  byteLength: 0,
  lastCheckedAt: null,
  lastRefreshError: null,
  createdAt: timestamp,
  updatedAt: timestamp,
};

const remoteSource = {
  id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
  sourceKey: "community",
  displayName: "Community mappings",
  kind: "remote",
  remoteUrl: "https://example.test/mappings.trakkin",
  priority: 1,
  enabled: true,
  contentHash: "b".repeat(64),
  byteLength: 24,
  lastCheckedAt: timestamp,
  lastRefreshError: null,
  createdAt: timestamp,
  updatedAt: timestamp,
};

const uploadedSource = {
  id: "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
  sourceKey: "imported",
  displayName: "Imported mappings",
  kind: "upload",
  remoteUrl: null,
  priority: 2,
  enabled: true,
  contentHash: "c".repeat(64),
  byteLength: 28,
  lastCheckedAt: null,
  lastRefreshError: null,
  createdAt: timestamp,
  updatedAt: timestamp,
};

type MappingOccurrence = ReturnType<typeof occurrence>;

function occurrence(
  sourceKey: string,
  statementId: string,
  statement: string,
  options: { active?: boolean; metadata?: string[] } = {},
) {
  return {
    relationKey: `${statementId}-relation`,
    statementId,
    canonicalStatement: statement,
    canonicalRecord: statement,
    operator: "<=>",
    sourceKey,
    sourceContentHash: sourceKey === "local" ? localSource.contentHash : remoteSource.contentHash,
    layerPosition: sourceKey === "local" ? 0 : 1,
    recordOrdinal: 0,
    active: options.active ?? true,
    occurrenceCount: 1,
    evidenceFingerprint: "d".repeat(64),
    metadata: options.metadata ?? [],
  };
}

test("uses route tabs and a compact relation list without a detail sidebar", async ({ page }) => {
  const queryUrls: URL[] = [];
  const imported = occurrence("community", "managed", "co.anilist://anime/1 <=> org.themoviedb://tv/2", {
    metadata: ["#@verified community"],
  });

  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces" && method === "GET") return json(route, [workspace]);
    if (url.pathname === "/api/v1/mapping-sources" && method === "GET") {
      return json(route, {
        activeChainFingerprint: "e".repeat(64),
        sources: [localSource, remoteSource],
      });
    }
    if (url.pathname === "/api/v1/mappings" && method === "GET") {
      queryUrls.push(url);
      return json(route, {
        chainFingerprint: "e".repeat(64),
        items: [occurrence("local", "local", "co.anilist://anime/10 <=> org.themoviedb://tv/20"), imported],
        nextCursor: null,
      });
    }
    return fallback(route);
  });

  await page.goto("/mappings");

  const viewNavigation = page.getByRole("navigation", { name: "Mapping views" });
  await expect(viewNavigation.getByRole("link", { name: "Mappings" })).toHaveAttribute("aria-current", "page");
  await expect(viewNavigation.getByRole("link", { name: "Sources" })).not.toHaveAttribute("aria-current", "page");
  await expect(page.getByRole("table")).toHaveCount(0);
  await expect(page.getByRole("complementary", { name: "Mapping details" })).toHaveCount(0);
  await expect(page.getByRole("heading", { name: "Source precedence" })).toHaveCount(0);

  const overrideButton = page.getByRole("button", {
    name: "Override co.anilist://anime/1 <=> org.themoviedb://tv/2 locally",
  });
  const importedRow = page.locator("li").filter({ has: overrideButton });
  await expect(importedRow).toContainText("Community mappings");
  const annotationButton = importedRow.locator('button[aria-controls^="mapping-annotations-"]');
  await expect(annotationButton).toHaveAttribute("aria-expanded", "false");
  const annotationRegionId = await annotationButton.getAttribute("aria-controls");
  expect(annotationRegionId).toBeTruthy();
  await annotationButton.click();
  await expect(annotationButton).toHaveAttribute("aria-expanded", "true");
  await expect(page.locator(`#${annotationRegionId}`)).toBeVisible();
  await expect(importedRow.getByText("#@verified community", { exact: true })).toBeVisible();

  await overrideButton.click();
  const overrideDialog = page.getByRole("dialog", { name: "Override mapping locally" });
  await expect(overrideDialog.getByLabel("Mapping record")).toHaveValue(
    "co.anilist://anime/1 <=> org.themoviedb://tv/2",
  );
  await overrideDialog.getByRole("button", { name: "Cancel" }).click();

  await page.getByRole("tab", { name: "All occurrences" }).click();
  await expect.poll(() => queryUrls.some((url) => url.searchParams.get("includeShadowed") === "true")).toBe(true);
  await expectNoHorizontalOverflow(page);
});

test("creates, edits, and deletes local mappings from the relation list", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  let fingerprintSequence = 1;
  let rows: MappingOccurrence[] = [
    occurrence("local", "local-one", "co.anilist://anime/10 <=> org.themoviedb://tv/20"),
    occurrence("community", "managed", "co.anilist://anime/30 <=> org.themoviedb://tv/40"),
  ];
  const writes: Array<{ method: string; path: string; record?: string }> = [];
  let releaseCreate = () => {};
  const createGate = new Promise<void>((resolve) => {
    releaseCreate = resolve;
  });

  const state = () => ({
    activeChainFingerprint: String(fingerprintSequence).repeat(64),
    sources: [localSource, remoteSource],
  });

  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces" && method === "GET") return json(route, [workspace]);
    if (url.pathname === "/api/v1/mapping-sources" && method === "GET") return json(route, state());
    if (url.pathname === "/api/v1/mappings" && method === "GET") {
      return json(route, {
        chainFingerprint: state().activeChainFingerprint,
        items: rows,
        nextCursor: null,
      });
    }
    if (url.pathname === "/api/v1/mappings/local" && method === "POST") {
      const body = route.request().postDataJSON() as { record: string };
      writes.push({ method, path: url.pathname, record: body.record });
      await createGate;
      rows = [...rows, occurrence("local", "local-created", body.record)];
      fingerprintSequence += 1;
      return json(route, state(), 201);
    }
    if (url.pathname === "/api/v1/mappings/local/local-created" && method === "PUT") {
      const body = route.request().postDataJSON() as { record: string };
      writes.push({ method, path: url.pathname, record: body.record });
      rows = rows.map((row) =>
        row.statementId === "local-created" ? occurrence("local", "local-updated", body.record) : row,
      );
      fingerprintSequence += 1;
      return json(route, state());
    }
    if (url.pathname === "/api/v1/mappings/local/local-updated" && method === "DELETE") {
      writes.push({ method, path: url.pathname });
      rows = rows.filter((row) => row.statementId !== "local-updated");
      fingerprintSequence += 1;
      return json(route, state());
    }
    return fallback(route);
  });

  await page.goto("/mappings");
  await page.getByRole("button", { name: "Add mapping", exact: true }).click();
  const mappingDialog = page.getByRole("dialog", { name: "Add local mapping" });
  await mappingDialog.getByLabel("Mapping record").fill("co.anilist://anime/50 <=> org.themoviedb://tv/60");
  const createButton = mappingDialog.getByRole("button", { name: "Add mapping", exact: true });
  await createButton.click();
  await expect.poll(() => writes).toHaveLength(1);
  await page.keyboard.press("Escape");
  await expect(mappingDialog).toBeVisible();
  await expect(createButton).toBeDisabled();
  releaseCreate();

  await expect(
    page.getByRole("button", { name: "Edit co.anilist://anime/50 <=> org.themoviedb://tv/60" }),
  ).toBeVisible();
  expect(writes[0]).toEqual({
    method: "POST",
    path: "/api/v1/mappings/local",
    record: "co.anilist://anime/50 <=> org.themoviedb://tv/60",
  });

  await page.getByRole("button", { name: "Edit co.anilist://anime/50 <=> org.themoviedb://tv/60" }).click();
  const editDialog = page.getByRole("dialog", { name: "Edit local mapping" });
  await editDialog.getByLabel("Mapping record").fill("co.anilist://anime/70 <=> org.themoviedb://tv/80");
  await editDialog.getByRole("button", { name: "Save mapping" }).click();
  await expect(
    page.getByRole("button", { name: "Delete co.anilist://anime/70 <=> org.themoviedb://tv/80" }),
  ).toBeVisible();

  await page.getByRole("button", { name: "Delete co.anilist://anime/70 <=> org.themoviedb://tv/80" }).click();
  const removalDialog = page.getByRole("alertdialog", { name: "Delete local mapping?" });
  await removalDialog.getByRole("button", { name: "Delete" }).click();
  await expect(
    page.getByRole("button", { name: "Delete co.anilist://anime/70 <=> org.themoviedb://tv/80" }),
  ).toHaveCount(0);

  expect(writes.map(({ method, path }) => ({ method, path }))).toEqual([
    { method: "POST", path: "/api/v1/mappings/local" },
    { method: "PUT", path: "/api/v1/mappings/local/local-created" },
    { method: "DELETE", path: "/api/v1/mappings/local/local-updated" },
  ]);
  await expect(
    page.getByRole("button", {
      name: "Override co.anilist://anime/30 <=> org.themoviedb://tv/40 locally",
    }),
  ).toBeVisible();
  await expectNoHorizontalOverflow(page);
});

test("keeps source precedence and lifecycle controls in the Sources tab", async ({ page }) => {
  let fingerprintSequence = 1;
  let sources = [localSource, remoteSource, uploadedSource];
  let refreshCount = 0;
  const enabledRequests: boolean[] = [];
  const reorderRequests: string[][] = [];
  let removalRequests = 0;
  let releaseToggle = () => {};
  let releaseRemoval = () => {};
  const toggleGate = new Promise<void>((resolve) => {
    releaseToggle = resolve;
  });
  const removalGate = new Promise<void>((resolve) => {
    releaseRemoval = resolve;
  });

  const state = () => ({
    activeChainFingerprint: String(fingerprintSequence).repeat(64),
    sources,
  });

  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces" && method === "GET") return json(route, [workspace]);
    if (url.pathname === "/api/v1/mapping-sources" && method === "GET") return json(route, state());
    if (url.pathname === `/api/v1/mapping-sources/${remoteSource.id}` && method === "PATCH") {
      const body = route.request().postDataJSON() as { enabled: boolean };
      enabledRequests.push(body.enabled);
      await toggleGate;
      sources = sources.map((source) =>
        source.id === remoteSource.id ? { ...source, enabled: body.enabled } : source,
      );
      fingerprintSequence += 1;
      return json(route, state());
    }
    if (url.pathname === `/api/v1/mapping-sources/${uploadedSource.id}` && method === "DELETE") {
      removalRequests += 1;
      await removalGate;
      sources = sources.filter((source) => source.id !== uploadedSource.id);
      return json(route, state());
    }
    if (url.pathname.endsWith(`/${remoteSource.id}/refresh`) && method === "POST") {
      refreshCount += 1;
      return json(route, state());
    }
    if (url.pathname === "/api/v1/mapping-sources/order" && method === "PUT") {
      const body = route.request().postDataJSON() as { sourceIds: string[] };
      reorderRequests.push(body.sourceIds);
      const byId = new Map(sources.map((source) => [source.id, source]));
      sources = [localSource, ...body.sourceIds.map((sourceId) => byId.get(sourceId) ?? uploadedSource)];
      fingerprintSequence += 1;
      return json(route, state());
    }
    return fallback(route);
  });

  await page.goto("/mappings");
  await page.getByRole("navigation", { name: "Mapping views" }).getByRole("link", { name: "Sources" }).click();
  await expect(page).toHaveURL(/\/mappings\/sources$/);
  await expect(page.getByRole("link", { name: "Sources" })).toHaveAttribute("aria-current", "page");

  const sourceChain = page.getByRole("region", { name: "Source precedence" });
  await expect(sourceChain.getByRole("listitem").nth(0)).toContainText("Local mappings");
  await expect(sourceChain.getByRole("listitem").nth(1)).toContainText("Community mappings");
  await expect(sourceChain.getByRole("listitem").nth(2)).toContainText("Imported mappings");
  await expect(page.getByRole("table")).toHaveCount(0);

  await page.getByRole("button", { name: "Refresh Community mappings" }).click();
  await expect.poll(() => refreshCount).toBe(1);

  const enableSwitch = page.getByRole("switch", { name: "Enable Community mappings" });
  await expect(enableSwitch).toBeChecked();
  await enableSwitch.click();
  await expect.poll(() => enabledRequests).toEqual([false]);
  await expect(page.getByRole("button", { name: "Add source" })).toBeDisabled();
  await expect(page.getByRole("button", { name: "Refresh Community mappings" })).toBeDisabled();
  releaseToggle();
  await expect(enableSwitch).not.toBeChecked();

  await page.getByRole("button", { name: "Manage Community mappings" }).click();
  await page.getByRole("menuitem", { name: "Move down" }).click();
  await expect.poll(() => reorderRequests).toEqual([[uploadedSource.id, remoteSource.id]]);
  await expect(sourceChain.getByRole("listitem").nth(1)).toContainText("Imported mappings");
  await expect(sourceChain.getByRole("listitem").nth(2)).toContainText("Community mappings");

  await page.getByRole("button", { name: "Manage Imported mappings" }).click();
  await page.getByRole("menuitem", { name: "Remove source" }).click();
  const removalDialog = page.getByRole("alertdialog", { name: "Remove mapping source?" });
  await removalDialog.getByRole("button", { name: "Remove" }).click();
  await expect.poll(() => removalRequests).toBe(1);
  await page.keyboard.press("Escape");
  await expect(removalDialog).toBeVisible();
  releaseRemoval();
  await expect(removalDialog).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Add source" })).toBeVisible();
  await expectNoHorizontalOverflow(page);
});
