import { expect, test } from "@playwright/test";
import {
  connection,
  connectionId,
  expectNoHorizontalOverflow,
  fallback,
  json,
  mockApi,
  source,
  sourceId,
  task,
  taskId,
  timestamp,
  workspace,
  workspaceId,
} from "./support/api.ts";
import { catalogEntry, catalogPage, childCatalog, rootCatalog } from "./support/catalog.ts";

test("browses published hierarchy, artwork, and refresh tasks", async ({ page }) => {
  const secondSourceId = "33333333-3333-3333-3333-444444444444";
  const secondSource = { ...source, id: secondSourceId, displayName: "Fixture Archive" };
  const workspaceWithSources = {
    ...workspace,
    sources: [...workspace.sources, { sourceId: secondSourceId, attachedAt: timestamp }],
  };
  let catalogReadCount = 0;
  const catalogMockModes = new Set<"deferRead" | "includeSecondSource">();
  let secondSourceCatalogRequests = 0;
  let releaseCatalogRead: () => void = () => {};
  const catalogReadGate = new Promise<void>((resolve) => {
    releaseCatalogRead = resolve;
  });
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces" && method === "GET") return json(route, [workspace]);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}`) {
      return json(route, catalogMockModes.has("includeSecondSource") ? workspaceWithSources : workspace);
    }
    if (url.pathname === "/api/v1/connections") return json(route, [connection]);
    if (url.pathname === `/api/v1/connections/${connectionId}/sources`) {
      return json(route, catalogMockModes.has("includeSecondSource") ? [source, secondSource] : [source]);
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks`) return json(route, []);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/sources/${sourceId}/catalog`) {
      return json(route, url.searchParams.has("parentNamespace") ? childCatalog() : rootCatalog());
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/sources/${secondSourceId}/catalog`) {
      secondSourceCatalogRequests += 1;
      return json(
        route,
        url.searchParams.has("parentNamespace")
          ? catalogPage([catalogEntry("Second light", "media/2", "second", false, false)])
          : catalogPage([catalogEntry("Southstar", "collection/southstar", "southstar", true, false)]),
      );
    }
    if (url.pathname.endsWith("/assets/fixture/6d656469612f6e6f72746873746172/fixture/706f73746572")) {
      return route.fulfill({
        status: 200,
        contentType: "image/png",
        body: Buffer.from(
          "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=",
          "base64",
        ),
      });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/sources/${sourceId}/reads` && method === "POST") {
      catalogReadCount += 1;
      return json(route, task("queued", false), 202);
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks/${taskId}`) {
      if (catalogMockModes.has("deferRead")) await catalogReadGate;
      return json(route, task("succeeded", true));
    }
    return fallback(route);
  });

  await page.goto(`/w/${workspaceId}/catalog`);
  await expect(page.getByRole("button", { name: "Inspect Northstar" })).toBeVisible();
  await expect(page.locator('img[src*="/assets/"]').first()).toBeVisible();
  await page.getByRole("button", { name: "Browse Northstar" }).click();
  await expect(page.getByRole("button", { name: "Inspect First light" })).toBeVisible();
  await page.getByRole("button", { name: "Scan Fixture Catalog" }).click();
  await expect.poll(() => catalogReadCount).toBe(1);
  await expect(page.getByRole("button", { name: "Scan Fixture Catalog" })).toBeEnabled();
  await expect(page.getByRole("button", { name: "Inspect First light" })).toBeVisible();
  await expectNoHorizontalOverflow(page);

  catalogMockModes.add("includeSecondSource");
  await page.getByLabel("Refresh Catalog").click();
  await expect(page.getByRole("tab", { name: "Fixture Archive" })).toBeVisible();
  catalogMockModes.add("deferRead");
  await page.getByRole("button", { name: "Scan Fixture Catalog" }).click();
  await expect.poll(() => catalogReadCount).toBe(2);
  await page.getByRole("tab", { name: "Fixture Archive" }).click();
  await expect(page.getByRole("button", { name: "Scan Fixture Archive" })).toBeEnabled();
  await page.getByRole("button", { name: "Browse Southstar" }).click();
  await expect(page.getByRole("button", { name: "Inspect Second light" })).toBeVisible();
  const secondSourceRequestsBeforeRelease = secondSourceCatalogRequests;
  releaseCatalogRead();
  await page.evaluate(() => new Promise<void>((resolve) => window.setTimeout(resolve, 350)));
  expect(secondSourceCatalogRequests).toBe(secondSourceRequestsBeforeRelease);
  await expect(page.getByRole("button", { name: "Inspect Second light" })).toBeVisible();
  await expectNoHorizontalOverflow(page);
});

test("catalog task polling stops after navigation", async ({ page }) => {
  let taskRequests = 0;
  let releaseTask: () => void = () => {};
  const taskGate = new Promise<void>((resolve) => {
    releaseTask = resolve;
  });
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces" && method === "GET") return json(route, [workspace]);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}`) return json(route, workspace);
    if (url.pathname === "/api/v1/connections") return json(route, [connection]);
    if (url.pathname === `/api/v1/connections/${connectionId}/sources`) return json(route, [source]);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks`) return json(route, []);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/sources/${sourceId}/catalog`) {
      return json(route, rootCatalog());
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/sources/${sourceId}/reads` && method === "POST") {
      return json(route, task("queued", false), 202);
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks/${taskId}`) {
      taskRequests += 1;
      await taskGate;
      return json(route, task("running", false));
    }
    return fallback(route);
  });

  await page.goto(`/w/${workspaceId}/catalog`);
  await page.getByRole("button", { name: "Scan Fixture Catalog" }).click();
  await expect.poll(() => taskRequests).toBe(1);
  if ((page.viewportSize()?.width ?? 0) < 1024) {
    await page.getByRole("button", { name: "Open navigation" }).click();
    await page.getByRole("menuitem", { name: "Connections" }).click();
  } else {
    await page.getByRole("link", { name: "Connections" }).click();
  }
  await expect(page).toHaveURL(new RegExp(`/connections\\?workspace=${workspaceId}$`));
  releaseTask();
  await page.evaluate(() => new Promise<void>((resolve) => window.setTimeout(resolve, 350)));
  expect(taskRequests).toBe(1);
  await expectNoHorizontalOverflow(page);
});

test("catalog pagination cannot replace a newer normalized search", async ({ page }) => {
  let resetCount = 0;
  let readCount = 0;
  let normalizedSearch = "";
  let staleCatalogPageRequested = false;
  let releaseStaleCatalogPage: () => void = () => {};
  const staleCatalogPageGate = new Promise<void>((resolve) => {
    releaseStaleCatalogPage = resolve;
  });
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces" && method === "GET") return json(route, [workspace]);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}`) return json(route, workspace);
    if (url.pathname === "/api/v1/connections") return json(route, [connection]);
    if (url.pathname === `/api/v1/connections/${connectionId}/sources`) return json(route, [source]);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks`) return json(route, []);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/sources/${sourceId}/catalog`) {
      normalizedSearch = url.searchParams.get("search") ?? "";
      if (normalizedSearch === "quiet signal") {
        return json(route, catalogPage([catalogEntry("Quiet signal", "media/quiet-signal", "quiet", false, false)]));
      }
      if (url.searchParams.has("cursor")) {
        staleCatalogPageRequested = true;
        await staleCatalogPageGate;
        return json(route, catalogPage([catalogEntry("Second entry", "movie/2", "second", false, false)]));
      }
      return json(route, {
        ...catalogPage([catalogEntry("First entry", "movie/1", "first", false, false)]),
        nextCursor: "next-page",
      });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/sources/${sourceId}/resets` && method === "POST") {
      resetCount += 1;
      return json(route, { ...task("succeeded", true), kind: "source_reset", mode: null }, 202);
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/sources/${sourceId}/reads` && method === "POST") {
      readCount += 1;
      return json(route, task("succeeded", true), 202);
    }
    return fallback(route);
  });

  await page.goto(`/w/${workspaceId}/catalog`);
  await page.getByRole("button", { name: "Load more" }).click();
  await expect.poll(() => staleCatalogPageRequested).toBe(true);
  await page.getByLabel("Search catalog").fill("  Quiet   Signal  ");
  await expect(page.getByRole("button", { name: "Inspect Quiet signal" })).toBeVisible();
  releaseStaleCatalogPage();
  await expect(page.getByRole("button", { name: "Inspect Second entry" })).not.toBeVisible();
  await expect(page.getByRole("button", { name: "Inspect Quiet signal" })).toBeVisible();
  await page.getByLabel("Search catalog").fill("");
  await expect(page.getByRole("button", { name: "Inspect First entry" })).toBeVisible();

  await page.getByRole("button", { name: "Reset Fixture Catalog" }).click();
  await page.getByRole("button", { name: "Reset and refetch" }).click();
  await expect.poll(() => ({ resetCount, readCount })).toEqual({ resetCount: 1, readCount: 1 });
  await expect(page.getByRole("button", { name: "Inspect First entry" })).toBeVisible();
  await expectNoHorizontalOverflow(page);
});
