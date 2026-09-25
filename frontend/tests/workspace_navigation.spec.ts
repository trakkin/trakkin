import { expect, test } from "@playwright/test";
import {
  apiError,
  expectNoHorizontalOverflow,
  fallback,
  json,
  mockApi,
  workspace,
  workspaceId,
} from "./support/api.ts";

test("shared controls use consistent dimensions and visible keyboard focus", async ({ page }) => {
  await mockApi(page, async (route) => {
    if (new URL(route.request().url()).pathname === "/api/v1/workspaces") return json(route, [workspace]);
    return fallback(route);
  });

  await page.goto("/ui");
  const buttons = page.getByRole("button", { name: "primary", exact: true });
  await expect(buttons).toHaveCount(3);
  for (const [index, size] of ["sm", "md", "lg"].entries()) {
    const input = page.getByRole("textbox", { name: `Input ${size}`, exact: true });
    const icon = page.getByRole("button", { name: new RegExp(`^Add item.*${size}$`) });
    const height = await input.evaluate((element) => element.getBoundingClientRect().height);
    expect(await buttons.nth(index).evaluate((element) => element.getBoundingClientRect().height)).toBe(height);
    expect(await icon.evaluate((element) => element.getBoundingClientRect().height)).toBe(height);
    expect(await icon.evaluate((element) => element.getBoundingClientRect().width)).toBe(height);
    await expect(buttons.nth(index)).toHaveAttribute("type", "button");
    await expect(icon).toHaveAttribute("type", "button");
  }
  const inputHeight = await page
    .getByRole("textbox", { name: "Input md", exact: true })
    .evaluate((element) => element.getBoundingClientRect().height);
  expect(
    await page
      .getByRole("button", { name: "Connection status", exact: true })
      .evaluate((element) => element.getBoundingClientRect().height),
  ).toBe(inputHeight);

  await buttons.nth(0).focus();
  await page.keyboard.press("Tab");
  await expect(buttons.nth(1)).toBeFocused();
  expect(await buttons.nth(1).evaluate((element) => getComputedStyle(element).boxShadow)).not.toBe("none");
  await expectNoHorizontalOverflow(page);
});

test("global pages preserve the selected workspace", async ({ page }) => {
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === "/api/v1/connections") return json(route, []);
    if (["/api/v1/provider-sources", "/api/v1/providers"].includes(url.pathname)) return json(route, []);
    return fallback(route);
  });

  await page.goto(`/connections?workspace=${workspaceId}`);
  const workspaceSelector = page.getByLabel("Active workspace").filter({ visible: true });
  await expect(workspaceSelector).toHaveText(workspace.name);
  if ((page.viewportSize()?.width ?? 0) < 1024) {
    await page.getByRole("button", { name: "Open navigation" }).click();
    await page.getByRole("menuitem", { name: "Providers" }).click();
  } else {
    await page.getByRole("link", { name: "Providers" }).click();
  }

  await expect(page).toHaveURL(/\/providers$/);
  await expect(workspaceSelector).toHaveText(workspace.name);
});

test("root workspace lookup cannot replace later navigation", async ({ page }) => {
  let workspaceRequests = 0;
  let releaseLookup: () => void = () => {};
  let lookupGate = new Promise<void>((resolve) => {
    releaseLookup = resolve;
  });
  const lookup: { fails: boolean } = { fails: false };
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === "/api/v1/workspaces") {
      const requestNumber = ++workspaceRequests;
      await lookupGate;
      if (lookup.fails && requestNumber <= 2) {
        return apiError(route, 503, "workspaces_unavailable", "Root workspace lookup failed");
      }
      return json(route, [workspace]);
    }
    if (url.pathname === "/api/v1/connections") return json(route, []);
    return fallback(route);
  });

  async function navigateToConnections(): Promise<void> {
    if ((page.viewportSize()?.width ?? 0) < 1024) {
      await page.getByRole("button", { name: "Open navigation" }).click();
      await page.getByRole("menuitem", { name: "Connections" }).click();
    } else {
      await page.getByRole("link", { name: "Connections" }).click();
    }
    await expect(page).toHaveURL(/\/connections$/);
    await expect(page.getByRole("heading", { name: "Connections" })).toBeVisible();
  }

  await page.goto("/");
  await expect.poll(() => workspaceRequests).toBe(2);
  await navigateToConnections();
  releaseLookup();
  await expect(page).toHaveURL(/\/connections$/);

  lookup.fails = true;
  workspaceRequests = 0;
  lookupGate = new Promise<void>((resolve) => {
    releaseLookup = resolve;
  });
  await page.goto("/");
  await expect.poll(() => workspaceRequests).toBe(2);
  await navigateToConnections();
  releaseLookup();
  await expect(page.getByRole("alert").filter({ hasText: "Root workspace lookup failed" })).toHaveCount(0);
  await expect(page).toHaveURL(/\/connections$/);
  await expectNoHorizontalOverflow(page);
});

test("workspace creation preserves later navigation and retry state", async ({ page }) => {
  const deferredWorkspaceId = "11111111-1111-1111-1111-222222222222";
  let createdName = "";
  let workspaceDirectoryRequests = 0;
  let deferredCreateRequested = false;
  let releaseDeferredCreate: () => void = () => {};
  const deferredCreateGate = new Promise<void>((resolve) => {
    releaseDeferredCreate = resolve;
  });
  const mockState: { failWorkspace: boolean } = { failWorkspace: false };
  let releaseCatalog: () => void = () => {};
  const catalogGate = new Promise<void>((resolve) => {
    releaseCatalog = resolve;
  });
  const createdWorkspace = { ...workspace, name: "Household media", sources: [] };
  const deferredWorkspace = { ...createdWorkspace, id: deferredWorkspaceId, name: "Deferred workspace" };
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces" && method === "GET") {
      workspaceDirectoryRequests += 1;
      return json(route, [createdWorkspace]);
    }
    if (url.pathname === "/api/v1/workspaces" && method === "POST") {
      createdName = (route.request().postDataJSON() as { name: string }).name;
      if (createdName === deferredWorkspace.name) {
        deferredCreateRequested = true;
        await deferredCreateGate;
        return json(route, deferredWorkspace, 201);
      }
      return json(route, createdWorkspace, 201);
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}`) {
      if (mockState.failWorkspace) return apiError(route, 503, "catalog_unavailable", "Catalog data is unavailable");
      await catalogGate;
      return json(route, createdWorkspace);
    }
    if (url.pathname === "/api/v1/connections") return json(route, []);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks`) return json(route, []);
    return fallback(route);
  });

  await page.goto("/workspaces/new");
  await expect(page.getByRole("button", { name: "Back to Catalog" })).toBeVisible();
  await page.getByLabel("Workspace name").fill(deferredWorkspace.name);
  await page.getByRole("main").getByRole("button", { name: "Create workspace" }).click();
  await expect.poll(() => deferredCreateRequested).toBe(true);
  await page.getByRole("button", { name: "Back to Catalog" }).click();
  await expect(page).toHaveURL(new RegExp(`/w/${workspaceId}/catalog$`));
  await expect(page.getByRole("status")).toContainText("Loading catalog");
  const directoryRequestsAfterBack = workspaceDirectoryRequests;
  const deferredCreateResponse = page.waitForResponse(
    (response) => response.request().method() === "POST" && new URL(response.url()).pathname === "/api/v1/workspaces",
  );
  releaseDeferredCreate();
  await deferredCreateResponse;
  await page.evaluate(
    () => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))),
  );
  await expect(page).toHaveURL(new RegExp(`/w/${workspaceId}/catalog$`));
  expect(workspaceDirectoryRequests).toBe(directoryRequestsAfterBack);
  releaseCatalog();
  await expect(page.getByRole("heading", { name: "No attached sources" })).toBeVisible();

  await page.goto("/workspaces/new");
  await page.getByLabel("Workspace name").fill("Household media");
  await page.getByRole("main").getByRole("button", { name: "Create workspace" }).click();
  await expect(page).toHaveURL(new RegExp(`/w/${workspaceId}/catalog$`));
  await expect(page.getByRole("heading", { name: "No attached sources" })).toBeVisible();
  expect(createdName).toBe("Household media");

  mockState.failWorkspace = true;
  await page.reload();
  const catalogError = page.getByRole("alert").filter({ hasText: "Catalog data is unavailable" });
  await expect(catalogError).toBeVisible();
  mockState.failWorkspace = false;
  await catalogError.getByRole("button", { name: "Retry" }).click();
  await expect(page.getByRole("heading", { name: "No attached sources" })).toBeVisible();
  await expectNoHorizontalOverflow(page);
});
