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
import { rootCatalog } from "./support/catalog.ts";

test("loads and polls a focused run with structured failure evidence", async ({ page }) => {
  const runId = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
  const actionId = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff";
  const requestedRunIds: Array<string | null> = [];
  let activityRequests = 0;
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces" && method === "GET") return json(route, [workspace]);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}` && method === "GET") return json(route, workspace);
    if (url.pathname === "/api/v1/connections" && method === "GET") return json(route, [connection]);
    if (url.pathname === `/api/v1/connections/${connectionId}/sources` && method === "GET") {
      return json(route, [source]);
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/activity` && method === "GET") {
      activityRequests += 1;
      requestedRunIds.push(url.searchParams.get("runId"));
      const terminal = activityRequests > 1;
      return json(route, {
        runs: [focusedActivityRun(runId, actionId, terminal ? "failed" : "running")],
        nextCursor: null,
      });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/activity/actions/${actionId}` && method === "GET") {
      return json(route, {
        action: focusedActivityRun(runId, actionId, "failed").actions[0],
        reconciliationGuidance: "not_required",
        provenance: null,
      });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks/${taskId}` && method === "GET") {
      return json(route, {
        ...task("failed", true),
        kind: "syncAction",
        mode: null,
        stream: null,
        failure: {
          code: "provider_rate_limited",
          message: "Provider rate limit reached",
          retryable: true,
          retryAfter: { seconds: 30, nanoseconds: 0 },
        },
      });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks` && method === "GET") return json(route, []);
    return fallback(route);
  });

  await page.goto(`/w/${workspaceId}/activity?runId=${runId}`);
  await expect.poll(() => requestedRunIds[0]).toBe(runId);
  const focusedRun = page.getByRole("article", { name: `Synchronization run ${runId}` });
  await expect(focusedRun).toHaveAttribute("aria-current", "true");
  await expect.poll(() => activityRequests).toBeGreaterThan(1);
  await expect(focusedRun.getByText("failed", { exact: true }).first()).toBeVisible();
  await page.getByLabel(`Inspect Progress action ${actionId}`).click();
  await expect(page.getByRole("alert")).toContainText("Provider rate limit reached");
  await expect(page.getByRole("alert")).toContainText("provider_rate_limited");
  await expect(page.getByRole("alert")).toContainText("Retryable after 30s");
  await expectNoHorizontalOverflow(page);
});

test("workspace task cancellation cannot mutate a later workspace", async ({ page }) => {
  const secondWorkspaceId = "11111111-1111-1111-1111-222222222222";
  const secondTaskId = "44444444-4444-4444-4444-555555555555";
  const secondWorkspace = { ...workspace, id: secondWorkspaceId, name: "Archive workspace" };
  let staleWorkspaceDirectoryRequested = false;
  let deferWorkspaceDirectory = true;
  let workspaceDirectoryRequests = 0;
  let releaseWorkspaceDirectory: () => void = () => {};
  const workspaceDirectoryGate = new Promise<void>((resolve) => {
    releaseWorkspaceDirectory = resolve;
  });
  let staleCancellationRequested = false;
  let releaseStaleCancellation: () => void = () => {};
  const staleCancellationGate = new Promise<void>((resolve) => {
    releaseStaleCancellation = resolve;
  });
  let secondWorkspaceTaskRequests = 0;
  let secondTaskCancelled = false;
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces" && method === "GET") {
      workspaceDirectoryRequests += 1;
      if (deferWorkspaceDirectory) {
        deferWorkspaceDirectory = false;
        staleWorkspaceDirectoryRequested = true;
        await workspaceDirectoryGate;
        return json(route, [workspace]);
      }
      return json(route, [workspace, secondWorkspace]);
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}`) return json(route, workspace);
    if (url.pathname === `/api/v1/workspaces/${secondWorkspaceId}`) return json(route, secondWorkspace);
    if (url.pathname === "/api/v1/connections") return json(route, [connection]);
    if (url.pathname === `/api/v1/connections/${connectionId}/sources`) return json(route, [source]);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks` && method === "GET") {
      return json(route, [task("running", false)]);
    }
    if (url.pathname === `/api/v1/workspaces/${secondWorkspaceId}/tasks` && method === "GET") {
      secondWorkspaceTaskRequests += 1;
      return json(route, [
        { ...task(secondTaskCancelled ? "cancelled" : "running", secondTaskCancelled), id: secondTaskId },
      ]);
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks/${taskId}/cancellation` && method === "POST") {
      staleCancellationRequested = true;
      await staleCancellationGate;
      return json(route, task("cancelled", true));
    }
    if (
      url.pathname === `/api/v1/workspaces/${secondWorkspaceId}/tasks/${secondTaskId}/cancellation` &&
      method === "POST"
    ) {
      secondTaskCancelled = true;
      return json(route, { ...task("cancelled", true), id: secondTaskId });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/sources/${sourceId}/catalog`) {
      return json(route, rootCatalog());
    }
    if (url.pathname === `/api/v1/workspaces/${secondWorkspaceId}/sources/${sourceId}/catalog`) {
      return json(route, rootCatalog());
    }
    return fallback(route);
  });

  await page.goto(`/w/${workspaceId}/catalog`);
  await expect.poll(() => staleWorkspaceDirectoryRequested).toBe(true);
  await page.evaluate(() => window.dispatchEvent(new Event("trakkin:workspaces-changed")));
  await expect.poll(() => workspaceDirectoryRequests).toBeGreaterThanOrEqual(2);
  const staleWorkspaceDirectoryResponse = page.waitForResponse(
    (response) => new URL(response.url()).pathname === "/api/v1/workspaces",
  );
  releaseWorkspaceDirectory();
  await staleWorkspaceDirectoryResponse;
  await page.evaluate(
    () => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))),
  );
  const workspaceSelector = page.getByLabel("Active workspace").filter({ visible: true });
  await workspaceSelector.click();
  await expect(page.getByRole("option", { name: secondWorkspace.name })).toBeVisible();
  await page.keyboard.press("Escape");

  await page
    .getByRole("button", { name: /Tasks, 1 active task/ })
    .filter({ visible: true })
    .click();
  await page.getByRole("button", { name: "Cancel catalog read" }).click();
  await expect.poll(() => staleCancellationRequested).toBe(true);
  await page.keyboard.press("Escape");
  await workspaceSelector.click();
  await page.getByRole("option", { name: secondWorkspace.name }).click();
  await expect(page).toHaveURL(new RegExp(`/w/${secondWorkspaceId}/catalog$`));
  await expect.poll(() => secondWorkspaceTaskRequests).toBeGreaterThan(0);
  await page.keyboard.press("Escape");
  await page
    .getByRole("button", { name: /Tasks, 1 active task/ })
    .filter({ visible: true })
    .click();
  await expect(page.getByRole("button", { name: "Cancel catalog read" })).toBeEnabled();
  const taskRequestsBeforeStaleCompletion = secondWorkspaceTaskRequests;
  const staleCancellationResponse = page.waitForResponse(
    (response) => new URL(response.url()).pathname === `/api/v1/workspaces/${workspaceId}/tasks/${taskId}/cancellation`,
  );
  releaseStaleCancellation();
  await staleCancellationResponse;
  await page.evaluate(
    () => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))),
  );
  expect(secondWorkspaceTaskRequests).toBe(taskRequestsBeforeStaleCompletion);
  await expect(page.getByRole("button", { name: "Cancel catalog read" })).toBeEnabled();
  await page.getByRole("button", { name: "Cancel catalog read" }).click();
  await expect.poll(() => secondTaskCancelled).toBe(true);
  await expect(page.getByText("Cancelled", { exact: true })).toBeVisible();
  await expect(page.getByRole("alert")).not.toBeVisible();
  await expectNoHorizontalOverflow(page);
});

function focusedActivityRun(runId: string, actionId: string, status: "running" | "failed") {
  const terminal = status === "failed";
  return {
    runId,
    workspaceId,
    previewId: "88888888-8888-4888-8888-888888888888",
    status,
    actions: [
      {
        actionId,
        runId,
        workspaceId,
        actionHash: `sha256:${"b".repeat(64)}`,
        receiverSourceId: sourceId,
        semanticField: "trakkin.state:progress",
        status,
        writeTask: {
          taskId,
          status,
          createdAt: timestamp,
          startedAt: timestamp,
          finishedAt: terminal ? timestamp : null,
          updatedAt: timestamp,
        },
        reconciliationTask: null,
        createdAt: timestamp,
        startedAt: timestamp,
        finishedAt: terminal ? timestamp : null,
        updatedAt: timestamp,
      },
    ],
    createdAt: timestamp,
    startedAt: timestamp,
    finishedAt: terminal ? timestamp : null,
    updatedAt: timestamp,
  };
}
