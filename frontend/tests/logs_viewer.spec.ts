import { expect, test } from "@playwright/test";
import { expectNoHorizontalOverflow, fallback, json, mockApi, workspace } from "./support/api.ts";

test("filters dense stdout-style runtime logs", async ({ page }) => {
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === "/api/v1/logs/stream") {
      return route.fulfill({
        status: 200,
        contentType: "text/event-stream",
        body: `event: log\nid: cursor-live\ndata: ${JSON.stringify({
          cursor: "cursor-live",
          timestamp: "2030-01-02T03:04:07.123Z",
          level: "DEBUG",
          target: "trakkin::runtime",
          fields: { message: "Live event received" },
          spans: [],
        })}\n\n`,
      });
    }
    if (url.pathname === "/api/v1/logs") {
      if (url.searchParams.get("before") === "cursor-info") {
        return json(route, {
          entries: [
            {
              cursor: "cursor-starting",
              timestamp: "2030-01-02T03:04:04.123Z",
              level: "INFO",
              target: "trakkin",
              fields: { message: "Application starting" },
              spans: [],
            },
          ],
          hasMoreBefore: false,
          hasMoreAfter: true,
        });
      }
      return json(route, {
        entries: [
          {
            cursor: "cursor-info",
            timestamp: "2030-01-02T03:04:05.123Z",
            level: "INFO",
            target: "trakkin",
            fields: { message: "HTTP server started" },
            spans: [],
          },
          {
            cursor: "cursor-error",
            timestamp: "2030-01-02T03:04:06.123Z",
            level: "ERROR",
            target: "trakkin::database",
            fields: {
              message: "Database connection unavailable",
              "database.mode": "embedded",
              attempts: 3,
            },
            span: { name: "database.start", "process.instance.id": "instance-1" },
            spans: [{ name: "http.request", "request.id": "request-1" }],
          },
        ],
        hasMoreBefore: true,
        hasMoreAfter: false,
      });
    }
    return fallback(route);
  });

  await page.goto("/logs");
  await expect(page.getByRole("heading", { name: "Logs" })).toBeVisible();
  await expect(page.getByText("Database connection unavailable", { exact: true })).toBeVisible();
  await expect(page.getByText("HTTP server started", { exact: true })).toBeVisible();
  await expect(page.getByText("Live event received", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Load older logs" }).click();
  await expect(page.getByText("Application starting", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: /Inspect log:/ })).toHaveCount(0);
  const errorLog = page.getByRole("article", { name: "ERROR log: Database connection unavailable" });
  const errorLogBox = await errorLog.evaluate((element) => ({
    height: element.getBoundingClientRect().height,
    scrollHeight: element.scrollHeight,
    whiteSpace: getComputedStyle(element).whiteSpace,
  }));
  expect(errorLogBox.height).toBeLessThanOrEqual(32);
  expect(errorLogBox.scrollHeight).toBeLessThanOrEqual(errorLogBox.height);
  expect(errorLogBox.whiteSpace).toBe("nowrap");
  await expect(errorLog).toContainText("2030-01-02 03:04:06.123Z");
  await expect(errorLog).toContainText("trakkin::database:");
  await expect(errorLog).toContainText("database.mode=embedded");
  await expect(errorLog).toContainText("attempts=3");
  await expect(errorLog).toContainText("span.name=database.start");
  await expect(errorLog).toContainText("span.process.instance.id=instance-1");
  await expect(errorLog).toContainText('spans=[{"name":"http.request","request.id":"request-1"}]');

  await page.getByRole("button", { name: "Log level filter" }).click();
  await page.getByRole("option", { name: "Error" }).click();
  await expect(page.getByText("HTTP server started", { exact: true })).not.toBeVisible();
  await expect(errorLog).toBeVisible();

  await page.getByRole("button", { name: "Log level filter" }).click();
  await page.getByRole("option", { name: "All levels" }).click();
  await page.getByRole("textbox", { name: "Search logs" }).fill("server started");
  await expect(page.getByText("Database connection unavailable", { exact: true })).not.toBeVisible();
  await expect(page.getByText("HTTP server started", { exact: true })).toBeVisible();
  await page.getByRole("textbox", { name: "Search logs" }).fill("no-matching-log-entry");
  await expect(page.getByRole("heading", { name: "No matching logs" })).toBeVisible();
  await page.getByRole("button", { name: "Clear filters" }).click();
  await expect(page.getByRole("textbox", { name: "Search logs" })).toHaveValue("");
  await expect(page.getByText("HTTP server started", { exact: true })).toBeVisible();
  await expect(errorLog).toBeVisible();
  await expectNoHorizontalOverflow(page);
});
