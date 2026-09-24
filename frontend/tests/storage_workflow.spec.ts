import { expect, type Page, test } from "@playwright/test";
import { fallback, json, mockApi } from "./support/api.ts";

const themePaletteKey = "dev.trakkin:root:theme-palette";

test("discards local storage values with a different version", async ({ page }) => {
  await page.addInitScript((key) => {
    localStorage.setItem(key, JSON.stringify({ version: 0, value: { hue: 320, lightness: 0.2, chroma: 0.04 } }));
  }, themePaletteKey);
  await mockUiApi(page);

  await page.goto("/ui");
  await expect
    .poll(() =>
      page.evaluate((key) => JSON.parse(localStorage.getItem(key) ?? "null")?.version ?? null, themePaletteKey),
    )
    .toBe(1);
  const stored = await page.evaluate((key) => localStorage.getItem(key), themePaletteKey);
  expect(JSON.parse(stored ?? "null")).toEqual({
    version: 1,
    value: { hue: 164, lightness: 0.12, chroma: 0.007 },
  });
});

test("isolates local storage values by application and frontend base path", async ({ page }) => {
  const unrelatedApplicationKey = "com.example:root:theme-palette";
  const otherBasePathKey = "dev.trakkin:other-base:theme-palette";
  const unrelatedValue = JSON.stringify({ version: 1, value: { hue: 20, lightness: 0.2, chroma: 0.03 } });
  await page.addInitScript(
    ({ applicationKey, basePathKey, value }) => {
      localStorage.setItem(applicationKey, value);
      localStorage.setItem(basePathKey, value);
    },
    { applicationKey: unrelatedApplicationKey, basePathKey: otherBasePathKey, value: unrelatedValue },
  );
  await mockUiApi(page);

  await page.goto("/ui");
  await expect.poll(() => page.evaluate((key) => localStorage.getItem(key), themePaletteKey)).not.toBeNull();
  await expect(page.evaluate((key) => localStorage.getItem(key), unrelatedApplicationKey)).resolves.toBe(
    unrelatedValue,
  );
  await expect(page.evaluate((key) => localStorage.getItem(key), otherBasePathKey)).resolves.toBe(unrelatedValue);
});

async function mockUiApi(page: Page): Promise<void> {
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === "/api/v1/workspaces" && route.request().method() === "GET") return json(route, []);
    return fallback(route);
  });
}
