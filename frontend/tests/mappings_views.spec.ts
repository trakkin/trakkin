import { expect, test } from "@playwright/test";
import type { MappingResult, MappingSource } from "../src/lib/api/mappings.ts";
import { expectNoHorizontalOverflow, fallback, json, localMappingSource, mockApi, workspace } from "./support/api.ts";

const fingerprint = "e".repeat(64);
const original = "com.imdb://title/tt0133093 <=> org.themoviedb://movie/603";
const corrected = "com.imdb://title/tt0133093 <=> org.themoviedb://movie/604";
const claim = (id: string, statement: string, sources = ["community"]): MappingResult => ({
  claimId: id,
  canonicalStatement: statement,
  canonicalRecord: `#@note Reviewed\n${statement}\n`,
  operator: "<=>",
  metadata: ["#@note Reviewed"],
  rejected: false,
  sources,
  status: "resolved",
  diagnostics: [],
});
const state = (items: MappingResult[], revision = fingerprint) => ({
  fingerprint: revision,
  records: items.length,
  active: items.filter((item) => !item.rejected).length,
  rejected: items.filter((item) => item.rejected).length,
  resolved: items.filter((item) => item.status === "resolved").length,
  unresolved: 0,
  conflicting: 0,
  units: items.filter((item) => item.status === "resolved").length * 2,
  metadataSnapshot: null,
});

test("sources are user managed and Local exists without an official placeholder", async ({ page }, testInfo) => {
  let sources: MappingSource[] = [localMappingSource];
  const longBranch = `review/${"changes".repeat(15)}`;
  let revision = fingerprint;
  let stateReads = 0;
  await mockApi(page, async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (path === "/api/v1/workspaces") return json(route, [workspace]);
    if (path === "/api/v1/mappings/state") {
      stateReads += 1;
      return json(route, state([], revision));
    }
    if (path === "/api/v1/mappings/sources") {
      if (request.method() === "GET") return json(route, { fingerprint: revision, sources });
      const body = request.postDataJSON();
      expect(body).toEqual({
        expectedFingerprint: revision,
        displayName: "My mirror",
        url: "https://example.com/mappings.git",
        branch: "review/community",
        pollIntervalSeconds: 3600,
        enabled: true,
      });
      sources = [
        ...sources,
        {
          ...localMappingSource,
          id: "mirror",
          displayName: body.displayName,
          kind: "upstream",
          url: body.url,
          branch: body.branch,
          repositoryPath: null,
          pollIntervalSeconds: body.pollIntervalSeconds,
          cachedRecords: 3,
        },
      ];
      revision = "created";
      return json(route, state([], revision));
    }
    if (path === "/api/v1/mappings/sources/mirror") {
      expect(request.postDataJSON().expectedFingerprint).toBe(revision);
      if (request.method() === "DELETE") sources = [localMappingSource];
      else {
        const body = request.postDataJSON();
        expect(body.branch).toBe(body.displayName === "My mirror" ? "review/community" : longBranch);
        sources = sources.map((source) =>
          source.id === "mirror"
            ? {
                ...source,
                displayName: body.displayName,
                url: body.url,
                branch: body.branch,
                pollIntervalSeconds: body.pollIntervalSeconds,
                enabled: body.enabled,
              }
            : source,
        );
      }
      revision += "-updated";
      return json(route, state([], revision));
    }
    if (path === "/api/v1/mappings/sources/mirror/refresh") {
      expect(request.postDataJSON()).toEqual({ expectedFingerprint: revision });
      revision += "-refresh";
      return json(route, state([], revision));
    }
    if (path === "/api/v1/mappings/sources/local/refresh") {
      expect(request.postDataJSON()).toEqual({ expectedFingerprint: revision });
      sources = sources.map((source) => (source.id === "local" ? { ...source, revision: "d".repeat(40) } : source));
      revision = "reloaded";
      return json(route, state([], revision));
    }
    return fallback(route);
  });
  await page.goto("/mappings/sources");
  const list = page.getByRole("list", { name: "Mapping sources" });
  await expect(list.getByRole("heading", { name: "Local", exact: true })).toBeVisible();
  await expect(list.getByRole("listitem")).toHaveCount(1);
  await expect(page.getByRole("button", { name: "Remove Local", exact: true })).toHaveCount(0);
  await expect(list).toContainText(localMappingSource.repositoryPath);
  await expect(page.getByRole("button", { name: /Export/ })).toHaveCount(0);
  await page.getByRole("button", { name: "Reload Local repository", exact: true }).click();
  await expect(page.getByRole("status")).toHaveText("Local repository reloaded");
  await expect(list).toContainText(`Commit ${"d".repeat(12)}`);
  const add = page.getByRole("button", { name: "Add mapping source", exact: true });
  await add.click();
  await page.getByLabel("Name", { exact: true }).fill("My mirror");
  await page.getByLabel("Git repository URL").fill("https://example.com/mappings.git");
  await expect(page.getByLabel("Branch", { exact: true })).toHaveValue("main");
  await page.getByLabel("Branch", { exact: true }).fill("review/community");
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(add).toBeFocused();
  const enabled = page.getByRole("switch", { name: "Enable My mirror", exact: true });
  await enabled.click();
  await expect(enabled).not.toBeChecked();
  await expect(page.getByRole("button", { name: "Refresh My mirror", exact: true })).toBeDisabled();
  await enabled.click();
  await expect(enabled).toBeChecked();
  await page.getByRole("button", { name: "Edit My mirror", exact: true }).click();
  await expect(page.getByLabel("Branch", { exact: true })).toHaveValue("review/community");
  await page.getByLabel("Branch", { exact: true }).fill(longBranch);
  await page.getByLabel("Name", { exact: true }).fill("Updated mirror");
  await page.getByLabel("Update interval (minutes)").fill("15");
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(list).toContainText("Every 15 min");
  await page.getByRole("button", { name: "Refresh Updated mirror", exact: true }).click();
  await expect(page.getByRole("status")).toHaveText("Fetch scheduled for Updated mirror");
  await expect(list).toContainText(longBranch);
  await expectNoHorizontalOverflow(page);
  await page.screenshot({ path: `test-results/mapping-sources-${testInfo.project.name}.png`, fullPage: true });
  await page.getByRole("button", { name: "Remove Updated mirror", exact: true }).click();
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await page.getByRole("button", { name: "Remove Updated mirror", exact: true }).click();
  await page.getByRole("button", { name: "Remove", exact: true }).click();
  await expect(list.getByRole("listitem")).toHaveCount(1);
  expect(stateReads).toBe(0);
});

test("source errors keep the draft and stale source writes can be retried explicitly", async ({ page }) => {
  let revision = fingerprint;
  let writes = 0;
  await mockApi(page, async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (path === "/api/v1/workspaces") return json(route, [workspace]);
    if (path === "/api/v1/mappings/state") return json(route, state([], revision));
    if (path === "/api/v1/mappings/sources" && request.method() === "GET")
      return json(route, { fingerprint: revision, sources: [localMappingSource] });
    if (path === "/api/v1/mappings/sources" && request.method() === "POST") {
      writes += 1;
      expect(request.postDataJSON().expectedFingerprint).toBe(writes === 1 ? fingerprint : "fresh");
      expect(request.postDataJSON().branch).toBe("review/draft");
      if (writes === 1) {
        revision = "fresh";
        return json(route, { code: "mappings_changed", message: "Mapping sources changed", fieldProblems: [] }, 409);
      }
      return json(route, state([], revision));
    }
    return fallback(route);
  });
  await page.goto("/mappings/sources");
  await page.getByRole("button", { name: "Add mapping source", exact: true }).click();
  await page.getByLabel("Name", { exact: true }).fill("Retained draft");
  await page.getByLabel("Git repository URL").fill("https://example.com/mappings.git");
  await page.getByLabel("Branch", { exact: true }).fill("review/draft");
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByRole("alert")).toHaveText("Mapping sources changed");
  await expect(page.getByLabel("Name", { exact: true })).toHaveValue("Retained draft");
  await expect(page.getByLabel("Branch", { exact: true })).toHaveValue("review/draft");
  await expect(page.getByRole("button", { name: "Save", exact: true })).toBeEnabled();
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  expect(writes).toBe(2);
});

test("a failed enable change rolls back its switch and reports the error", async ({ page }) => {
  await mockApi(page, async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (path === "/api/v1/workspaces") return json(route, [workspace]);
    if (path === "/api/v1/mappings/state") return json(route, state([]));
    if (path === "/api/v1/mappings/sources")
      return json(route, {
        fingerprint,
        sources: [
          localMappingSource,
          {
            ...localMappingSource,
            id: "mirror",
            displayName: "My mirror",
            kind: "upstream",
            url: "https://example.com/mappings.git",
            branch: "main",
            repositoryPath: null,
            pollIntervalSeconds: 3600,
          },
        ],
      });
    if (path === "/api/v1/mappings/sources/mirror")
      return json(route, { code: "internal_error", message: "Source change failed", fieldProblems: [] }, 500);
    return fallback(route);
  });
  await page.goto("/mappings/sources");
  const enabled = page.getByRole("switch", { name: "Enable My mirror", exact: true });
  await enabled.click();
  await expect(page.getByRole("alert")).toHaveText("Source change failed");
  await expect(enabled).toBeChecked();
  await expect(enabled).toBeEnabled();
});

test("a local decision can be explicitly reverted to its upstream record", async ({ page }) => {
  let items = [claim("original", original, ["local", "mirror"])];
  let revision = fingerprint;
  await mockApi(page, async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path === "/api/v1/workspaces") return json(route, [workspace]);
    if (path === "/api/v1/mappings/state") return json(route, state(items, revision));
    if (path === "/api/v1/mappings") return json(route, { fingerprint: revision, items, nextCursor: null });
    if (path === "/api/v1/mappings/original/revert") {
      expect(route.request().postDataJSON()).toEqual({ expectedFingerprint: fingerprint });
      items = [claim("original", original, ["mirror"])];
      revision = "reverted";
      return json(route, state(items, revision));
    }
    return fallback(route);
  });
  await page.goto("/mappings");
  await page.getByRole("button", { name: "Revert to upstream", exact: true }).click();
  await page.getByRole("button", { name: "Revert", exact: true }).click();
  await expect(page.getByRole("button", { name: "Revert to upstream", exact: true })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Delete mapping", exact: true })).toBeDisabled();
});

test("claims remain searchable and paginated with visible decisions", async ({ page }, testInfo) => {
  const queries: URL[] = [];
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === "/api/v1/mappings/state") return json(route, state([claim("first", original)]));
    if (url.pathname === "/api/v1/mappings") {
      queries.push(url);
      const second = url.searchParams.has("cursor");
      return json(route, {
        fingerprint,
        items: [claim(second ? "second" : "first", second ? corrected : original)],
        nextCursor: second ? null : "next",
      });
    }
    return fallback(route);
  });
  await page.goto("/mappings");
  const list = page.getByRole("list", { name: "Mapping claims" });
  await expect(list).toContainText("movie/603");
  await expect(page.getByRole("button", { name: "Create mapping" })).toBeEnabled();
  await expect(page.getByRole("button", { name: "Delete mapping" })).toBeDisabled();
  await page.getByText("Annotations (1)").click();
  await expect(page.getByText("#@note Reviewed", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Next page" }).click();
  await expect(list).toContainText("movie/604");
  expect(queries.at(-1)?.searchParams.get("expectedFingerprint")).toBe(fingerprint);
  await page.getByRole("button", { name: "Previous page" }).click();
  await page.getByRole("textbox", { name: "Search mappings" }).fill("Matrix");
  await expect.poll(() => queries.at(-1)?.searchParams.get("search")).toBe("Matrix");
  expect(queries.at(-1)?.searchParams.has("cursor")).toBe(false);
  await expectNoHorizontalOverflow(page);
  await page.screenshot({ path: `test-results/mappings-${testInfo.project.name}.png`, fullPage: true });
});

test("an empty store supports preview, creation, editing and local deletion", async ({ page }) => {
  let items: MappingResult[] = [];
  let revision = fingerprint;
  const writes: unknown[] = [];
  await mockApi(page, async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (path === "/api/v1/workspaces") return json(route, [workspace]);
    if (path === "/api/v1/mappings/state") return json(route, state(items, revision));
    if (path === "/api/v1/mappings/preview")
      return json(route, claim("local", request.postDataJSON().record, ["local"]));
    if (path === "/api/v1/mappings/local" && request.method() === "DELETE") {
      expect(request.postDataJSON().expectedFingerprint).toBe(revision);
      items = [];
      return json(route, state(items, "deleted"));
    }
    if (path === "/api/v1/mappings") {
      if (request.method() === "GET") return json(route, { fingerprint: revision, items, nextCursor: null });
      const body = request.postDataJSON();
      expect(body.expectedFingerprint).toBe(revision);
      writes.push(body);
      items = [claim("local", body.record, ["local"])];
      revision = `saved-${writes.length}`;
      return json(route, state(items, revision));
    }
    return fallback(route);
  });
  await page.goto("/mappings");
  await expect(page.getByText("No mappings found", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: /Import mappings|Export .*mappings/ })).toHaveCount(0);
  const create = page.getByRole("button", { name: "Create mapping" });
  await create.click();
  await page.getByRole("textbox", { name: "Mapping record", exact: true }).fill(original);
  await expect(page.getByRole("button", { name: "Save", exact: true })).toBeDisabled();
  await page.getByRole("button", { name: "Preview", exact: true }).click();
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(create).toBeFocused();
  await page.getByRole("button", { name: "Edit mapping" }).click();
  await page.getByRole("textbox", { name: "Mapping record", exact: true }).fill(corrected);
  await page.getByRole("button", { name: "Preview", exact: true }).click();
  await page.getByRole("button", { name: "Save", exact: true }).click();
  expect(writes).toEqual([
    { expectedFingerprint: fingerprint, record: original },
    { expectedFingerprint: "saved-1", record: corrected, replaces: "local" },
  ]);
  await expect(page.getByRole("list", { name: "Mapping claims" })).toContainText("movie/604");
  await page.getByRole("button", { name: "Delete mapping" }).click();
  await page.getByRole("button", { name: "Delete", exact: true }).click();
  await expect(page.getByText("No mappings found", { exact: true })).toBeVisible();
  await expectNoHorizontalOverflow(page);
});

test("replacing an upstream claim is explicit and its suppression can be restored", async ({ page }) => {
  let items = [claim("original", original)];
  let revision = fingerprint;
  await mockApi(page, async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (path === "/api/v1/workspaces") return json(route, [workspace]);
    if (path === "/api/v1/mappings/state") return json(route, state(items, revision));
    if (path === "/api/v1/mappings/preview") return json(route, claim("corrected", corrected, ["local"]));
    if (path === "/api/v1/mappings/original/decision") {
      expect(request.postDataJSON()).toEqual({ expectedFingerprint: revision, rejected: false });
      items = items.map((item) => ({ ...item, rejected: false, status: "conflicting" }));
      revision = "restored";
      return json(route, state(items, revision));
    }
    if (path === "/api/v1/mappings") {
      if (request.method() === "GET") return json(route, { fingerprint: revision, items, nextCursor: null });
      expect(request.postDataJSON()).toEqual({
        expectedFingerprint: fingerprint,
        record: corrected,
        replaces: "original",
      });
      items = [{ ...items[0], rejected: true, status: "rejected" }, claim("corrected", corrected, ["local"])];
      revision = "replaced";
      return json(route, state(items, revision));
    }
    return fallback(route);
  });
  await page.goto("/mappings");
  await page.getByRole("button", { name: "Replace mapping" }).click();
  await page.getByRole("textbox", { name: "Mapping record", exact: true }).fill(corrected);
  await page.getByRole("button", { name: "Preview", exact: true }).click();
  await page.getByRole("button", { name: "Replace", exact: true }).click();
  await expect(page.getByRole("list", { name: "Mapping claims" })).toContainText("Suppressed");
  await page.getByRole("button", { name: "Restore mapping" }).click();
  await expect(page.getByRole("list", { name: "Mapping claims" })).toContainText("Conflicting");
});

test("record changes discard stale queries and reload status", async ({ page }) => {
  let statusReads = 0;
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === "/api/v1/mappings/state")
      return json(route, state([claim("new", corrected)], ++statusReads === 1 ? fingerprint : "new"));
    if (url.pathname === "/api/v1/mappings") {
      if (url.searchParams.get("expectedFingerprint") === fingerprint)
        return json(route, { code: "mappings_changed", message: "Mappings changed", fieldProblems: [] }, 409);
      return json(route, { fingerprint: "new", items: [claim("new", corrected)], nextCursor: null });
    }
    return fallback(route);
  });
  await page.goto("/mappings");
  await expect(page.getByRole("list", { name: "Mapping claims" })).toContainText("movie/604");
  expect(statusReads).toBe(2);
});

test("a stale write preserves the record and requires a new preview before retrying", async ({ page }) => {
  let statusReads = 0;
  let writes = 0;
  await mockApi(page, async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (path === "/api/v1/workspaces") return json(route, [workspace]);
    if (path === "/api/v1/mappings/state") return json(route, state([], ++statusReads === 1 ? fingerprint : "new"));
    if (path === "/api/v1/mappings/preview") return json(route, claim("local", original, ["local"]));
    if (path === "/api/v1/mappings") {
      if (request.method() === "GET") return json(route, { fingerprint: "new", items: [], nextCursor: null });
      writes += 1;
      if (writes === 2) {
        expect(request.postDataJSON().expectedFingerprint).toBe("new");
        return json(route, state([claim("local", original, ["local"])], "saved"));
      }
      return json(route, { code: "mappings_changed", message: "Mappings changed", fieldProblems: [] }, 409);
    }
    return fallback(route);
  });
  await page.goto("/mappings");
  await page.getByRole("button", { name: "Create mapping" }).click();
  await page.getByRole("textbox", { name: "Mapping record", exact: true }).fill(original);
  await page.getByRole("button", { name: "Preview", exact: true }).click();
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByRole("alert")).toHaveText("Mappings changed");
  await expect(page.getByRole("textbox", { name: "Mapping record", exact: true })).toHaveValue(original);
  await expect.poll(() => statusReads).toBe(2);
  expect(writes).toBe(1);
  await expect(page.getByRole("button", { name: "Save", exact: true })).toBeDisabled();
  await page.getByRole("button", { name: "Preview", exact: true }).click();
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  expect(writes).toBe(2);
  await expect(page.getByRole("button", { name: "Create mapping" })).toBeFocused();
});
