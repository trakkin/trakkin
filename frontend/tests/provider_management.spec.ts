import { expect, test } from "@playwright/test";
import type { Provider, ProviderReleaseInspection, ProviderSource } from "../src/lib/api/providers.ts";
import { expectNoHorizontalOverflow, fallback, json, mockApi, timestamp, workspace } from "./support/api.ts";

const communitySourceId = "00000000-0000-0000-0000-000000000001";
const exampleSourceId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const providerId = "dev.trakkin.fixture";
const inspectedIndexDigest = `sha256:${"c".repeat(64)}`;

const communitySource: ProviderSource = {
  id: communitySourceId,
  repositoryUrl: "https://registry.example/trakkin/providers",
  availableProviders: [{ providerId, versions: ["1.1.0", "1.0.0"] }],
  trustedAt: timestamp,
  lastCheckedAt: timestamp,
  lastError: null,
};

function provider(overrides: Partial<Provider> = {}): Provider {
  return {
    providerId,
    sourceId: communitySourceId,
    name: "Fixture provider",
    description: "Deterministic provider",
    version: "1.0.0",
    platform: "linux/amd64",
    indexDigest: `sha256:${"a".repeat(64)}`,
    binaryDigest: `sha256:${"b".repeat(64)}`,
    updatePolicy: "manual",
    availableUpdate: null,
    lastUpdateError: null,
    installedAt: timestamp,
    updatedAt: timestamp,
    ...overrides,
  };
}

function inspection(overrides: Partial<ProviderReleaseInspection> = {}): ProviderReleaseInspection {
  return {
    providerId,
    version: "1.0.0",
    name: "Fixture provider",
    description: "Deterministic provider",
    repositoryUrl: "https://github.com/trakkin/trakkin-community-providers",
    homepageUrl: "https://trakkin.dev",
    documentationUrl: null,
    license: "Apache-2.0",
    maintainer: { name: "Trakkin", email: null, url: "https://trakkin.dev" },
    distributor: "Trakkin",
    publishedAt: timestamp,
    revision: "0123456789abcdef",
    platforms: ["linux/amd64", "linux/arm64"],
    requirements: { trakkin: ">=0.0.0, <1.0.0" },
    compatibility: "compatible",
    platform: "linux/amd64",
    indexDigest: inspectedIndexDigest,
    manifestDigest: `sha256:${"d".repeat(64)}`,
    metadataDigest: `sha256:${"e".repeat(64)}`,
    binaryDigest: `sha256:${"f".repeat(64)}`,
    binarySize: 1_500_000,
    ...overrides,
  };
}

test("trusts a local OCI source and installs one provider identity", async ({ page }) => {
  let sources = [communitySource];
  let providers: Provider[] = [];
  let trustedSourceRequest: unknown;
  let installRequest: unknown;
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === "/api/v1/provider-sources" && method === "GET") return json(route, sources);
    if (url.pathname === "/api/v1/providers" && method === "GET") return json(route, providers);
    if (url.pathname.includes("/releases/") && method === "GET") {
      return json(
        route,
        inspection({
          providerId: "dev.example.calendar",
          version: decodeURIComponent(url.pathname.split("/").at(-1) ?? ""),
          name: "Calendar",
        }),
      );
    }
    if (url.pathname === "/api/v1/provider-sources" && method === "POST") {
      trustedSourceRequest = route.request().postDataJSON();
      const source: ProviderSource = {
        id: exampleSourceId,
        repositoryUrl: "http://127.0.0.1:5000/trakkin-dev/providers",
        availableProviders: [
          { providerId: "dev.example.calendar", versions: ["2.2.0", "2.1.0"] },
          { providerId: "dev.example.contacts", versions: ["1.0.0"] },
        ],
        trustedAt: timestamp,
        lastCheckedAt: null,
        lastError: null,
      };
      sources = [...sources, source];
      return json(route, source, 201);
    }
    if (url.pathname === "/api/v1/providers" && method === "POST") {
      installRequest = route.request().postDataJSON();
      const installed = provider({
        providerId: "dev.example.calendar",
        sourceId: exampleSourceId,
        name: "Calendar",
        version: "2.1.0",
        indexDigest: inspectedIndexDigest,
        updatePolicy: "automatic",
      });
      providers = [installed];
      return json(route, installed, 201);
    }
    return fallback(route);
  });

  await page.goto("/providers");
  await expect(page.getByText("https://registry.example/trakkin/providers", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Trust source", exact: true }).click();
  await page.getByLabel("Repository URL").fill("http://127.0.0.1:5000/trakkin-dev/providers");
  await page.getByLabel("I trust this source to publish executable provider code.").check();
  await page.getByRole("button", { name: "Trust source", exact: true }).last().click();

  await expect(page.getByText("Trusted provider repository", { exact: true })).toBeVisible();
  await expect(page.getByText("2 providers", { exact: true })).toBeVisible();
  expect(trustedSourceRequest).toEqual({
    repositoryUrl: "http://127.0.0.1:5000/trakkin-dev/providers",
  });

  const calendarRow = page.getByRole("listitem").filter({ hasText: "dev.example.calendar" });
  const previewButton = calendarRow.getByRole("button", {
    name: "Preview release for dev.example.calendar",
    exact: true,
  });
  const installButton = calendarRow.getByRole("button", { name: "Install dev.example.calendar", exact: true });
  await expect
    .poll(async () => {
      const previewBox = await previewButton.boundingBox();
      const installBox = await installButton.boundingBox();
      return {
        equalHeight: previewBox?.height === installBox?.height,
        equalWidth: previewBox?.width === installBox?.width,
        installIsSquare: installBox?.width === installBox?.height,
        previewIsSquare: previewBox?.width === previewBox?.height,
      };
    })
    .toEqual({ equalHeight: true, equalWidth: true, installIsSquare: true, previewIsSquare: true });
  await previewButton.click();
  await expect(page.getByRole("dialog", { name: "Preview provider release" })).toBeVisible();
  await page.getByRole("button", { name: "Close provider release review" }).click();
  await calendarRow.getByRole("button", { name: "Install dev.example.calendar", exact: true }).click();
  const releaseReview = page.getByRole("dialog", { name: "Preview provider release" });
  await releaseReview.getByLabel("Version for dev.example.calendar").click();
  await page.getByRole("option", { name: "2.1.0" }).click();
  await releaseReview.getByRole("switch", { name: "Install updates automatically for dev.example.calendar" }).check();
  await expect(releaseReview.getByRole("heading", { name: "Calendar", exact: true })).toBeVisible();
  await expect(releaseReview.getByText("Trakkin", { exact: true }).first()).toBeVisible();
  const releaseDetails = releaseReview.getByRole("region", {
    name: "Release details for dev.example.calendar 2.1.0",
  });
  await expect
    .poll(async () => ({
      drawerFitsViewport: await releaseReview.evaluate(
        (element) => element.getBoundingClientRect().width <= window.innerWidth,
      ),
      releaseHasHorizontalOverflow: await releaseDetails.evaluate(
        (element) => element.scrollWidth > element.clientWidth,
      ),
    }))
    .toEqual({ drawerFitsViewport: true, releaseHasHorizontalOverflow: false });
  await releaseReview.locator("#confirm-provider-install").click();

  expect(installRequest).toEqual({
    sourceId: exampleSourceId,
    providerId: "dev.example.calendar",
    version: "2.1.0",
    indexDigest: inspectedIndexDigest,
    updatePolicy: "automatic",
  });
  const installedCalendarRow = page.getByRole("listitem").filter({ hasText: "Calendar" });
  await expect(installedCalendarRow.getByText("Version 2.1.0", { exact: true })).toBeVisible();
  await expect(installedCalendarRow.getByText(`Index ${"c".repeat(12)}...`, { exact: true })).toBeVisible();
  await expect(installedCalendarRow.locator(`time[datetime="${timestamp}"]`)).toBeVisible();
  await expectNoHorizontalOverflow(page);
});

test("bounds each source provider catalog with vertical overflow", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const sourceWithManyProviders: ProviderSource = {
    ...communitySource,
    availableProviders: Array.from({ length: 14 }, (_, index) => ({
      providerId: `dev.example.provider-${index.toString().padStart(2, "0")}`,
      versions: ["1.0.0"],
    })),
  };
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === "/api/v1/provider-sources") return json(route, [sourceWithManyProviders]);
    if (url.pathname === "/api/v1/providers") return json(route, []);
    return fallback(route);
  });

  await page.goto("/providers");
  const catalog = page.getByRole("list", { name: `Providers from ${communitySource.repositoryUrl}` });
  await expect(catalog.getByRole("listitem")).toHaveCount(14);
  await expect(page.getByText("Not installed", { exact: true })).toHaveCount(0);
  const firstInstallButton = catalog.getByRole("button", { name: "Install dev.example.provider-00", exact: true });
  await expect(firstInstallButton.getByText("Install", { exact: true })).toBeHidden();
  await expect
    .poll(() =>
      catalog.evaluate((element) => ({
        hasHorizontalOverflow: element.scrollWidth > element.clientWidth,
        hasOverflow: element.scrollHeight > element.clientHeight,
        overflowY: getComputedStyle(element).overflowY,
      })),
    )
    .toEqual({ hasHorizontalOverflow: false, hasOverflow: true, overflowY: "auto" });
});

test("notifies and applies a manual update without creating another installation", async ({ page }) => {
  let providers = [
    provider({
      version: "1.0.0",
      availableUpdate: { version: "1.1.0", discoveredAt: timestamp },
    }),
  ];
  let updateRequest: unknown;
  let releaseUpdate: () => void = () => {};
  const updateGate = new Promise<void>((resolve) => {
    releaseUpdate = resolve;
  });
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === "/api/v1/provider-sources") return json(route, [communitySource]);
    if (url.pathname === "/api/v1/providers" && method === "GET") return json(route, providers);
    if (url.pathname.includes("/releases/") && method === "GET") {
      return json(route, inspection({ version: "1.1.0" }));
    }
    if (url.pathname === `/api/v1/providers/${encodeURIComponent(providerId)}/update` && method === "POST") {
      updateRequest = route.request().postDataJSON();
      await updateGate;
      const updated = provider({ version: "1.1.0", availableUpdate: null });
      providers = [updated];
      return json(route, updated);
    }
    if (url.pathname === `/api/v1/providers/${encodeURIComponent(providerId)}/update-policy` && method === "PUT") {
      const updatePolicy = (route.request().postDataJSON() as { updatePolicy: Provider["updatePolicy"] }).updatePolicy;
      const updated = { ...providers[0], updatePolicy } as Provider;
      providers = [updated];
      return json(route, updated);
    }
    return fallback(route);
  });

  await page.goto("/providers");
  await expect(page.getByText("1.1.0 available", { exact: true })).toBeVisible();
  const updateButton = page.getByRole("button", { name: "Update Fixture provider", exact: true });
  await updateButton.click();
  await expect(page.getByText("Immutable release digest", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Update provider", exact: true }).click();
  expect(updateRequest).toEqual({ version: "1.1.0", indexDigest: inspectedIndexDigest });
  releaseUpdate();

  await expect(page.getByText("1.1.0 available", { exact: true })).toHaveCount(0);
  await expect(page.getByText("Version 1.1.0", { exact: true })).toHaveCount(1);
  const automaticUpdates = page.getByRole("switch", { name: "Automatic updates for Fixture provider" });
  await automaticUpdates.check();
  await expect(automaticUpdates).toBeChecked();
  await expectNoHorizontalOverflow(page);
});

test("offers an explicit replacement when the current SemVer tag has a new digest", async ({ page }) => {
  let providers = [provider()];
  let updateRequest: unknown;
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === "/api/v1/provider-sources") return json(route, [communitySource]);
    if (url.pathname === "/api/v1/providers" && method === "GET") return json(route, providers);
    if (url.pathname.includes("/releases/") && method === "GET") return json(route, inspection());
    if (url.pathname === `/api/v1/providers/${encodeURIComponent(providerId)}/update` && method === "POST") {
      updateRequest = route.request().postDataJSON();
      const replacement = provider({ indexDigest: inspectedIndexDigest });
      providers = [replacement];
      return json(route, replacement);
    }
    return fallback(route);
  });

  await page.goto("/providers");
  await page.getByRole("button", { name: "Review current release for Fixture provider" }).click();
  await expect(page.getByRole("button", { name: "Replace provider", exact: true })).toBeEnabled();
  await page.getByRole("button", { name: "Replace provider", exact: true }).click();

  expect(updateRequest).toEqual({ version: "1.0.0", indexDigest: inspectedIndexDigest });
  await expect(page.getByText(`Index ${"c".repeat(12)}...`, { exact: true })).toBeVisible();
  await expectNoHorizontalOverflow(page);
});

test("renders durable source and update failures and keeps removal errors in context", async ({ page }) => {
  const failedSource: ProviderSource = {
    ...communitySource,
    lastError: "registry tag discovery timed out",
  };
  const failedProvider = provider({ lastUpdateError: "provider probe failed" });
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === "/api/v1/provider-sources") return json(route, [failedSource]);
    if (url.pathname === "/api/v1/providers" && method === "GET") return json(route, [failedProvider]);
    if (url.pathname === `/api/v1/providers/${encodeURIComponent(providerId)}` && method === "DELETE") {
      return json(
        route,
        {
          code: "provider_conflict",
          message: "provider cannot be removed while connections use it",
          fieldProblems: [],
        },
        409,
      );
    }
    return fallback(route);
  });

  await page.goto("/providers");
  await expect(page.getByRole("alert").filter({ hasText: "Last refresh failed" })).toContainText(
    "registry tag discovery timed out",
  );
  await expect(page.getByRole("alert").filter({ hasText: "Update failed" })).toContainText("provider probe failed");
  await page.getByRole("button", { name: "Remove provider Fixture provider" }).click();
  const dialog = page.getByRole("alertdialog");
  await dialog.getByRole("button", { name: "Remove", exact: true }).click();

  await expect(dialog.getByRole("alert")).toHaveText("provider cannot be removed while connections use it");
  await expect(dialog).toBeVisible();
  await expectNoHorizontalOverflow(page);
});
