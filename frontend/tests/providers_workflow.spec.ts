import { createHash } from "node:crypto";
import { expect, test } from "@playwright/test";
import type { ProviderInstallation, ProviderRepository } from "../src/lib/api/providers.ts";
import {
  expectNoHorizontalOverflow,
  fallback,
  json,
  mockApi,
  providerTarget,
  timestamp,
  workspace,
} from "./support/api.ts";

const providerId = "dev.trakkin.fixture";
const officialRepository: ProviderRepository = {
  id: "dev.trakkin.official",
  displayName: "Trakkin official",
  metadataBaseUrl: "https://providers.trakkin.dev/metadata/",
  targetsBaseUrl: "https://providers.trakkin.dev/targets/",
  official: true,
  enabled: true,
  catalogSequence: 1,
  refreshedAt: timestamp,
};
const oldInstallation: ProviderInstallation = {
  id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  repositoryId: officialRepository.id,
  providerId,
  version: "0.9.0",
  target: providerTarget,
  packageSha256: "a".repeat(64),
  active: false,
  generation: null,
  installedAt: timestamp,
};

test("imports repositories and manages provider versions", async ({ page }) => {
  let repositories: ProviderRepository[] = [officialRepository];
  let installations: ProviderInstallation[] = [oldInstallation];
  let refreshRequests = 0;
  let registeredRootLength = 0;
  let registeredRootSha256 = "";
  let activeInstallationId = "";
  let deleteRequests = 0;
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces" && method === "GET") return json(route, [workspace]);
    if (url.pathname === "/api/v1/provider-repositories" && method === "GET") {
      return json(route, repositories);
    }
    if (url.pathname === "/api/v1/provider-repositories" && method === "POST") {
      const body = route.request().postDataJSON() as {
        id: string;
        displayName: string;
        metadataBaseUrl: string;
        targetsBaseUrl: string;
        trustedRoot: number[];
        trustedRootSha256: string;
      };
      registeredRootLength = body.trustedRoot.length;
      registeredRootSha256 = body.trustedRootSha256;
      const repository = {
        id: body.id,
        displayName: body.displayName,
        metadataBaseUrl: body.metadataBaseUrl,
        targetsBaseUrl: body.targetsBaseUrl,
        official: false,
        enabled: true,
        catalogSequence: null,
        refreshedAt: null,
      };
      repositories = [...repositories, repository];
      return json(route, repository, 201);
    }
    if (url.pathname === `/api/v1/provider-repositories/${officialRepository.id}/refresh` && method === "POST") {
      refreshRequests += 1;
      return json(route, officialRepository);
    }
    if (url.pathname === "/api/v1/providers" && method === "GET") {
      return json(route, [
        {
          repositoryId: officialRepository.id,
          providerId,
          name: "Fixture",
          summary: "Deterministic provider",
          version: "1.0.0",
          channels: ["stable"],
          targets: [providerTarget],
          installedTargets: installations
            .filter((installation) => installation.version === "1.0.0")
            .map((installation) => installation.target),
          activeTarget:
            installations.find((installation) => installation.version === "1.0.0" && installation.active)?.target ??
            null,
        },
      ]);
    }
    if (url.pathname === "/api/v1/provider-installations" && method === "GET") {
      return json(route, installations);
    }
    if (url.pathname === "/api/v1/provider-installations" && method === "POST") {
      const body = route.request().postDataJSON() as {
        repositoryId: string;
        providerId: string;
        version: string;
        target: string;
      };
      const installation = {
        id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        ...body,
        packageSha256: "b".repeat(64),
        active: false,
        generation: null,
        installedAt: timestamp,
      };
      installations = [...installations, installation];
      return json(route, installation, 201);
    }
    if (url.pathname === `/api/v1/providers/${providerId}/activation` && method === "PUT") {
      activeInstallationId = (route.request().postDataJSON() as { installationId: string }).installationId;
      installations = installations.map((installation) => ({
        ...installation,
        active: installation.id === activeInstallationId,
        generation: installation.id === activeInstallationId ? "cccccccc-cccc-4ccc-8ccc-cccccccccccc" : null,
      }));
      return json(
        route,
        installations.find((installation) => installation.active),
      );
    }
    if (url.pathname.startsWith("/api/v1/provider-installations/") && method === "DELETE") {
      deleteRequests += 1;
      const installationId = url.pathname.split("/").at(-1);
      installations = installations.filter((installation) => installation.id !== installationId);
      return route.fulfill({ status: 204 });
    }
    return fallback(route);
  });

  await page.goto("/providers");
  await expect(page.getByRole("heading", { name: "Providers" })).toBeVisible();
  await expect(page.getByText("Trakkin official", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Refresh" }).first().click();
  await expect.poll(() => refreshRequests).toBe(1);

  await page.getByRole("button", { name: "Add repository" }).click();
  await page.getByLabel("Repository ID").fill("org.example.providers");
  await page.getByLabel("Display name").fill("Example providers");
  await page.getByLabel("Metadata URL").fill("https://example.org/metadata/");
  await page.getByLabel("Targets URL").fill("https://example.org/targets/");
  const trustedRoot = Buffer.from('{"signed":{}}');
  const trustedRootSha256 = createHash("sha256").update(trustedRoot).digest("hex");
  await page.getByLabel("Trusted root metadata").setInputFiles({
    name: "root.json",
    mimeType: "application/json",
    buffer: trustedRoot,
  });
  await page.getByLabel("Trusted root SHA-256").fill(trustedRootSha256);
  await expect(page.getByText(trustedRootSha256, { exact: true })).toBeVisible();
  await expectNoHorizontalOverflow(page);
  await page.getByRole("button", { name: "Trust repository" }).click();
  await expect(page.getByText("Example providers", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Add repository" })).toBeFocused();
  expect(registeredRootLength).toBeGreaterThan(0);
  expect(registeredRootSha256).toBe(trustedRootSha256);

  await page.getByRole("button", { name: `Install ${providerId} 1.0.0 for ${providerTarget}` }).click();
  expect(activeInstallationId).toBe("");
  await expect(page.getByText("1.0.0", { exact: true }).last()).toBeVisible();
  await page.getByRole("button", { name: `Activate ${providerId} 1.0.0 for ${providerTarget}` }).click();
  await expect.poll(() => activeInstallationId).toBe("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb");
  await page.getByRole("button", { name: `Activate ${providerId} 0.9.0 for ${providerTarget}` }).click();
  await expect.poll(() => activeInstallationId).toBe(oldInstallation.id);
  await page.getByRole("button", { name: `Remove ${providerId} 1.0.0 for ${providerTarget}` }).click();
  await expect(page.getByRole("alertdialog")).toBeVisible();
  await page.getByRole("button", { name: "Cancel" }).click();
  expect(deleteRequests).toBe(0);
  await page.getByRole("button", { name: `Remove ${providerId} 1.0.0 for ${providerTarget}` }).click();
  await page.getByRole("button", { name: "Remove version" }).click();
  await expect.poll(() => deleteRequests).toBe(1);
  await expect(page.getByRole("button", { name: `Remove ${providerId} 1.0.0 for ${providerTarget}` })).toHaveCount(0);
  await expectNoHorizontalOverflow(page);
});

test("rejects oversized trusted root files before hashing", async ({ page }) => {
  let registerRequests = 0;
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === "/api/v1/provider-repositories" && method === "GET") {
      return json(route, [officialRepository]);
    }
    if (url.pathname === "/api/v1/provider-repositories" && method === "POST") {
      registerRequests += 1;
      return json(route, officialRepository, 201);
    }
    if (url.pathname === "/api/v1/providers" || url.pathname === "/api/v1/provider-installations") {
      return json(route, []);
    }
    return fallback(route);
  });

  await page.goto("/providers");
  await page.getByRole("button", { name: "Add repository" }).click();
  await page.getByLabel("Trusted root metadata").setInputFiles({
    name: "oversized-root.json",
    mimeType: "application/json",
    buffer: Buffer.alloc(1024 * 1024 + 1),
  });

  await expect(page.getByRole("alert")).toContainText("Trusted root metadata must not exceed 1 MiB");
  await expect(page.getByRole("button", { name: "Trust repository" })).toBeDisabled();
  expect(registerRequests).toBe(0);
});

test("surfaces repository trust field problems", async ({ page }) => {
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === "/api/v1/provider-repositories" && method === "GET") {
      return json(route, [officialRepository]);
    }
    if (url.pathname === "/api/v1/provider-repositories" && method === "POST") {
      return json(
        route,
        {
          code: "invalid_field",
          message: "provider repository is invalid",
          fieldProblems: [
            {
              path: "trustedRoot",
              code: "invalid_tuf_root",
              message: "trusted root must contain signed TUF root metadata",
            },
          ],
        },
        400,
      );
    }
    if (url.pathname === "/api/v1/providers" || url.pathname === "/api/v1/provider-installations") {
      return json(route, []);
    }
    return fallback(route);
  });

  await page.goto("/providers");
  await page.getByRole("button", { name: "Add repository" }).click();
  await page.getByLabel("Repository ID").fill("org.invalid.providers");
  await page.getByLabel("Display name").fill("Invalid providers");
  await page.getByLabel("Metadata URL").fill("https://invalid.example/metadata/");
  await page.getByLabel("Targets URL").fill("https://invalid.example/targets/");
  await page.getByLabel("Trusted root metadata").setInputFiles({
    name: "root.json",
    mimeType: "application/json",
    buffer: Buffer.from("{}"),
  });
  await page.getByLabel("Trusted root SHA-256").fill(createHash("sha256").update(Buffer.from("{}")).digest("hex"));
  await page.getByRole("button", { name: "Trust repository" }).click();

  await expect(page.getByRole("alert")).toContainText(
    "provider repository is invalid: trusted root must contain signed TUF root metadata",
  );
});

test("serializes lifecycle actions for one provider", async ({ page }) => {
  const first = { ...oldInstallation, id: "11111111-1111-4111-8111-111111111111", version: "1.0.0" };
  const second = { ...oldInstallation, id: "22222222-2222-4222-8222-222222222222", version: "2.0.0" };
  let installations = [first, second];
  let activationRequests = 0;
  let releaseActivation: (() => void) | undefined;
  const activationBarrier = new Promise<void>((resolve) => {
    releaseActivation = resolve;
  });
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === "/api/v1/provider-repositories") return json(route, [officialRepository]);
    if (url.pathname === "/api/v1/providers" && method === "GET") return json(route, []);
    if (url.pathname === "/api/v1/provider-installations" && method === "GET") {
      return json(route, installations);
    }
    if (url.pathname === `/api/v1/providers/${providerId}/activation` && method === "PUT") {
      activationRequests += 1;
      const installationId = (route.request().postDataJSON() as { installationId: string }).installationId;
      await activationBarrier;
      installations = installations.map((installation) => ({
        ...installation,
        active: installation.id === installationId,
        generation: installation.id === installationId ? "33333333-3333-4333-8333-333333333333" : null,
      }));
      return json(
        route,
        installations.find((installation) => installation.active),
      );
    }
    return fallback(route);
  });

  await page.goto("/providers");
  const firstActivation = page.getByRole("button", { name: `Activate ${providerId} 1.0.0 for ${providerTarget}` });
  const secondActivation = page.getByRole("button", { name: `Activate ${providerId} 2.0.0 for ${providerTarget}` });
  await firstActivation.click();
  await expect(secondActivation).toBeDisabled();
  expect(activationRequests).toBe(1);
  releaseActivation?.();
  await expect(page.getByText("Active", { exact: true }).last()).toBeVisible();
});

test("keeps removal failures visible in the confirmation dialog", async ({ page }) => {
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === "/api/v1/provider-repositories") return json(route, [officialRepository]);
    if (url.pathname === "/api/v1/providers" && method === "GET") return json(route, []);
    if (url.pathname === "/api/v1/provider-installations" && method === "GET") {
      return json(route, [oldInstallation]);
    }
    if (url.pathname === `/api/v1/provider-installations/${oldInstallation.id}` && method === "DELETE") {
      return json(
        route,
        {
          code: "provider_installation_active",
          message: "active provider installation cannot be removed",
          fieldProblems: [],
        },
        409,
      );
    }
    return fallback(route);
  });

  await page.goto("/providers");
  await page.getByRole("button", { name: `Remove ${providerId} 0.9.0 for ${providerTarget}` }).click();
  const dialog = page.getByRole("alertdialog");
  await dialog.getByRole("button", { name: "Remove version" }).click();

  await expect(dialog.getByRole("alert")).toHaveText("active provider installation cannot be removed");
  await expect(dialog).toBeVisible();
});
