import { expect, test } from "@playwright/test";
import {
  apiError,
  authenticationId,
  bytes,
  connection,
  connectionId,
  expectNoHorizontalOverflow,
  fallback,
  json,
  mockApi,
  source,
  sourceId,
  task,
  timestamp,
  workspace,
  workspaceId,
} from "./support/api.ts";

test("shows connected status before expanding a connection", async ({ page }) => {
  let inspectionRequests = 0;
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === "/api/v1/connections") return json(route, [connection]);
    if (url.pathname === `/api/v1/connections/${connectionId}/sources`) return json(route, [source]);
    if (url.pathname === `/api/v1/connections/${connectionId}/inspection`) {
      inspectionRequests += 1;
      return json(route, authenticationInspection());
    }
    return fallback(route);
  });

  await page.goto(`/connections?workspace=${workspaceId}`);
  await expect(page.getByText("Connected", { exact: true })).toBeVisible();
  await expect(page.getByText("Setup required", { exact: true })).toHaveCount(0);
  expect(inspectionRequests).toBe(0);
});

test("edits source mapping roots", async ({ page }) => {
  let namespaces: string[] | null = null;
  const submitted: Array<string[] | null> = [];
  const policyPath = `/api/v1/workspaces/${workspaceId}/sources/${sourceId}/mapping-root-policy`;

  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces" && method === "GET") return json(route, [workspace]);
    if (url.pathname === "/api/v1/connections" && method === "GET") return json(route, [connection]);
    if (url.pathname === `/api/v1/connections/${connectionId}/inspection`) {
      return json(route, authenticationInspection());
    }
    if (url.pathname === `/api/v1/connections/${connectionId}/sources`) return json(route, [source]);
    if (url.pathname === policyPath && method === "GET") {
      return json(route, mappingRootPolicy(namespaces));
    }
    if (url.pathname === policyPath && method === "PUT") {
      const body = route.request().postDataJSON() as { namespaces: string[] | null };
      submitted.push(body.namespaces);
      namespaces = body.namespaces;
      return json(route, mappingRootPolicy(namespaces));
    }
    return fallback(route);
  });

  await page.goto(`/connections?workspace=${workspaceId}`);
  await page.getByRole("button", { name: "Expand Fixture account" }).first().click();
  await page.getByLabel("Configure mapping roots for Fixture Catalog").click();
  const drawer = page.getByRole("dialog", { name: "Mapping roots" });
  await expect(drawer).toBeVisible();
  await expect(drawer.getByRole("button", { name: "Provider defaults" })).toHaveAttribute("aria-pressed", "true");

  await drawer.getByRole("button", { name: "Custom namespaces" }).click();
  const exampleNamespace = drawer.getByLabel("Use example.media as an automatic mapping root");
  await expect(exampleNamespace).toBeChecked();
  await exampleNamespace.uncheck();
  await drawer.getByRole("button", { name: "Save" }).click();
  await expect.poll(() => submitted[0]).toEqual(["org.themoviedb"]);
  await expect(drawer).not.toBeVisible();

  await page.getByLabel("Configure mapping roots for Fixture Catalog").click();
  await drawer.getByRole("button", { name: "Provider defaults" }).click();
  await drawer.getByRole("button", { name: "Save" }).click();
  await expect.poll(() => submitted[1]).toBeNull();
  await expectNoHorizontalOverflow(page);
});

test("creates and edits active provider connections without stale command state", async ({ page }) => {
  const archiveConnectionId = "22222222-2222-2222-2222-333333333333";
  const archiveConnection = { ...connection, id: archiveConnectionId, displayName: "Archive fixture" };
  let createdDisplayName = "";
  let createdProviderId = "";
  let renamedDisplayName = "";
  let connectionDirectoryRequests = 0;
  let renameRequested = false;
  let releaseRename: () => void = () => {};
  const renameGate = new Promise<void>((resolve) => {
    releaseRename = resolve;
  });
  let deletionRequested = false;
  let releaseDeletion: () => void = () => {};
  const deletionGate = new Promise<void>((resolve) => {
    releaseDeletion = resolve;
  });
  let archiveCreateRequested = false;
  let releaseArchiveCreate: () => void = () => {};
  const archiveCreateGate = new Promise<void>((resolve) => {
    releaseArchiveCreate = resolve;
  });
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces" && method === "GET") return json(route, [workspace]);
    if (url.pathname === "/api/v1/providers" && method === "GET") {
      return json(route, [
        {
          providerId: "org.example.provider",
          sourceId: "88888888-8888-4888-8888-888888888888",
          name: "Example provider",
          description: "Example active provider",
          version: "2.0.0",
          platform: "linux/amd64",
          indexDigest: `sha256:${"a".repeat(64)}`,
          binaryDigest: `sha256:${"b".repeat(64)}`,
          updatePolicy: "manual",
          availableUpdate: null,
          lastUpdateError: null,
          installedAt: timestamp,
          updatedAt: timestamp,
        },
      ]);
    }
    if (url.pathname === "/api/v1/connections" && method === "GET") {
      connectionDirectoryRequests += 1;
      if (connectionDirectoryRequests === 1) {
        return apiError(route, 503, "connections_unavailable", "Connection directory is temporarily unavailable");
      }
      return json(route, []);
    }
    if (url.pathname === "/api/v1/connections" && method === "POST") {
      const body = route.request().postDataJSON() as { displayName: string; providerId: string };
      if (body.displayName === archiveConnection.displayName) {
        archiveCreateRequested = true;
        await archiveCreateGate;
        return json(route, archiveConnection, 201);
      }
      createdDisplayName = body.displayName;
      createdProviderId = body.providerId;
      return json(
        route,
        { ...connection, providerId: body.providerId, displayName: body.displayName, status: "pending" },
        201,
      );
    }
    if (url.pathname === `/api/v1/connections/${connectionId}` && method === "PATCH") {
      renamedDisplayName = (route.request().postDataJSON() as { displayName: string }).displayName;
      renameRequested = true;
      await renameGate;
      return json(route, { ...connection, displayName: renamedDisplayName });
    }
    if (
      url.pathname === `/api/v1/connections/${connectionId}/deletion-impact` ||
      url.pathname === `/api/v1/connections/${archiveConnectionId}/deletion-impact`
    ) {
      return json(route, deletionImpact());
    }
    if (url.pathname === `/api/v1/connections/${connectionId}/deletion` && method === "POST") {
      deletionRequested = true;
      await deletionGate;
      return json(route, deletionImpact());
    }
    if (url.pathname === `/api/v1/connections/${connectionId}/inspection`) {
      return json(route, {
        fields: [],
        settings: [],
        configuredSecretKeys: [],
        fieldProblems: [],
        authenticationMethods: [],
      });
    }
    if (url.pathname === `/api/v1/connections/${archiveConnectionId}/inspection`) {
      return json(route, {
        fields: [],
        settings: [],
        configuredSecretKeys: [],
        fieldProblems: [],
        authenticationMethods: [],
      });
    }
    if (url.pathname === `/api/v1/connections/${connectionId}/sources`) return json(route, []);
    if (url.pathname === `/api/v1/connections/${archiveConnectionId}/sources`) return json(route, []);
    return fallback(route);
  });

  await page.goto(`/connections?workspace=${workspaceId}`);
  const directoryError = page.getByRole("alert").filter({ hasText: "Connection directory is temporarily unavailable" });
  await expect(directoryError).toBeVisible();
  await expect(page.getByLabel("Connection name")).toHaveCount(0);
  await directoryError.getByRole("button", { name: "Retry" }).click();
  await expect.poll(() => connectionDirectoryRequests).toBe(2);
  await expect(page.getByLabel("Provider")).toHaveText("Example provider 2.0.0");
  await expect(page.getByLabel("Fixture token")).toHaveCount(0);
  await page.getByLabel("Connection name").fill("Fixture account");
  await page.getByRole("button", { name: "Create connection" }).click();

  await expect(page.getByRole("button", { name: "Collapse Fixture account" }).first()).toBeVisible();
  expect(createdDisplayName).toBe("Fixture account");
  expect(createdProviderId).toBe("org.example.provider");
  await expectNoHorizontalOverflow(page);

  await page.getByRole("button", { name: "New connection" }).click();
  await page.getByLabel("Connection name").fill(archiveConnection.displayName);
  await page.getByRole("button", { name: "Create connection" }).click();
  await expect.poll(() => archiveCreateRequested).toBe(true);
  await page.getByRole("button", { name: "Close" }).click();
  await page.getByRole("button", { name: "New connection" }).click();
  const currentComposerDraft = page.getByLabel("Connection name");
  await currentComposerDraft.fill("Current connection draft");
  await expect(page.getByRole("button", { name: "Create connection" })).toBeEnabled();
  const archiveCreateResponse = page.waitForResponse(
    (response) => new URL(response.url()).pathname === "/api/v1/connections" && response.request().method() === "POST",
  );
  releaseArchiveCreate();
  await archiveCreateResponse;
  await page.evaluate(
    () => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))),
  );
  await expect(page.getByRole("button", { name: "Expand Archive fixture" }).first()).toBeVisible();
  await expect(currentComposerDraft).toHaveValue("Current connection draft");
  await expect(page.getByRole("button", { name: "Create connection" })).toBeEnabled();
  await page.getByRole("button", { name: "Close" }).click();

  await page.getByRole("button", { name: "Rename Fixture account" }).click();
  await page.getByLabel("Rename Fixture account").fill("Renamed fixture");
  await page.getByRole("button", { name: "Save connection name" }).click();
  await expect.poll(() => renameRequested).toBe(true);
  await page.getByRole("button", { name: "Rename Archive fixture" }).click();
  const archiveRename = page.getByLabel("Rename Archive fixture");
  await archiveRename.fill("Current archive draft");
  const renameResponse = page.waitForResponse(
    (response) =>
      new URL(response.url()).pathname === `/api/v1/connections/${connectionId}` &&
      response.request().method() === "PATCH",
  );
  releaseRename();
  await renameResponse;
  await page.evaluate(
    () => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))),
  );
  await expect(page.getByText("Renamed fixture", { exact: true })).toBeVisible();
  await expect(archiveRename).toHaveValue("Current archive draft");
  await expect(page.getByRole("button", { name: "Save connection name" })).toBeEnabled();
  expect(renamedDisplayName).toBe("Renamed fixture");
  await page.getByRole("button", { name: "Cancel renaming" }).click();

  await page.getByRole("button", { name: "Delete Renamed fixture" }).click();
  await expect(page.getByRole("heading", { name: "Delete connection permanently?" })).toBeVisible();
  await page.getByRole("button", { name: "Delete permanently" }).click();
  await expect.poll(() => deletionRequested).toBe(true);
  await page.getByRole("button", { name: "Cancel" }).click();
  await page.getByRole("button", { name: "Delete Archive fixture" }).click();
  const archiveDeletionDialog = page.getByRole("alertdialog", { name: "Delete connection permanently?" });
  await expect(archiveDeletionDialog).toContainText("Archive fixture and its stored authentication data");
  await expect(page.getByRole("button", { name: "Delete permanently" })).toBeEnabled();
  const deletionResponse = page.waitForResponse(
    (response) =>
      new URL(response.url()).pathname === `/api/v1/connections/${connectionId}/deletion` &&
      response.request().method() === "POST",
  );
  releaseDeletion();
  await deletionResponse;
  await page.evaluate(
    () => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))),
  );
  await expect(page.getByText("Renamed fixture", { exact: true })).toHaveCount(0);
  await expect(page.getByRole("heading", { name: "Delete connection permanently?" })).toBeVisible();
  await expect(archiveDeletionDialog).toContainText("Archive fixture and its stored authentication data");
  await expect(page.getByRole("button", { name: "Delete permanently" })).toBeEnabled();
  await page.getByRole("button", { name: "Cancel" }).click();
  await expectNoHorizontalOverflow(page);
});

test("requires an active provider before creating a connection", async ({ page }) => {
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === "/api/v1/connections") return json(route, []);
    if (url.pathname === "/api/v1/providers") return json(route, []);
    return fallback(route);
  });

  await page.goto("/connections");

  await expect(page.getByRole("status")).toHaveText("Install and activate a provider before creating a connection.");
  await expect(page.getByRole("button", { name: "Create connection" })).toBeDisabled();
  await expectNoHorizontalOverflow(page);
});

test("keeps retained active providers selectable after catalog removal", async ({ page }) => {
  const retainedProviderId = "org.example.retained";
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === "/api/v1/workspaces") return json(route, [workspace]);
    if (url.pathname === "/api/v1/connections") return json(route, []);
    if (url.pathname === "/api/v1/providers") {
      return json(route, [
        {
          providerId: retainedProviderId,
          sourceId: "88888888-8888-4888-8888-888888888888",
          name: null,
          description: null,
          version: "2.0.0",
          platform: "linux/amd64",
          indexDigest: `sha256:${"a".repeat(64)}`,
          binaryDigest: `sha256:${"b".repeat(64)}`,
          updatePolicy: "manual",
          availableUpdate: null,
          lastUpdateError: null,
          installedAt: "2030-01-02T03:04:05Z",
          updatedAt: "2030-01-02T03:04:05Z",
        },
      ]);
    }
    return fallback(route);
  });

  await page.goto("/connections");

  await expect(page.getByLabel("Provider")).toHaveText(`${retainedProviderId} 2.0.0`);
  await page.getByLabel("Connection name").fill("Retained provider");
  await expect(page.getByRole("button", { name: "Create connection" })).toBeEnabled();
  await expectNoHorizontalOverflow(page);
});

test("reconfigures provider settings without exposing stored secrets", async ({ page }) => {
  let revision = "v2";
  let reconfiguration:
    | {
        settings: Array<{ key: string; value: { kind: string; value: string } }>;
        secretPatches: Array<{ key: string; value: number[] }>;
      }
    | undefined;
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces" && method === "GET") return json(route, [workspace]);
    if (url.pathname === "/api/v1/connections" && method === "GET") return json(route, [connection]);
    if (url.pathname === `/api/v1/connections/${connectionId}/inspection` && method === "GET") {
      return json(route, configurationInspection(revision));
    }
    if (url.pathname === `/api/v1/connections/${connectionId}/configuration` && method === "PUT") {
      reconfiguration = route.request().postDataJSON() as typeof reconfiguration;
      revision = reconfiguration?.settings[0]?.value.value ?? revision;
      return json(route, { ...connection, capabilityRevision: connection.capabilityRevision + 1 });
    }
    if (url.pathname === `/api/v1/connections/${connectionId}/sources` && method === "GET") return json(route, []);
    return fallback(route);
  });

  await page.goto(`/connections?workspace=${workspaceId}`);
  await page.getByRole("button", { name: "Expand Fixture account" }).first().click();
  await expect(page.getByLabel("Catalog revision")).toHaveText("Revision v2");
  await expect(page.getByLabel("API token")).toHaveValue("");
  await expect(page.getByText("Secret saved", { exact: true })).toBeVisible();
  await expect(page.getByLabel("API token")).toHaveAttribute("placeholder", "Enter replacement");
  await page.getByLabel("Catalog revision").click();
  await page.getByRole("option", { name: "Revision v1", exact: true }).click();
  await page.getByLabel("API token").fill("replacement-secret");
  await page.getByRole("button", { name: "Save", exact: true }).click();

  await expect
    .poll(() => reconfiguration?.settings[0])
    .toEqual({
      key: "revision",
      value: { kind: "text", value: "v1" },
    });
  expect(reconfiguration?.secretPatches).toEqual([{ key: "api_token", value: bytes("replacement-secret") }]);
  await expect(page.getByLabel("API token")).toHaveValue("");
  await expectNoHorizontalOverflow(page);
});

test("authenticates, discovers, and attaches a source", async ({ page }) => {
  let sourceAttached = false;
  let discovered = false;
  let connectionDisplayName = connection.displayName;
  const connectionMockModes = new Set<"deferRefresh">();
  let staleConnectionRefreshRequested = false;
  let releaseStaleConnectionRefresh: () => void = () => {};
  const staleConnectionRefreshGate = new Promise<void>((resolve) => {
    releaseStaleConnectionRefresh = resolve;
  });
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces" && method === "GET") {
      return json(route, [{ ...workspace, sources: sourceAttached ? workspace.sources : [] }]);
    }
    if (url.pathname === "/api/v1/connections" && method === "GET") {
      const snapshot = { ...connection, displayName: connectionDisplayName };
      if (connectionMockModes.delete("deferRefresh")) {
        staleConnectionRefreshRequested = true;
        await staleConnectionRefreshGate;
      }
      return json(route, [snapshot]);
    }
    if (url.pathname === `/api/v1/connections/${connectionId}` && method === "PATCH") {
      connectionDisplayName = ((await route.request().postDataJSON()) as { displayName: string }).displayName;
      return json(route, { ...connection, displayName: connectionDisplayName });
    }
    if (url.pathname === `/api/v1/connections/${connectionId}/inspection`) {
      return json(route, authenticationInspection());
    }
    if (url.pathname === `/api/v1/connections/${connectionId}/sources` && method === "GET") {
      return json(route, discovered ? [source] : []);
    }
    if (url.pathname === `/api/v1/connections/${connectionId}/authentications` && method === "POST") {
      return json(route, inputRequiredAuthentication(), 201);
    }
    if (
      url.pathname === `/api/v1/connections/${connectionId}/authentications/${authenticationId}/continuation` &&
      method === "POST"
    ) {
      return json(route, completedAuthentication());
    }
    if (url.pathname === `/api/v1/connections/${connectionId}/discovery-tasks` && method === "POST") {
      discovered = true;
      return json(route, { ...task("succeeded", true), kind: "source_discovery", sourceId: null }, 202);
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/sources/${sourceId}` && method === "PUT") {
      sourceAttached = true;
      return json(route, workspace);
    }
    return fallback(route);
  });

  await page.goto(`/connections?workspace=${workspaceId}`);
  await page.getByRole("button", { name: "Expand Fixture account" }).first().click();
  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await expect(page.getByLabel("Account name")).toHaveAttribute("aria-invalid", "true");
  await expect(page.getByRole("alert")).toHaveCount(0);
  await page.getByLabel("Account name").fill("Alex");
  await expect(page.getByLabel("Account name")).toHaveAttribute("aria-invalid", "false");
  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await page.getByLabel("Verification code").fill("123456");
  await page.getByRole("button", { name: "Continue" }).click();
  await expect(page.getByText("Connected as Alex")).toBeVisible();
  await expect(page.getByText("Fixture Catalog")).toBeVisible();
  await page.getByLabel("Attach Fixture Catalog to the workspace").click();
  await expect.poll(() => sourceAttached).toBe(true);

  connectionMockModes.add("deferRefresh");
  await page.getByRole("button", { name: "Discover", exact: true }).click();
  await expect.poll(() => staleConnectionRefreshRequested).toBe(true);
  await page.getByLabel("Rename Fixture account").click();
  await page.getByLabel("Rename Fixture account").fill("Renamed fixture account");
  await page.getByLabel("Save connection name").click();
  await expect(page.getByText("Renamed fixture account", { exact: true })).toBeVisible();
  const staleConnectionResponse = page.waitForResponse(
    (response) => new URL(response.url()).pathname === "/api/v1/connections" && response.request().method() === "GET",
  );
  releaseStaleConnectionRefresh();
  await staleConnectionResponse;
  await page.evaluate(
    () => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))),
  );
  await expect(page.getByText("Renamed fixture account", { exact: true })).toBeVisible();
  await expectNoHorizontalOverflow(page);
});

test("connection authentication polling stops after navigation", async ({ page }) => {
  let continuationRequests = 0;
  let releaseContinuation: () => void = () => {};
  const continuationGate = new Promise<void>((resolve) => {
    releaseContinuation = resolve;
  });
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces" && method === "GET") return json(route, [workspace]);
    if (url.pathname === "/api/v1/connections" && method === "GET") return json(route, [connection]);
    if (url.pathname === `/api/v1/connections/${connectionId}/inspection`) {
      return json(route, authenticationInspection());
    }
    if (url.pathname === `/api/v1/connections/${connectionId}/sources`) return json(route, []);
    if (url.pathname === `/api/v1/connections/${connectionId}/authentications` && method === "POST") {
      return json(route, waitingAuthentication(), 201);
    }
    if (
      url.pathname === `/api/v1/connections/${connectionId}/authentications/${authenticationId}/continuation` &&
      method === "POST"
    ) {
      continuationRequests += 1;
      await continuationGate;
      return json(route, waitingAuthentication());
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks`) return json(route, []);
    return fallback(route);
  });

  await page.goto(`/connections?workspace=${workspaceId}`);
  await page.getByRole("button", { name: "Expand Fixture account" }).first().click();
  await page.getByLabel("Account name").fill("Alex");
  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await expect.poll(() => continuationRequests).toBe(1);
  if ((page.viewportSize()?.width ?? 0) < 1024) {
    await page.getByRole("button", { name: "Open navigation" }).click();
    await page.getByRole("menuitem", { name: "Create workspace" }).click();
  } else {
    await page.getByRole("button", { name: "Create workspace" }).click();
  }
  await expect(page).toHaveURL(/\/workspaces\/new$/);
  releaseContinuation();
  await page.evaluate(() => new Promise<void>((resolve) => window.setTimeout(resolve, 350)));
  expect(continuationRequests).toBe(1);
  await expectNoHorizontalOverflow(page);
});

test("connection authentication highlights and saves required configuration before connecting", async ({ page }) => {
  let savedConfiguration: unknown;
  let authenticationRequests = 0;
  let authenticationStartedAfterSave = false;
  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === "/api/v1/workspaces" && method === "GET") return json(route, [workspace]);
    if (url.pathname === "/api/v1/connections" && method === "GET") return json(route, [connection]);
    if (url.pathname === `/api/v1/connections/${connectionId}/inspection`) {
      return json(route, configuredAuthenticationInspection());
    }
    if (url.pathname === `/api/v1/connections/${connectionId}/sources`) return json(route, []);
    if (url.pathname === `/api/v1/connections/${connectionId}/configuration` && method === "PUT") {
      savedConfiguration = await route.request().postDataJSON();
      return json(route, connection);
    }
    if (url.pathname === `/api/v1/connections/${connectionId}/authentications` && method === "POST") {
      authenticationRequests += 1;
      authenticationStartedAfterSave = savedConfiguration !== undefined;
      return json(route, inputRequiredAuthentication(), 201);
    }
    return fallback(route);
  });

  await page.goto(`/connections?workspace=${workspaceId}`);
  await page.getByRole("button", { name: "Expand Fixture account" }).first().click();
  await expect(page.getByRole("button", { name: "Save", exact: true })).toHaveCount(0);
  await expect(page.getByRole("textbox")).toHaveCount(1);
  const serviceUrl = page.getByLabel("Service URL");
  await expect(serviceUrl).toHaveAttribute("aria-invalid", "false");

  await page.getByRole("button", { name: "Connect", exact: true }).click();

  await expect(serviceUrl).toHaveAttribute("aria-invalid", "true");
  expect(savedConfiguration).toBeUndefined();
  expect(authenticationRequests).toBe(0);

  await serviceUrl.fill("http://localhost:8096");
  await expect(serviceUrl).toHaveAttribute("aria-invalid", "false");
  await page.getByRole("button", { name: "Connect", exact: true }).click();

  await expect.poll(() => authenticationStartedAfterSave).toBe(true);
  expect(savedConfiguration).toEqual({
    settings: [{ key: "service_url", value: { kind: "text", value: "http://localhost:8096" } }],
    secretPatches: [],
  });
});

function deletionImpact() {
  return {
    connectionId,
    sourceCount: 0,
    workspaceAttachmentCount: 0,
    taskCount: 0,
    activeTaskCount: 0,
    authenticationSessionCount: 0,
  };
}

function mappingRootPolicy(namespaces: string[] | null) {
  return {
    namespaces,
    availableNamespaces: [
      {
        namespace: "example.media",
        candidateCount: 3,
      },
      {
        namespace: "org.themoviedb",
        candidateCount: 2,
      },
    ],
  };
}

function authenticationInspection() {
  return {
    fields: [],
    settings: [],
    configuredSecretKeys: [],
    fieldProblems: [],
    authenticationMethods: [
      {
        key: "fixture_prompt",
        displayName: "Fixture prompt",
        interactions: [],
        fields: [adapterField("account_name", "Account name")],
      },
    ],
  };
}

function configuredAuthenticationInspection() {
  return {
    fields: [adapterField("service_url", "Service URL")],
    settings: [],
    configuredSecretKeys: [],
    fieldProblems: [],
    authenticationMethods: [
      {
        key: "provider/device",
        displayName: "Sign in",
        interactions: [],
        fields: [],
      },
    ],
  };
}

function configurationInspection(revision: string) {
  return {
    fields: [
      {
        ...adapterField("revision", "Catalog revision"),
        defaultValue: { kind: "text", value: "v1" },
        options: [
          { displayName: "Revision v1", value: { kind: "text", value: "v1" } },
          { displayName: "Revision v2", value: { kind: "text", value: "v2" } },
        ],
      },
      { ...adapterField("api_token", "API token"), secret: true },
    ],
    settings: [{ key: "revision", value: { kind: "text", value: revision } }],
    configuredSecretKeys: ["api_token"],
    fieldProblems: [],
    authenticationMethods: [],
  };
}

function inputRequiredAuthentication() {
  return {
    id: authenticationId,
    status: "inputRequired",
    accounts: [],
    error: null,
    retryAfter: null,
    prompt: {
      message: "Enter the fixture verification code.",
      verificationUrl: "",
      userCode: "",
      fields: [adapterField("verification_code", "Verification code")],
    },
  };
}

function waitingAuthentication() {
  return {
    id: authenticationId,
    status: "waiting",
    accounts: [],
    error: null,
    retryAfter: { seconds: 0, nanoseconds: 0 },
    prompt: {
      message: "Complete authentication with the provider.",
      verificationUrl: "",
      userCode: "",
      fields: [],
    },
  };
}

function completedAuthentication() {
  return {
    id: authenticationId,
    status: "completed",
    accounts: [{ key: { namespace: "fixture", value: bytes("account/alex") }, displayName: "Alex" }],
    error: null,
    retryAfter: null,
    prompt: null,
  };
}

function adapterField(key: string, displayName: string) {
  return {
    key,
    displayName,
    valueKind: "text",
    required: true,
    secret: false,
    placeholder: "",
    defaultValue: null,
    options: [],
  };
}
