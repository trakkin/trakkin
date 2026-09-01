import { expect, test } from "@playwright/test";
import {
  apiError,
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
  taskId,
  timestamp,
  workspace,
  workspaceId,
} from "./support/api.ts";
import { rootCatalog } from "./support/catalog.ts";

test("starts synchronization from a stored policy preview", async ({ page }) => {
  const firstPolicyId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
  const selectedPolicyId = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
  const createdPolicyId = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
  const automationSource = {
    ...source,
    displayName: "Household episodic media library with an intentionally long name",
  };
  const previewId = "88888888-8888-4888-8888-888888888888";
  const runId = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
  const actionId = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff";
  const partialRunId = "cccccccc-dddd-4eee-8fff-aaaaaaaaaaaa";
  const partialActionId = "dddddddd-eeee-4fff-8aaa-bbbbbbbbbbbb";
  const cancelledRunId = "eeeeeeee-ffff-4aaa-8bbb-cccccccccccc";
  const cancelledActionId = "ffffffff-aaaa-4bbb-8ccc-dddddddddddd";
  const previewHash = `sha256:${"a".repeat(64)}`;
  const actionHash = `sha256:${"b".repeat(64)}`;
  let runAttempts = 0;
  let activityRequests = 0;
  let cancellationRequests = 0;
  let taskCancelled = false;
  let policyCreationRequests = 0;
  let releasePolicyCreation: () => void = () => {};
  const policyCreationGate = new Promise<void>((resolve) => {
    releasePolicyCreation = resolve;
  });
  let releaseCancellationCompletion: () => void = () => {};
  const cancellationCompletionGate = new Promise<void>((resolve) => {
    releaseCancellationCompletion = resolve;
  });
  let scheduledPolicyId = "";
  let scheduleState: "enabled" | "paused" | null = null;
  let scheduleIntervalSeconds = 0;
  let scheduleUpdates: number[] = [];
  let schedulePauseRequests = 0;
  let scheduleReadFailures = 1;

  function scheduleResponse(policyId: string) {
    return {
      scheduleId: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
      workspaceId,
      policyId,
      intervalSeconds: scheduleIntervalSeconds,
      state: scheduleState,
      nextRunAt: scheduleState === "enabled" ? timestamp : null,
      createdAt: timestamp,
      updatedAt: timestamp,
    };
  }

  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    if (url.pathname === `/api/v1/workspaces/${workspaceId}`) return json(route, workspace);
    if (url.pathname === "/api/v1/connections") return json(route, [connection]);
    if (url.pathname === `/api/v1/connections/${connectionId}/sources`) return json(route, [automationSource]);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks`) return json(route, []);
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/sources/${sourceId}/catalog`) {
      return json(route, rootCatalog());
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/sync-policies` && method === "GET") {
      return json(route, {
        policies: [
          synchronizationPolicy(firstPolicyId, "trakkin.state:watched"),
          synchronizationPolicy(selectedPolicyId),
        ],
        nextCursor: null,
      });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/sync-policies` && method === "POST") {
      policyCreationRequests += 1;
      await policyCreationGate;
      return json(route, synchronizationPolicy(createdPolicyId), 201);
    }
    const schedulePath = url.pathname.match(
      new RegExp(`^/api/v1/workspaces/${workspaceId}/sync-policies/([^/]+)/schedule$`),
    );
    if (schedulePath && method === "GET") {
      const policyId = schedulePath[1] ?? "";
      if (policyId === selectedPolicyId && scheduleReadFailures > 0) {
        scheduleReadFailures -= 1;
        return apiError(route, 503, "sync_schedule_unavailable", "Recurring schedule is temporarily unavailable");
      }
      if (policyId !== scheduledPolicyId || scheduleState === null) {
        return apiError(route, 404, "sync_schedule_not_found", "Synchronization schedule was not found");
      }
      return json(route, scheduleResponse(policyId));
    }
    if (schedulePath && method === "PUT") {
      const policyId = schedulePath[1] ?? "";
      const body = route.request().postDataJSON() as { intervalSeconds: number };
      scheduledPolicyId = policyId;
      scheduleState = "enabled";
      scheduleIntervalSeconds = body.intervalSeconds;
      scheduleUpdates = [...scheduleUpdates, body.intervalSeconds];
      return json(route, scheduleResponse(policyId));
    }
    const pauseSchedulePath = url.pathname.match(
      new RegExp(`^/api/v1/workspaces/${workspaceId}/sync-policies/([^/]+)/schedule/pause$`),
    );
    if (pauseSchedulePath && method === "POST") {
      const policyId = pauseSchedulePath[1] ?? "";
      schedulePauseRequests += 1;
      scheduleState = "paused";
      return json(route, scheduleResponse(policyId));
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/sync-previews` && method === "POST") {
      return json(
        route,
        {
          previewId,
          workspaceId,
          policyId: selectedPolicyId,
          policyRevision: 1,
          previewHash,
          counts: { proposed: 1, noOp: 0, findings: 0 },
          entries: [
            {
              actionHash,
              ordinal: 0,
              category: "proposed",
              semanticField: "trakkin.state:progress",
              receiverSourceId: sourceId,
              receiverSubject: {
                kind: "providerItem",
                key: { namespace: "fixture", value: bytes("media/northstar") },
              },
              contributorSourceIds: [sourceId],
              segmentCoordinate: "segment",
              segmentStart: 1,
              segmentEnd: 12,
              before: 3,
              intended: 6,
              lossy: false,
              membershipEffect: "unchanged",
              finding: null,
            },
          ],
          createdAt: timestamp,
          expiresAt: timestamp,
          freshUntil: timestamp,
        },
        201,
      );
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/sync-previews/${previewId}/runs`) {
      runAttempts += 1;
      if (runAttempts === 1) {
        return apiError(route, 409, "sync_preview_stale", "Synchronization preview is stale");
      }
      return json(route, { runId }, 201);
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/activity`) {
      activityRequests += 1;
      if (url.searchParams.get("cursor") === "activity-next") {
        return json(route, {
          runs: [
            activityRun(cancelledRunId, "cancelled", [
              activityAction(cancelledActionId, cancelledRunId, "cancelled", "cancelled"),
            ]),
          ],
          nextCursor: null,
        });
      }
      if (url.searchParams.get("status") === "partial") {
        return json(route, {
          runs: [
            activityRun(partialRunId, "partial", [activityAction(partialActionId, partialRunId, "failed", "failed")]),
          ],
          nextCursor: null,
        });
      }
      return json(route, {
        runs: [
          activityRun(runId, taskCancelled ? "cancelled" : "running", [
            activityAction(
              actionId,
              runId,
              taskCancelled ? "cancelled" : "running",
              taskCancelled ? "cancelled" : "running",
            ),
          ]),
          activityRun(partialRunId, "partial", [activityAction(partialActionId, partialRunId, "failed", "failed")]),
        ],
        nextCursor: "activity-next",
      });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/activity/actions/${actionId}`) {
      return json(route, activityActionDetail(actionId, runId, actionHash, previewId, previewHash));
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks/${taskId}/cancellation`) {
      cancellationRequests += 1;
      await cancellationCompletionGate;
      taskCancelled = true;
      return json(route, {
        ...task("cancelling", false),
        cancellationSupported: false,
        kind: "syncAction",
        mode: null,
        stream: null,
      });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks/${taskId}`) {
      return json(route, {
        ...task(taskCancelled ? "cancelling" : "running", false),
        cancellationSupported: !taskCancelled,
        kind: "syncAction",
        mode: null,
        stream: null,
      });
    }
    return fallback(route);
  });

  await page.goto(`/w/${workspaceId}/catalog`);
  if ((page.viewportSize()?.width ?? 0) < 1024) {
    const navigationToggle = page.getByRole("button", { name: "Open navigation" });
    await expect(navigationToggle).toBeVisible();
    await navigationToggle.click();
    await page.getByRole("menuitem", { name: "Automation" }).click();
  } else {
    await page.getByRole("link", { name: "Automation" }).click();
  }
  await expect(page).toHaveURL(new RegExp(`/w/${workspaceId}/automation$`));
  await expect(page.getByRole("heading", { name: "Automation" })).toBeVisible();
  if ((page.viewportSize()?.width ?? 0) < 1024) {
    await expect(page.getByLabel("Stored synchronization policy")).toBeVisible();
  } else {
    await expect(page.getByRole("button", { name: /Select Policy aaaaaaaa/ })).toHaveAttribute("aria-current", "true");
  }
  await expect(page.getByRole("heading", { name: "Policy aaaaaaaa" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Recurring schedule" })).toBeVisible();
  await expect(page.getByText("Not configured", { exact: true })).toBeVisible();
  await expectNoHorizontalOverflow(page);
  await page.getByRole("button", { name: "New policy" }).click();
  await expect(page.getByRole("heading", { name: "Create sync policy" })).toBeVisible();
  await expect(page.getByText(automationSource.displayName).first()).toBeVisible();
  await expectNoHorizontalOverflow(page);
  await page.getByLabel(`${automationSource.displayName} contributes`).click();
  await page.getByLabel(`${automationSource.displayName} receives`).click();
  await page.getByLabel("Synchronize Progress").click();
  await page.getByRole("button", { name: "Create policy" }).click();
  await expect.poll(() => policyCreationRequests).toBe(1);
  await page.keyboard.press("Escape");
  await expect(page.getByRole("heading", { name: "Create sync policy" })).not.toBeVisible();
  await page.getByRole("button", { name: "New policy" }).click();
  await expect(page.getByRole("heading", { name: "Create sync policy" })).toBeVisible();
  await page.getByLabel("Synchronize Rating").click();
  const policyCreationResponse = page.waitForResponse(
    (response) =>
      new URL(response.url()).pathname === `/api/v1/workspaces/${workspaceId}/sync-policies` &&
      response.request().method() === "POST",
  );
  releasePolicyCreation();
  await policyCreationResponse;
  await page.evaluate(
    () => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))),
  );
  await expect(page.getByRole("heading", { name: "Create sync policy" })).toBeVisible();
  await expect(page.getByLabel("Synchronize Rating")).toBeChecked();
  await expect(page.getByRole("button", { name: "Create policy" })).toBeEnabled();
  await page.getByLabel("Synchronize Rating").click();
  await page.getByRole("button", { name: "Create policy" }).click();
  await expect.poll(() => policyCreationRequests).toBe(2);
  await expect(page.getByRole("heading", { name: "Create sync policy" })).not.toBeVisible();
  if ((page.viewportSize()?.width ?? 0) < 1024) {
    await page.getByLabel("Stored synchronization policy").click();
    await page.getByRole("option", { name: /Policy bbbbbbbb/ }).click();
  } else {
    const selectedPolicy = page.getByRole("button", { name: /Select Policy bbbbbbbb/ });
    await selectedPolicy.click();
    await expect(selectedPolicy).toHaveAttribute("aria-current", "true");
  }
  await expect(page.getByRole("heading", { name: "Policy bbbbbbbb" })).toBeVisible();
  await expect(page.getByText(automationSource.displayName).first()).toBeVisible();
  await expect(page.getByRole("heading", { name: "Recurring schedule" })).toBeVisible();
  const scheduleError = page.getByRole("alert").filter({ hasText: "Recurring schedule is temporarily unavailable" });
  await expect(scheduleError).toBeVisible();
  await scheduleError.getByRole("button", { name: "Retry" }).click();
  await expect(page.getByText("Not configured", { exact: true })).toBeVisible();
  const recurringScheduleToggle = page.getByRole("switch", { name: "Recurring synchronization" });
  await page.getByLabel("Schedule interval in minutes").fill("90");
  await recurringScheduleToggle.click();
  await expect.poll(() => scheduleUpdates.at(-1)).toBe(5_400);
  await expect(recurringScheduleToggle).toBeChecked();
  await page.getByLabel("Schedule interval in minutes").fill("120");
  await page.getByRole("button", { name: "Save interval" }).click();
  await expect.poll(() => scheduleUpdates.at(-1)).toBe(7_200);
  await recurringScheduleToggle.click();
  await expect.poll(() => schedulePauseRequests).toBe(1);
  await expect(recurringScheduleToggle).not.toBeChecked();
  await expectNoHorizontalOverflow(page);
  await page.getByRole("button", { name: "Generate preview" }).click();

  await expect(page.getByRole("heading", { name: "Sync preview" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Progress" })).toBeVisible();
  await expectNoHorizontalOverflow(page);

  await page.getByRole("button", { name: "Continue" }).click();
  await page.getByRole("button", { name: "Confirm and start" }).click();
  await expect(page.getByRole("alert")).toContainText("changed or expired");
  await page.getByRole("button", { name: "Retry" }).click();
  await expect(page.getByRole("heading", { name: "Progress" })).toBeVisible();
  await page.getByRole("button", { name: "Continue" }).click();
  await page.getByRole("button", { name: "Confirm and start" }).click();
  await expect(page.getByRole("heading", { name: "Updates queued" })).toBeVisible();
  await expect(page.getByText("1 receiver update ready for execution")).toBeVisible();
  expect(runAttempts).toBe(2);
  await expectNoHorizontalOverflow(page);

  await page.getByRole("button", { name: "View activity" }).click();
  await expect(page).toHaveURL(new RegExp(`/w/${workspaceId}/activity\\?runId=${runId}$`));
  await expect(page.getByRole("heading", { name: "Activity" })).toBeVisible();
  const admittedRun = page.getByRole("article", { name: `Synchronization run ${runId}` });
  await expect(admittedRun).toHaveAttribute("aria-current", "true");
  await expect(admittedRun.getByText("Just queued", { exact: true })).toBeVisible();
  await expect(page.getByText("running", { exact: true }).first()).toBeVisible();
  await expect(page.getByText("partial", { exact: true }).first()).toBeVisible();

  await page.getByRole("button", { name: "Next" }).click();
  await expect(page.getByText("Page 2")).toBeVisible();
  await expect(page.getByText("cancelled", { exact: true }).first()).toBeVisible();
  await page.getByRole("button", { name: "Previous" }).click();

  await page.getByLabel("Activity run status").click();
  await page.getByRole("option", { name: "Partial", exact: true }).click();
  await page.getByLabel("Receiver source ID").fill(sourceId);
  await page.getByLabel("Activity state field").click();
  await page.getByRole("option", { name: "Progress", exact: true }).click();
  await page.getByLabel("Activity from").fill("2030-01-01T00:00");
  await page.getByLabel("Activity before").fill("2030-02-01T00:00");
  await page.getByRole("button", { name: "Apply" }).click();
  await expect(page.getByText("failed", { exact: true }).first()).toBeVisible();

  await page.getByLabel("Reset activity filters").click();
  await page.getByLabel(new RegExp(`Inspect Progress action ${actionId}`)).click();
  await expect(page.getByRole("heading", { name: "Sync action detail" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Decision chain" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Contributors" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Mapping proofs" })).toBeVisible();
  await page.getByRole("button", { name: "Cancel" }).click();
  await expect.poll(() => cancellationRequests).toBe(1);
  await page.keyboard.press("Escape");
  await expect(page.getByRole("heading", { name: "Sync action detail" })).not.toBeVisible();
  await page.getByLabel(new RegExp(`Inspect Progress action ${actionId}`)).click();
  await expect(page.getByRole("heading", { name: "Sync action detail" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Cancel" })).toBeEnabled();
  const activityRequestsBeforeCancellationCompletion = activityRequests;
  const cancellationCompletionResponse = page.waitForResponse(
    (response) =>
      new URL(response.url()).pathname === `/api/v1/workspaces/${workspaceId}/tasks/${taskId}/cancellation` &&
      response.request().method() === "POST",
  );
  releaseCancellationCompletion();
  await cancellationCompletionResponse;
  await page.evaluate(
    () => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))),
  );
  expect(activityRequests).toBe(activityRequestsBeforeCancellationCompletion);
  await expect(page.getByRole("heading", { name: "Sync action detail" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Cancel" })).toBeEnabled();
  await expect(page.getByText("cancelling", { exact: true })).not.toBeVisible();
  await page.getByRole("button", { name: "Cancel" }).click();
  await expect.poll(() => cancellationRequests).toBe(2);
  await expect(page.getByText("cancelling", { exact: true })).toBeVisible();
  await expectNoHorizontalOverflow(page);
});

test("automation retries its initial load and explains missing sources", async ({ page }) => {
  const emptyWorkspace = { ...workspace, sources: [] };
  let connectionRequests = 0;

  await mockApi(page, async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === `/api/v1/workspaces/${workspaceId}`) return json(route, emptyWorkspace);
    if (url.pathname === "/api/v1/connections") {
      connectionRequests += 1;
      if (connectionRequests === 1) {
        return apiError(route, 503, "connections_unavailable", "Connection directory is temporarily unavailable");
      }
      return json(route, []);
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/sync-policies`) {
      return json(route, { policies: [], nextCursor: null });
    }
    if (url.pathname === `/api/v1/workspaces/${workspaceId}/tasks`) return json(route, []);
    return fallback(route);
  });

  await page.goto(`/w/${workspaceId}/automation`);
  const loadError = page.getByRole("alert").filter({ hasText: "Connection directory is temporarily unavailable" });
  await expect(loadError).toBeVisible();
  await loadError.getByRole("button", { name: "Retry" }).click();

  await expect(page.getByRole("heading", { name: "No sources attached" })).toBeVisible();
  await expect(
    page.getByText("Attach a source in Connections before creating a synchronization policy."),
  ).toBeVisible();
  await expect(page.getByRole("button", { name: "New policy" })).toHaveCount(0);
  await expect.poll(() => connectionRequests).toBe(2);
  await expectNoHorizontalOverflow(page);
});

function synchronizationPolicy(policyId: string, semanticField = "trakkin.state:progress") {
  return {
    policyId,
    workspaceId,
    revision: 1,
    contentHash: `sha256:${"c".repeat(64)}`,
    createdAt: timestamp,
    document: {
      schema: "trakkin.sync-policy/v1",
      contributors: [sourceId],
      receivers: [sourceId],
      resolutionMode: "direct",
      rules: [
        {
          semanticField,
          reducer: semanticField === "trakkin.state:progress" ? "coverage_contiguous_prefix" : "any",
          conflict: "require_reducer",
          presence: { absent: "ignore", deleted: "ignore", stale: "block", unknown: "block" },
          translation: { allowLossy: false, quantizer: "exact" },
          writeConstraints: { allowClear: false, allowCreateMembership: true, requireStrongPrecondition: true },
        },
      ],
    },
  };
}

function activityRun(runId: string, status: string, actions: ReturnType<typeof activityAction>[]) {
  return {
    runId,
    workspaceId,
    previewId: "88888888-8888-4888-8888-888888888888",
    status,
    actions,
    createdAt: timestamp,
    startedAt: timestamp,
    finishedAt: ["running", "queued"].includes(status) ? null : timestamp,
    updatedAt: timestamp,
  };
}

function activityAction(actionId: string, runId: string, status: string, taskStatus: string) {
  return {
    actionId,
    runId,
    workspaceId,
    actionHash: `sha256:${"b".repeat(64)}`,
    receiverSourceId: sourceId,
    semanticField: "trakkin.state:progress",
    status,
    writeTask: activityTask(taskId, taskStatus),
    reconciliationTask: null,
    createdAt: timestamp,
    startedAt: timestamp,
    finishedAt: ["running", "queued"].includes(status) ? null : timestamp,
    updatedAt: timestamp,
  };
}

function activityTask(activityTaskId: string, status: string) {
  return {
    taskId: activityTaskId,
    status,
    createdAt: timestamp,
    startedAt: timestamp,
    finishedAt: ["running", "queued", "cancelling"].includes(status) ? null : timestamp,
    updatedAt: timestamp,
  };
}

function activityActionDetail(
  actionId: string,
  runId: string,
  actionHash: string,
  previewId: string,
  previewHash: string,
) {
  return {
    action: activityAction(actionId, runId, "running", "running"),
    reconciliationGuidance: "not_required",
    provenance: {
      policyId: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
      policyRevision: 1,
      policyContentHash: `sha256:${"c".repeat(64)}`,
      stateRegistryDigest: `sha256:${"d".repeat(64)}`,
      previewId,
      previewHash,
      previewActionHash: actionHash,
      runId,
      runCreatedAt: timestamp,
      contributors: [
        {
          evidenceId: "state-evidence-r1",
          statePublicationId: "state-publication-r1",
          presence: "present",
          mappingProofKey: "mapping-proof-r1",
        },
      ],
      mappingProofs: [
        {
          key: "mapping-proof-r1",
          revisionHash: `sha256:${"e".repeat(64)}`,
          rowHash: `sha256:${"f".repeat(64)}`,
        },
      ],
      reducer: "coverage_contiguous_prefix",
      translation: {
        exactValueNumerator: 6,
        exactValueDenominator: 1,
        intendedValue: 6,
        unit: "segment",
        quantizer: "exact",
        lossy: false,
      },
      membershipEffect: "unchanged",
      attempt: {
        taskAttemptId: "task-attempt-r1",
        taskId,
        receiverSourceId: sourceId,
        sourceKey: { namespace: "fixture", value: bytes("source/catalog") },
        receiverSubject: {
          kind: "providerItem",
          key: { namespace: "fixture", value: bytes("media/northstar") },
        },
        semanticField: "trakkin.state:progress",
        providerField: {
          field: { namespace: "fixture.state", name: "progress" },
          unit: { namespace: "fixture.unit", name: "segment" },
        },
        intendedValue: 6,
        expectedMembership: "present",
        allowCreateMembership: true,
        idempotencyKey: "sync-action-r1",
        operationId: "sync-operation-r1",
        state: "dispatched",
        preparedAt: timestamp,
        dispatchedAt: timestamp,
        settledAt: null,
        recoveryResult: null,
        outcome: null,
      },
      reconciliation: null,
      recordedAt: timestamp,
    },
  };
}
