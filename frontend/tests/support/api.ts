import { expect, type Page, type Route } from "@playwright/test";
import type { TaskRun } from "../../src/lib/api/operations.ts";

export const workspaceId = "11111111-1111-1111-1111-111111111111";
export const connectionId = "22222222-2222-2222-2222-222222222222";
export const sourceId = "33333333-3333-3333-3333-333333333333";
export const taskId = "44444444-4444-4444-4444-444444444444";
export const authenticationId = "77777777-7777-7777-7777-777777777777";
export const timestamp = "2030-01-02T03:04:05Z";
export const providerTarget = "x86_64-unknown-linux-gnu";

export const activeFixtureProviderRelease = {
  repositoryId: "dev.trakkin.community",
  providerId: "dev.trakkin.fixture",
  name: "Fixture",
  summary: "Deterministic provider",
  version: "1.0.0",
  channels: ["development"],
  targets: [providerTarget],
  installedTargets: [providerTarget],
  activeTarget: providerTarget,
};

export const activeFixtureProviderInstallation = {
  id: "88888888-8888-4888-8888-888888888888",
  repositoryId: "dev.trakkin.community",
  providerId: "dev.trakkin.fixture",
  version: "1.0.0",
  target: providerTarget,
  packageSha256: "a".repeat(64),
  active: true,
  generation: "99999999-9999-4999-8999-999999999999",
  installedAt: timestamp,
};

export const workspace = {
  id: workspaceId,
  name: "Fixture workspace",
  status: "active",
  createdAt: timestamp,
  updatedAt: timestamp,
  sources: [{ sourceId, attachedAt: timestamp }],
};

export const connection = {
  id: connectionId,
  providerId: "dev.trakkin.fixture",
  displayName: "Fixture account",
  status: "ready",
  capabilityRevision: 1,
  createdAt: timestamp,
  updatedAt: timestamp,
};

export const source = {
  id: sourceId,
  accountId: "55555555-5555-5555-5555-555555555555",
  accountDisplayName: "Alex",
  displayName: "Fixture Catalog",
  kind: { namespace: "fixture", name: "media" },
  availability: "available",
  capabilityRevision: 1,
};

export async function mockApi(page: Page, handler: (route: Route) => Promise<unknown>) {
  await page.route("**/api/v1/**", (route) => handler(route));
}

export async function json(route: Route, body: unknown, status = 200) {
  await route.fulfill({ status, contentType: "application/json", body: JSON.stringify(body) });
}

export async function fallback(route: Route) {
  const url = new URL(route.request().url());
  if (url.pathname === "/api/v1/providers" && route.request().method() === "GET") {
    return json(route, [activeFixtureProviderRelease]);
  }
  if (url.pathname === "/api/v1/provider-installations" && route.request().method() === "GET") {
    return json(route, [activeFixtureProviderInstallation]);
  }
  await route.fulfill({
    status: 404,
    contentType: "application/json",
    body: JSON.stringify({ code: "not_found", message: "Mock route not found", fieldProblems: [] }),
  });
}

export async function apiError(route: Route, status: number, code: string, message: string) {
  await json(route, { code, message, fieldProblems: [] }, status);
}

export function bytes(value: string): number[] {
  return Array.from(new TextEncoder().encode(value));
}

export function task(status: TaskRun["status"], terminal: boolean): TaskRun {
  return {
    id: taskId,
    connectionId,
    workspaceId,
    sourceId,
    kind: "catalogRead",
    stream: "catalog",
    mode: "full",
    status,
    terminal,
    cancellationSupported: !terminal,
    createdAt: timestamp,
    startedAt: terminal ? timestamp : null,
    finishedAt: terminal ? timestamp : null,
    cancellationRequestedAt: null,
    failure: null,
    updatedAt: timestamp,
  };
}

export async function expectNoHorizontalOverflow(page: Page) {
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
}
