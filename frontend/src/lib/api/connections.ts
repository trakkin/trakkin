import { request } from "./client.ts";
import type { components, operations } from "./generated/schema.ts";

export type AdapterField = components["schemas"]["AdapterConfigurationFieldResponse"];
export type AdapterSetting = components["schemas"]["AdapterSettingRequest"];
export type AdapterValue = components["schemas"]["AdapterValueRequest"];
export type AdapterValueDisplay = components["schemas"]["AdapterValueResponse"];
export type AuthenticationMethod = components["schemas"]["AuthenticationMethodResponse"];
export type AuthenticationStep = components["schemas"]["AuthenticationStepResponse"];
export type Connection = components["schemas"]["ConnectionResponse"];
export type ConnectionDeletionImpact = components["schemas"]["ConnectionDeletionImpactResponse"];
export type ConnectionInspection = components["schemas"]["ConnectionInspectionResponse"];
export type ConnectionSource = components["schemas"]["ConnectionSourceResponse"];
export type ConnectionTask = components["schemas"]["TaskResponse"];

export type CreateConnectionInput = operations["create_connection"]["requestBody"]["content"]["application/json"];
export type ReconfigureConnectionInput =
  operations["reconfigure_connection"]["requestBody"]["content"]["application/json"];
type RenameConnectionRequest = operations["rename_connection"]["requestBody"]["content"]["application/json"];
type StartAuthenticationRequest = operations["start_authentication"]["requestBody"]["content"]["application/json"];
type ContinueAuthenticationRequest =
  operations["continue_authentication"]["requestBody"]["content"]["application/json"];
type ListConnectionsResponse = operations["list_connections"]["responses"][200]["content"]["application/json"];

export function listConnections(signal?: AbortSignal): Promise<ListConnectionsResponse> {
  return request("/api/v1/connections", { signal });
}

export function createConnection(input: CreateConnectionInput, signal?: AbortSignal): Promise<Connection> {
  return request("/api/v1/connections", { method: "POST", body: input, signal });
}

export function renameConnection(connectionId: string, displayName: string, signal?: AbortSignal): Promise<Connection> {
  const body: RenameConnectionRequest = { displayName };
  return request(`/api/v1/connections/${connectionId}`, { method: "PATCH", body, signal });
}

export function reconfigureConnection(
  connectionId: string,
  input: ReconfigureConnectionInput,
  signal?: AbortSignal,
): Promise<Connection> {
  return request(`/api/v1/connections/${connectionId}/configuration`, { method: "PUT", body: input, signal });
}

export function getConnectionDeletionImpact(
  connectionId: string,
  signal?: AbortSignal,
): Promise<ConnectionDeletionImpact> {
  return request(`/api/v1/connections/${connectionId}/deletion-impact`, { signal });
}

export function deleteConnection(connectionId: string, signal?: AbortSignal): Promise<ConnectionDeletionImpact> {
  return request(`/api/v1/connections/${connectionId}/deletion`, {
    method: "POST",
    body: { confirmed: true },
    signal,
  });
}

export function inspectConnection(connectionId: string, signal?: AbortSignal): Promise<ConnectionInspection> {
  return request(`/api/v1/connections/${connectionId}/inspection`, { signal });
}

export function startConnectionAuthentication(
  connectionId: string,
  method: string,
  values: AdapterSetting[],
  signal?: AbortSignal,
): Promise<AuthenticationStep> {
  const body: StartAuthenticationRequest = { method, values };
  return request(`/api/v1/connections/${connectionId}/authentications`, { method: "POST", body, signal });
}

export function continueConnectionAuthentication(
  connectionId: string,
  authenticationId: string,
  values: AdapterSetting[],
  signal?: AbortSignal,
): Promise<AuthenticationStep> {
  const body: ContinueAuthenticationRequest = { values };
  return request(`/api/v1/connections/${connectionId}/authentications/${authenticationId}/continuation`, {
    method: "POST",
    body,
    signal,
  });
}

export function cancelConnectionAuthentication(
  connectionId: string,
  authenticationId: string,
  signal?: AbortSignal,
): Promise<AuthenticationStep> {
  return request(`/api/v1/connections/${connectionId}/authentications/${authenticationId}/cancellation`, {
    method: "POST",
    signal,
  });
}

export function listConnectionSources(connectionId: string, signal?: AbortSignal): Promise<ConnectionSource[]> {
  return request(`/api/v1/connections/${connectionId}/sources`, { signal });
}

export async function discoverConnectionSources(
  connectionId: string,
  signal?: AbortSignal,
): Promise<ConnectionSource[]> {
  let task = await request<ConnectionTask>(`/api/v1/connections/${connectionId}/discovery-tasks`, {
    method: "POST",
    signal,
  });
  while (!task.terminal) {
    await delay(250, signal);
    task = await request(`/api/v1/connections/${connectionId}/tasks/${task.id}`, { signal });
  }
  if (task.status !== "succeeded") {
    throw new Error(
      task.status === "cancelled"
        ? "Source discovery was cancelled."
        : "Sources could not be discovered. Check the connection and try again.",
    );
  }
  return listConnectionSources(connectionId, signal);
}

export function retryAfterMilliseconds(step: AuthenticationStep): number {
  if (!step.retryAfter) return 1_500;
  return Math.max(200, step.retryAfter.seconds * 1_000 + Math.ceil(step.retryAfter.nanoseconds / 1_000_000));
}

function delay(milliseconds: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason);
      return;
    }
    const timeout = window.setTimeout(resolve, milliseconds);
    signal?.addEventListener(
      "abort",
      () => {
        window.clearTimeout(timeout);
        reject(signal.reason);
      },
      { once: true },
    );
  });
}
