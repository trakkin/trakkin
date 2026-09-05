import { request } from "./client.ts";
import type { components, operations } from "./generated/schema.ts";

export type ProviderSource = components["schemas"]["ProviderSourceResponse"];
export type Provider = components["schemas"]["ProviderResponse"];
export type ProviderUpdatePolicy = components["schemas"]["ProviderUpdatePolicyValue"];
export type TrustProviderSourceInput =
  operations["trust_provider_source"]["requestBody"]["content"]["application/json"];
export type InstallProviderInput = operations["install_provider"]["requestBody"]["content"]["application/json"];
export type UpdateProviderInput = operations["update_provider"]["requestBody"]["content"]["application/json"];
export type ChangeProviderUpdatePolicyInput =
  operations["change_provider_update_policy"]["requestBody"]["content"]["application/json"];

export function listProviderSources(signal?: AbortSignal): Promise<ProviderSource[]> {
  return request("/api/v1/provider-sources", { signal });
}

export function trustProviderSource(input: TrustProviderSourceInput, signal?: AbortSignal): Promise<ProviderSource> {
  return request("/api/v1/provider-sources", { method: "POST", body: input, signal });
}

export function refreshProviderSource(sourceId: string, signal?: AbortSignal): Promise<ProviderSource> {
  return request(`/api/v1/provider-sources/${encodeURIComponent(sourceId)}/refresh`, {
    method: "POST",
    signal,
  });
}

export function removeProviderSource(sourceId: string, signal?: AbortSignal): Promise<void> {
  return request(`/api/v1/provider-sources/${encodeURIComponent(sourceId)}`, { method: "DELETE", signal });
}

export function listProviders(signal?: AbortSignal): Promise<Provider[]> {
  return request("/api/v1/providers", { signal });
}

export function installProvider(input: InstallProviderInput, signal?: AbortSignal): Promise<Provider> {
  return request("/api/v1/providers", { method: "POST", body: input, signal });
}

export function updateProvider(
  providerId: string,
  input: UpdateProviderInput,
  signal?: AbortSignal,
): Promise<Provider> {
  return request(`/api/v1/providers/${encodeURIComponent(providerId)}/update`, {
    method: "POST",
    body: input,
    signal,
  });
}

export function changeProviderUpdatePolicy(
  providerId: string,
  input: ChangeProviderUpdatePolicyInput,
  signal?: AbortSignal,
): Promise<Provider> {
  return request(`/api/v1/providers/${encodeURIComponent(providerId)}/update-policy`, {
    method: "PUT",
    body: input,
    signal,
  });
}

export function removeProvider(providerId: string, signal?: AbortSignal): Promise<void> {
  return request(`/api/v1/providers/${encodeURIComponent(providerId)}`, {
    method: "DELETE",
    signal,
  });
}
