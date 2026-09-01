import { request } from "./client.ts";
import type { components, operations } from "./generated/schema.ts";

export type ProviderRepository = components["schemas"]["ProviderRepositoryResponse"];
export type ProviderRelease = components["schemas"]["ProviderReleaseResponse"];
export type ProviderInstallation = components["schemas"]["ProviderInstallationResponse"];
export type RegisterProviderRepositoryInput =
  operations["register_repository"]["requestBody"]["content"]["application/json"];
export type InstallProviderInput = operations["install_provider"]["requestBody"]["content"]["application/json"];

export function listProviderRepositories(signal?: AbortSignal): Promise<ProviderRepository[]> {
  return request("/api/v1/provider-repositories", { signal });
}

export function registerProviderRepository(
  input: RegisterProviderRepositoryInput,
  signal?: AbortSignal,
): Promise<ProviderRepository> {
  return request("/api/v1/provider-repositories", { method: "POST", body: input, signal });
}

export function refreshProviderRepository(repositoryId: string, signal?: AbortSignal): Promise<ProviderRepository> {
  return request(`/api/v1/provider-repositories/${encodeURIComponent(repositoryId)}/refresh`, {
    method: "POST",
    signal,
  });
}

export function listProviderReleases(signal?: AbortSignal): Promise<ProviderRelease[]> {
  return request("/api/v1/providers", { signal });
}

export function listProviderInstallations(signal?: AbortSignal): Promise<ProviderInstallation[]> {
  return request("/api/v1/provider-installations", { signal });
}

export function installProvider(input: InstallProviderInput, signal?: AbortSignal): Promise<ProviderInstallation> {
  return request("/api/v1/provider-installations", { method: "POST", body: input, signal });
}

export function activateProvider(
  providerId: string,
  installationId: string,
  signal?: AbortSignal,
): Promise<ProviderInstallation> {
  return request(`/api/v1/providers/${encodeURIComponent(providerId)}/activation`, {
    method: "PUT",
    body: { installationId },
    signal,
  });
}

export function removeProviderInstallation(installationId: string, signal?: AbortSignal): Promise<void> {
  return request(`/api/v1/provider-installations/${encodeURIComponent(installationId)}`, {
    method: "DELETE",
    signal,
  });
}
