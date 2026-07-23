import { writable } from "svelte/store";

export type ApiStatus = "connecting" | "online" | "unavailable";
export type ApiHealth = { contract: "available" };

export const apiStatus = writable<ApiStatus>("connecting");

export async function status(): Promise<ApiHealth> {
  const response = await fetch("/api/v1/openapi.json");
  if (!response.ok) throw new Error("API unavailable");
  return { contract: "available" };
}
