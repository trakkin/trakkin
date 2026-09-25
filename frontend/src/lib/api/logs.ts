import { request } from "./client.ts";
import type { components, operations } from "./generated/schema.ts";

export type LogEntry = components["schemas"]["LogEntryResponse"];
export type LogPage = components["schemas"]["LogPageResponse"];

type ListLogsResponse = operations["list_logs"]["responses"][200]["content"]["application/json"];
type ListLogsOptions = NonNullable<operations["list_logs"]["parameters"]["query"]>;

export function listLogs(options: ListLogsOptions = {}, signal?: AbortSignal): Promise<ListLogsResponse> {
  const parameters = new URLSearchParams();
  if (options.before) parameters.set("before", options.before);
  if (options.after) parameters.set("after", options.after);
  if (options.limit) parameters.set("limit", String(options.limit));
  const query = parameters.size > 0 ? `?${parameters.toString()}` : "";
  return request(`/api/v1/logs${query}`, { signal });
}

type LogStreamHandlers = {
  onEntry: (entry: LogEntry) => void;
  onError?: () => void;
  onOpen?: () => void;
  onReset?: () => void;
};

export function streamLogs(after: string | undefined, handlers: LogStreamHandlers): () => void {
  const parameters = new URLSearchParams();
  if (after) parameters.set("after", after);
  const query = parameters.size > 0 ? `?${parameters.toString()}` : "";
  const source = new EventSource(`/api/v1/logs/stream${query}`);

  source.addEventListener("log", (event) => {
    try {
      handlers.onEntry(JSON.parse(event.data) as LogEntry);
    } catch {
      handlers.onReset?.();
    }
  });
  source.addEventListener("reset", () => handlers.onReset?.());
  source.addEventListener("open", () => handlers.onOpen?.());
  source.addEventListener("error", () => handlers.onError?.());

  return () => source.close();
}
