import type { components } from "./generated/schema.ts";

export type FieldProblem = components["schemas"]["FieldProblemResponse"];
export type ApiErrorBody = components["schemas"]["ErrorResponse"];

export class ApiRequestError extends Error {
  readonly status: number;
  readonly code: string;
  readonly fieldProblems: FieldProblem[];
  readonly body: ApiErrorBody;

  constructor(status: number, body: ApiErrorBody) {
    super(body.message);
    this.name = "ApiRequestError";
    this.status = status;
    this.code = body.code;
    this.fieldProblems = body.fieldProblems;
    this.body = body;
  }
}

type RequestOptions = Omit<RequestInit, "body"> & { body?: unknown };

export async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const { body, headers, ...init } = options;
  const requestHeaders = new Headers(headers);
  if (body !== undefined && !requestHeaders.has("content-type")) {
    requestHeaders.set("content-type", "application/json");
  }
  const response = await fetch(path, {
    ...init,
    body: body === undefined ? undefined : JSON.stringify(body),
    headers: requestHeaders,
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => null);
    const body: ApiErrorBody =
      payload && typeof payload === "object" && "code" in payload
        ? (payload as ApiErrorBody)
        : {
            code: "unexpected_response",
            message: `Request failed with status ${response.status}`,
            fieldProblems: [],
          };
    throw new ApiRequestError(response.status, body);
  }
  return (await response.json().catch(() => null)) as T;
}
