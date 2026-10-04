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

type RequestOptions = Omit<RequestInit, "body"> &
  ({ body?: unknown; rawBody?: never } | { body?: never; rawBody: BodyInit });

export async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const { body, rawBody, headers, ...init } = options;
  const requestHeaders = new Headers(headers);
  if (body !== undefined && !requestHeaders.has("content-type")) {
    requestHeaders.set("content-type", "application/json");
  }
  const response = await fetch(path, {
    ...init,
    body: rawBody ?? (body === undefined ? undefined : JSON.stringify(body)),
    headers: requestHeaders,
  });
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
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
  return payload as T;
}
