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
  const response = await fetch(path, {
    ...options,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
    headers: {
      ...(options.body === undefined ? {} : { "content-type": "application/json" }),
      ...options.headers,
    },
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
