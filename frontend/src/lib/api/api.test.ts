import assert from "node:assert/strict";
import { test } from "node:test";
import { ApiRequestError, request } from "./client.ts";
import { discoverConnectionSources } from "./connections.ts";
import { deleteMappingSource, queryMappings, refreshMappingSource, saveMappingSource } from "./mappings.ts";
import { getTask, resetAndRefetch, waitForTask } from "./operations.ts";
import { abortableDelay, pollUntilTerminal } from "./taskPolling.ts";

test("mapping queries preserve fingerprint binding, filters, cursor, and signal", async (context) => {
  const controller = new AbortController();
  const state = { fingerprint: "fingerprint", items: [], nextCursor: null };
  context.mock.method(globalThis, "fetch", async (path: string, options: RequestInit) => {
    const url = new URL(path, "http://localhost");
    assert.equal(url.pathname, "/api/v1/mappings");
    assert.equal(url.searchParams.get("expectedFingerprint"), "fingerprint");
    assert.equal(url.searchParams.get("search"), "title & date");
    assert.equal(url.searchParams.get("cursor"), "cursor");
    assert.equal(options.body, undefined);
    assert.equal(options.signal, controller.signal);
    assert.equal("rawBody" in options, false);
    return Response.json(state);
  });
  assert.deepEqual(
    await queryMappings(
      { expectedFingerprint: "fingerprint", search: "title & date", cursor: "cursor" },
      controller.signal,
    ),
    state,
  );
});

test("JSON bodies retain defaults and Headers or tuple overrides", async (context) => {
  const calls: RequestInit[] = [];
  context.mock.method(globalThis, "fetch", async (_path: string, options: RequestInit) => {
    calls.push(options);
    return Response.json(null);
  });
  await request("/json", { body: { enabled: true } });
  await request("/headers", { body: {}, headers: new Headers({ "Content-Type": "custom/type" }) });
  await request("/tuples", { body: {}, headers: [["Content-Type", "tuple/type"]] });
  await request("/get");
  assert.equal(calls[0].body, '{"enabled":true}');
  assert.equal(new Headers(calls[0].headers).get("content-type"), "application/json");
  assert.equal(new Headers(calls[1].headers).get("content-type"), "custom/type");
  assert.equal(new Headers(calls[2].headers).get("content-type"), "tuple/type");
  assert.equal(calls[3].body, undefined);
  assert.equal(new Headers(calls[3].headers).has("content-type"), false);
});

test("JSON requests use structured errors and non-JSON error fallbacks", async (context) => {
  const problem = { field: "record", code: "invalid", message: "Invalid record" };
  context.mock.method(globalThis, "fetch", async () =>
    Response.json({ code: "invalid_mapping", message: "Bad mapping", fieldProblems: [problem] }, { status: 422 }),
  );
  await assert.rejects(request("/json", { body: "invalid" }), (error: unknown) => {
    assert.ok(error instanceof ApiRequestError);
    assert.equal(error.status, 422);
    assert.equal(error.code, "invalid_mapping");
    assert.equal(error.message, "Bad mapping");
    assert.deepEqual(error.fieldProblems, [problem]);
    return true;
  });
  context.mock.method(globalThis, "fetch", async () => new Response("Unavailable", { status: 503 }));
  await assert.rejects(request("/json", { body: "content" }), (error: unknown) => {
    assert.ok(error instanceof ApiRequestError);
    assert.equal(error.status, 503);
    assert.equal(error.code, "unexpected_response");
    assert.equal(error.message, "Request failed with status 503");
    assert.deepEqual(error.fieldProblems, []);
    return true;
  });
});

test("Git source commands preserve branch, fingerprint, encoded identity, and signal", async (context) => {
  const controller = new AbortController();
  const body = {
    expectedFingerprint: "revision & claim",
    displayName: "Community",
    url: "https://example.com/mappings.git",
    branch: "release/v1",
    pollIntervalSeconds: 3600,
    enabled: true,
  };
  const calls: { path: string; method: string | undefined; body: unknown }[] = [];
  context.mock.method(globalThis, "fetch", async (path: string, options: RequestInit) => {
    assert.equal(options.signal, controller.signal);
    assert.equal(new Headers(options.headers).get("content-type"), "application/json");
    calls.push({ path, method: options.method, body: JSON.parse(options.body as string) });
    return Response.json({ fingerprint: "next" });
  });
  await saveMappingSource(body, "personal/source", controller.signal);
  await refreshMappingSource("local", body.expectedFingerprint, controller.signal);
  await deleteMappingSource("personal/source", body.expectedFingerprint, controller.signal);
  assert.deepEqual(calls, [
    { path: "/api/v1/mappings/sources/personal%2Fsource", method: "PUT", body },
    {
      path: "/api/v1/mappings/sources/local/refresh",
      method: "POST",
      body: { expectedFingerprint: body.expectedFingerprint },
    },
    {
      path: "/api/v1/mappings/sources/personal%2Fsource",
      method: "DELETE",
      body: { expectedFingerprint: body.expectedFingerprint },
    },
  ]);
  context.mock.method(globalThis, "fetch", async () =>
    Response.json({ code: "mappings_changed", message: "Changed", fieldProblems: [] }, { status: 409 }),
  );
  await assert.rejects(
    refreshMappingSource("local", "outdated"),
    (error: unknown) => error instanceof ApiRequestError && error.code === "mappings_changed",
  );
});

test("delay removes the same abort listener after every normal completion", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const controller = new AbortController();
  const add = context.mock.method(controller.signal, "addEventListener");
  const remove = context.mock.method(controller.signal, "removeEventListener");
  for (let iteration = 0; iteration < 3; iteration += 1) {
    const pending = abortableDelay(250, controller.signal);
    context.mock.timers.tick(250);
    await pending;
    assert.equal(remove.mock.callCount(), iteration + 1);
    assert.equal(remove.mock.calls[iteration].arguments[1], add.mock.calls[iteration].arguments[1]);
  }
  controller.abort();
});

test("abort during delay clears its timer and listener and stops polling", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const controller = new AbortController();
  const reason = new Error("Stopped");
  const clear = context.mock.method(globalThis, "clearTimeout");
  const remove = context.mock.method(controller.signal, "removeEventListener");
  let requests = 0;
  const pending = pollUntilTerminal<{ id: string; terminal: boolean }>(
    { id: "task", terminal: false },
    async () => {
      requests += 1;
      return { id: "task", terminal: true };
    },
    controller.signal,
  );
  const rejected = assert.rejects(pending, (error: unknown) => error === reason);
  controller.abort(reason);
  await rejected;
  context.mock.timers.tick(250);
  assert.equal(clear.mock.callCount(), 1);
  assert.equal(remove.mock.callCount(), 1);
  assert.equal(requests, 0);
});

test("pre-aborted delay rejects without registering listeners; terminal tasks do not poll", async (context) => {
  const controller = new AbortController();
  controller.abort("Stopped");
  const add = context.mock.method(controller.signal, "addEventListener");
  await assert.rejects(abortableDelay(250, controller.signal), (error: unknown) => error === "Stopped");
  assert.equal(add.mock.callCount(), 0);
  const task = { id: "done", terminal: true };
  assert.equal(
    await pollUntilTerminal(task, async () => assert.fail("Terminal tasks must not poll"), controller.signal),
    task,
  );
});

test("discovery polls connection-scoped tasks before listing sources", async (context) => {
  const paths: string[] = [];
  const controller = new AbortController();
  context.mock.method(globalThis, "fetch", async (path: string, options: RequestInit) => {
    paths.push(path);
    assert.equal(options.signal, controller.signal);
    if (path.endsWith("/discovery-tasks")) {
      assert.equal(options.method, "POST");
      return Response.json({ id: "discovery", terminal: false });
    }
    if (path.endsWith("/tasks/discovery"))
      return Response.json({ id: "discovery", terminal: true, status: "succeeded" });
    return Response.json([]);
  });
  assert.deepEqual(await discoverConnectionSources("connection", controller.signal), []);
  assert.deepEqual(paths, [
    "/api/v1/connections/connection/discovery-tasks",
    "/api/v1/connections/connection/tasks/discovery",
    "/api/v1/connections/connection/sources",
  ]);
});

test("discovery preserves failed and cancelled messages without listing sources", async (context) => {
  for (const status of ["failed", "cancelled"]) {
    context.mock.method(globalThis, "fetch", async (path: string) => {
      assert.ok(path.endsWith("/discovery-tasks"));
      return Response.json({ id: "discovery", terminal: true, status });
    });
    await assert.rejects(discoverConnectionSources("connection"), {
      message:
        status === "cancelled"
          ? "Source discovery was cancelled."
          : "Sources could not be discovered. Check the connection and try again.",
    });
  }
});

test("workspace polling stays workspace-scoped and returns terminal failures unchanged", async (context) => {
  const paths: string[] = [];
  const controller = new AbortController();
  context.mock.method(globalThis, "fetch", async (path: string, options: RequestInit) => {
    paths.push(path);
    assert.equal(options.signal, controller.signal);
    return Response.json({ id: "read", terminal: paths.length > 1, status: "failed" });
  });
  const initial = await getTask("workspace", "read", controller.signal);
  const result = await waitForTask("workspace", initial, controller.signal);
  assert.equal(result.status, "failed");
  assert.deepEqual(paths, ["/api/v1/workspaces/workspace/tasks/read", "/api/v1/workspaces/workspace/tasks/read"]);
});

test("reset waits for success before starting a read and skips reads after failure", async (context) => {
  for (const status of ["succeeded", "failed"]) {
    const paths: string[] = [];
    context.mock.method(globalThis, "fetch", async (path: string) => {
      paths.push(path);
      if (path.endsWith("/resets")) return Response.json({ id: "reset", terminal: false });
      if (path.endsWith("/tasks/reset")) return Response.json({ id: "reset", terminal: true, status });
      return Response.json({ id: "read", terminal: false });
    });
    const result = await resetAndRefetch("workspace", "source");
    assert.equal(result.id, status === "succeeded" ? "read" : "reset");
    assert.deepEqual(paths, [
      "/api/v1/workspaces/workspace/sources/source/resets",
      "/api/v1/workspaces/workspace/tasks/reset",
      ...(status === "succeeded" ? ["/api/v1/workspaces/workspace/sources/source/reads"] : []),
    ]);
  }
});
