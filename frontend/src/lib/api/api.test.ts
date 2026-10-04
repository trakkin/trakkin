import assert from "node:assert/strict";
import { test } from "node:test";
import { ApiRequestError, request } from "./client.ts";
import { discoverConnectionSources } from "./connections.ts";
import { createUploadedMappingSource } from "./mappings.ts";
import { getTask, resetAndRefetch, waitForTask } from "./operations.ts";
import { abortableDelay, pollUntilTerminal } from "./taskPolling.ts";

test("uploads raw mapping content and preserves headers, parameters, and signal", async (context) => {
  const controller = new AbortController();
  const content = 'co.anilist://anime/1 <=> org.themoviedb://tv/2\n# "quoted"\n';
  const state = { activeChainFingerprint: "fingerprint", sources: [] };
  context.mock.method(globalThis, "fetch", async (path: string, options: RequestInit) => {
    const url = new URL(path, "http://localhost");
    assert.equal(url.pathname, "/api/v1/mapping-sources/uploads");
    assert.equal(url.searchParams.get("sourceKey"), "key & value");
    assert.equal(url.searchParams.get("displayName"), "My upload");
    assert.equal(url.searchParams.get("enabled"), "false");
    assert.equal(options.method, "POST");
    assert.equal(options.body, content);
    assert.equal(new Headers(options.headers).get("content-type"), "text/plain; charset=utf-8");
    assert.equal(options.signal, controller.signal);
    assert.equal("rawBody" in options, false);
    return Response.json(state);
  });
  assert.deepEqual(
    await createUploadedMappingSource(
      { sourceKey: "key & value", displayName: "My upload", enabled: false },
      content,
      controller.signal,
    ),
    state,
  );
});

test("JSON bodies retain defaults and Headers or tuple overrides; empty raw bodies stay raw", async (context) => {
  const calls: RequestInit[] = [];
  context.mock.method(globalThis, "fetch", async (_path: string, options: RequestInit) => {
    calls.push(options);
    return Response.json(null);
  });
  await request("/json", { body: { enabled: true } });
  await request("/headers", { body: {}, headers: new Headers({ "Content-Type": "custom/type" }) });
  await request("/tuples", { body: {}, headers: [["Content-Type", "tuple/type"]] });
  await request("/raw", { rawBody: "" });
  await request("/get");
  assert.equal(calls[0].body, '{"enabled":true}');
  assert.equal(new Headers(calls[0].headers).get("content-type"), "application/json");
  assert.equal(new Headers(calls[1].headers).get("content-type"), "custom/type");
  assert.equal(new Headers(calls[2].headers).get("content-type"), "tuple/type");
  assert.equal(calls[3].body, "");
  assert.equal(new Headers(calls[3].headers).has("content-type"), false);
  assert.equal(calls[4].body, undefined);
  assert.equal(new Headers(calls[4].headers).has("content-type"), false);
});

test("raw uploads use structured errors and non-JSON error fallbacks", async (context) => {
  const problem = { field: "record", code: "invalid", message: "Invalid record" };
  context.mock.method(globalThis, "fetch", async () =>
    Response.json({ code: "invalid_mapping", message: "Bad mapping", fieldProblems: [problem] }, { status: 422 }),
  );
  await assert.rejects(
    createUploadedMappingSource({ sourceKey: "upload", displayName: "Upload", enabled: true }, "invalid"),
    (error: unknown) => {
      assert.ok(error instanceof ApiRequestError);
      assert.equal(error.status, 422);
      assert.equal(error.code, "invalid_mapping");
      assert.equal(error.message, "Bad mapping");
      assert.deepEqual(error.fieldProblems, [problem]);
      return true;
    },
  );
  context.mock.method(globalThis, "fetch", async () => new Response("Unavailable", { status: 503 }));
  await assert.rejects(request("/raw", { rawBody: "content" }), (error: unknown) => {
    assert.ok(error instanceof ApiRequestError);
    assert.equal(error.status, 503);
    assert.equal(error.code, "unexpected_response");
    assert.equal(error.message, "Request failed with status 503");
    assert.deepEqual(error.fieldProblems, []);
    return true;
  });
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
