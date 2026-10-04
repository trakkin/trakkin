import assert from "node:assert/strict";
import { test } from "node:test";
import type { AuthenticationStep } from "../../api/connections.ts";
import { authenticationPolling, mergePrompt } from "./authenticationPolling.ts";

const waiting: AuthenticationStep = {
  id: "authentication",
  status: "waiting",
  accounts: [],
  retryAfter: { seconds: 1, nanoseconds: 0 },
  prompt: null,
};

test("polling replaces timers and abort cancels scheduled and future polls", (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const controller = new AbortController();
  let calls = 0;
  const polling = authenticationPolling(controller.signal, () => {
    calls += 1;
  });
  polling.schedule(waiting);
  context.mock.timers.tick(500);
  polling.schedule(waiting);
  context.mock.timers.tick(500);
  assert.equal(calls, 0);
  context.mock.timers.tick(500);
  assert.equal(calls, 1);
  polling.schedule(waiting);
  controller.abort();
  context.mock.timers.tick(2000);
  polling.schedule(waiting);
  context.mock.timers.tick(2000);
  assert.equal(calls, 1);
});

test("terminal steps and prompts requiring input clear automatic polling", (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  let calls = 0;
  const polling = authenticationPolling(new AbortController().signal, () => {
    calls += 1;
  });
  const prompt = {
    message: "Enter code",
    verificationUrl: "",
    userCode: "",
    fields: [
      {
        key: "code",
        displayName: "Code",
        valueKind: "text" as const,
        required: true,
        secret: false,
        options: [],
        placeholder: "",
      },
    ],
  };
  for (const step of [
    { ...waiting, status: "completed" as const },
    { ...waiting, prompt },
  ]) {
    polling.schedule(waiting);
    polling.schedule(step);
    context.mock.timers.tick(2000);
  }
  assert.equal(calls, 0);
  assert.deepEqual(mergePrompt({ ...waiting, prompt }, waiting).prompt, prompt);
  assert.equal(mergePrompt({ ...waiting, prompt }, { ...waiting, status: "completed" }).prompt, null);
});
