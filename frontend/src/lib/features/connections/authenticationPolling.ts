import type { AuthenticationStep } from "../../api/connections.ts";
import { retryAfterMilliseconds } from "../../api/connections.ts";

export function authenticationPolling(signal: AbortSignal, poll: () => void) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  function clear() {
    if (timer !== undefined) clearTimeout(timer);
    timer = undefined;
  }
  signal.addEventListener("abort", clear, { once: true });
  return {
    clear,
    schedule(step: AuthenticationStep) {
      clear();
      if (signal.aborted || step.status !== "waiting" || (step.prompt?.fields.length ?? 0) > 0) return;
      timer = setTimeout(() => {
        timer = undefined;
        if (!signal.aborted) poll();
      }, retryAfterMilliseconds(step));
    },
  };
}

export function mergePrompt(current: AuthenticationStep, next: AuthenticationStep): AuthenticationStep {
  return next.status === "waiting" && !next.prompt ? { ...next, prompt: current.prompt } : next;
}
