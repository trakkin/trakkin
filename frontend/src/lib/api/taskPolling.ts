export async function pollUntilTerminal<Task extends { id: string; terminal: boolean }>(
  task: Task,
  fetchTask: (taskId: string, signal?: AbortSignal) => Promise<Task>,
  signal?: AbortSignal,
): Promise<Task> {
  let current = task;
  while (!current.terminal) {
    await abortableDelay(250, signal);
    current = await fetchTask(current.id, signal);
  }
  return current;
}

export function abortableDelay(milliseconds: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason);
      return;
    }
    const onAbort = () => {
      clearTimeout(timeout);
      signal?.removeEventListener("abort", onAbort);
      reject(signal?.reason);
    };
    const timeout = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, milliseconds);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
