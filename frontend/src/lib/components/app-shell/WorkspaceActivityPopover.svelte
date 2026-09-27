<script lang="ts">
import { Activity, CircleCheck, CircleStop, TriangleAlert } from "@lucide/svelte";
import { Popover } from "bits-ui";
import { untrack } from "svelte";
import { cancelTask, listTasks, type TaskRun } from "$lib/api/operations";
import { IconButton, Spinner } from "$lib/components/ui";

type Props = {
  workspaceId: string;
};

let { workspaceId }: Props = $props();
let tasks = $state<TaskRun[]>([]);
let popoverOpen = $state(false);
let loading = $state(false);
let errorMessage = $state("");
let cancellingIds = $state<string[]>([]);
let now = $state(Date.now());
let refreshingWorkspaceId = "";
let contextSequence = 0;
let activeTasks = $derived(tasks.filter((task) => !task.terminal));
let recentTasks = $derived(
  tasks
    .filter((task) => task.terminal)
    .toSorted((left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt))
    .slice(0, 4),
);
let visibleTasks = $derived([...activeTasks, ...recentTasks]);
let taskLabel = $derived.by(() => {
  if (activeTasks.length > 0) {
    return `Tasks, ${activeTasks.length} active ${activeTasks.length === 1 ? "task" : "tasks"}`;
  }
  return "Tasks";
});

$effect(() => {
  const selectedWorkspaceId = workspaceId;
  const sequence = ++contextSequence;
  tasks = [];
  loading = Boolean(selectedWorkspaceId);
  errorMessage = "";
  cancellingIds = [];
  refreshingWorkspaceId = "";
  if (!selectedWorkspaceId) {
    loading = false;
    return;
  }
  const controller = new AbortController();
  untrack(() => void refresh(selectedWorkspaceId, sequence, controller.signal));
  return () => controller.abort();
});

$effect(() => {
  const selectedWorkspaceId = workspaceId;
  if (!popoverOpen || !selectedWorkspaceId) return;
  const sequence = contextSequence;
  const controller = new AbortController();
  untrack(() => void refresh(selectedWorkspaceId, sequence, controller.signal));
  return () => controller.abort();
});

$effect(() => {
  const selectedWorkspaceId = workspaceId;
  if (!selectedWorkspaceId || (!popoverOpen && activeTasks.length === 0)) return;
  const sequence = contextSequence;
  const polling = window.setInterval(() => {
    void refresh(selectedWorkspaceId, sequence);
  }, 4_000);
  return () => window.clearInterval(polling);
});

$effect(() => {
  if (activeTasks.length === 0) return;
  const clock = window.setInterval(() => (now = Date.now()), 1_000);
  return () => window.clearInterval(clock);
});

async function refresh(selectedWorkspaceId: string, sequence: number, signal?: AbortSignal) {
  if (!ownsContext(sequence, selectedWorkspaceId)) return;
  if (refreshingWorkspaceId === selectedWorkspaceId) return;
  refreshingWorkspaceId = selectedWorkspaceId;
  loading = tasks.length === 0;
  errorMessage = "";
  try {
    const result = await listTasks(selectedWorkspaceId, signal);
    if (ownsContext(sequence, selectedWorkspaceId)) tasks = result;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (ownsContext(sequence, selectedWorkspaceId)) {
      errorMessage = error instanceof Error ? error.message : "Workspace activity is unavailable";
    }
  } finally {
    if (ownsContext(sequence, selectedWorkspaceId)) {
      refreshingWorkspaceId = "";
      loading = false;
    }
  }
}

async function requestCancellation(task: TaskRun) {
  const selectedWorkspaceId = workspaceId;
  const sequence = contextSequence;
  if (!ownsContext(sequence, selectedWorkspaceId) || cancellingIds.includes(task.id)) return;
  cancellingIds = [...cancellingIds, task.id];
  errorMessage = "";
  try {
    await cancelTask(selectedWorkspaceId, task.id);
    if (!ownsCancellation(sequence, selectedWorkspaceId, task.id)) return;
    await refresh(selectedWorkspaceId, sequence);
  } catch (error) {
    if (ownsCancellation(sequence, selectedWorkspaceId, task.id)) {
      errorMessage = error instanceof Error ? error.message : `Could not cancel ${taskTitle(task)}`;
    }
  } finally {
    if (ownsCancellation(sequence, selectedWorkspaceId, task.id)) {
      cancellingIds = cancellingIds.filter((taskId) => taskId !== task.id);
    }
  }
}

function ownsContext(sequence: number, selectedWorkspaceId: string): boolean {
  return sequence === contextSequence && workspaceId === selectedWorkspaceId;
}

function ownsCancellation(sequence: number, selectedWorkspaceId: string, taskId: string): boolean {
  return ownsContext(sequence, selectedWorkspaceId) && cancellingIds.includes(taskId);
}

function taskTitle(task: TaskRun): string {
  switch (task.kind) {
    case "sourceDiscovery":
      return "source discovery";
    case "catalogRead":
      return "catalog read";
    case "stateRead":
      return "state read";
    case "catalogReset":
      return "catalog reset";
    case "stateReset":
      return "state reset";
  }
}

function taskDetail(task: TaskRun): string {
  const mode = task.mode ? `${task.mode.replaceAll("_", " ")} ` : "";
  const subject = task.sourceId ? `source ${task.sourceId.slice(0, 8)}` : `connection ${task.connectionId.slice(0, 8)}`;
  return `${mode}${subject}`;
}

function elapsedTime(task: TaskRun): string {
  const startedAt = Date.parse(task.startedAt ?? task.createdAt);
  if (!Number.isFinite(startedAt)) return "";
  const elapsedSeconds = Math.max(0, Math.floor((now - startedAt) / 1_000));
  if (elapsedSeconds < 60) return `${elapsedSeconds}s`;
  const minutes = Math.floor(elapsedSeconds / 60);
  return `${minutes}m ${elapsedSeconds % 60}s`;
}

function taskStatus(task: TaskRun): string {
  if (task.status === "succeeded") return "Completed";
  if (task.status === "failed") return "Failed";
  if (task.status === "cancelled") return "Cancelled";
  if (task.status === "interrupted") return "Interrupted";
  return "Running";
}
</script>

<Popover.Root bind:open={popoverOpen}>
  <Popover.Trigger>
    {#snippet child({ props })}
      <IconButton
        {...props}
        class={activeTasks.length > 0
          ? "border-success/30 bg-success/10 text-success shadow-[0_0_0_3px_color-mix(in_oklab,var(--success)_10%,transparent)]"
          : popoverOpen
            ? "border-border-strong bg-white/6 text-text-primary"
            : "text-text-secondary"}
        label={taskLabel}
        size="sm"
        title="Task status"
      >
        <Activity
          aria-hidden="true"
          size={15}
        />
        {#if activeTasks.length > 0}
          <span
            aria-hidden="true"
            class="absolute top-0.5 right-0.5 size-1.5 rounded-full bg-success shadow-[0_0_0_2px_var(--surface-sidebar)]"
          ></span>
        {/if}
      </IconButton>
    {/snippet}
  </Popover.Trigger>
  <Popover.Portal>
    <Popover.Content
      align="end"
      class="z-50 w-80 max-w-[calc(100vw-1rem)] origin-(--bits-popover-content-transform-origin) overflow-hidden rounded-overlay border border-border-subtle bg-surface-raised text-text-secondary shadow-[0_12px_32px_rgb(0_0_0/34%)] outline-none transition-[opacity,transform] duration-(--motion-emphasis) ease-kinetic data-starting-style:translate-y-1 data-starting-style:opacity-0 data-ending-style:translate-y-1 data-ending-style:opacity-0"
      collisionPadding={8}
      sideOffset={7}
      strategy="fixed"
    >
      <header class="flex h-10 items-center justify-between border-b border-border-subtle px-3.5">
        <h2 class="text-label font-semibold text-text-primary">Task status</h2>
        {#if activeTasks.length > 0}
          <span class="font-mono text-meta text-success">{activeTasks.length} active</span>
        {:else if recentTasks.length > 0}
          <span class="font-mono text-meta text-text-disabled">Recent</span>
        {/if}
      </header>

      {#if loading}
        <div class="flex h-24 flex-col items-center justify-center gap-2 text-text-muted">
          <Spinner size={18} />
          <p class="text-label">Loading tasks</p>
        </div>
      {:else if visibleTasks.length > 0}
        <ul class="divide-y divide-border-subtle">
          {#each visibleTasks as task (task.id)}
            {@const active = !task.terminal}
            <li class="flex min-h-16 items-center gap-3 px-3.5 py-3">
              <span
                class="flex size-7 shrink-0 items-center justify-center rounded-md {active ? 'bg-success/8 text-success' : task.status === 'failed' || task.status === 'interrupted' ? 'bg-warning/8 text-warning' : 'bg-white/4 text-text-muted'}"
              >
                {#if active}
                  <Spinner size={16} />
                {:else if task.status === "failed" || task.status === "interrupted"}
                  <TriangleAlert
                    aria-hidden="true"
                    size={15}
                  />
                {:else}
                  <CircleCheck
                    aria-hidden="true"
                    size={15}
                  />
                {/if}
              </span>
              <div class="min-w-0 flex-1">
                <p class="truncate text-label font-[560] capitalize text-text-primary">{taskTitle(task)}</p>
                <p class="mt-0.5 truncate text-meta text-text-muted">{taskDetail(task)}</p>
              </div>
              <div class="flex shrink-0 items-center gap-1">
                {#if active && task.cancellationSupported}
                  <IconButton
                    disabled={cancellingIds.includes(task.id)}
                    label={`Cancel ${taskTitle(task)}`}
                    onclick={() => requestCancellation(task)}
                    size="sm"
                    title="Cancel task"
                    variant="danger"
                  >
                    <CircleStop
                      aria-hidden="true"
                      size={14}
                    />
                  </IconButton>
                {/if}
                <span class="w-15 text-right text-meta text-text-disabled">
                  {active ? elapsedTime(task) : taskStatus(task)}
                </span>
              </div>
            </li>
          {/each}
        </ul>
      {:else}
        <div class="flex h-24 flex-col items-center justify-center gap-2 text-text-disabled">
          <CircleCheck
            aria-hidden="true"
            size={18}
          />
          <p class="text-label">No recent tasks</p>
        </div>
      {/if}

      {#if errorMessage}
        <p
          class="border-t border-danger/20 bg-danger/6 px-3.5 py-2 text-meta text-danger"
          role="alert"
        >
          {errorMessage}
        </p>
      {/if}
    </Popover.Content>
  </Popover.Portal>
</Popover.Root>
