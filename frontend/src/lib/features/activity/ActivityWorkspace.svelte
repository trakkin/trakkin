<script lang="ts">
import { ArrowRight, Clock3, RefreshCw, RotateCcw, Search, TimerReset } from "@lucide/svelte";
import { untrack } from "svelte";
import { page } from "$app/state";
import { Button, Chip, IconButton, PageHeader, Panel, Select, Spinner, TextInput } from "$lib";
import { type ActivityAction, type ActivityQuery, type ActivityRun, listActivity } from "$lib/api/activity";
import ActivityActionDrawer from "./ActivityActionDrawer.svelte";

const statusItems = [
  { label: "All run statuses", value: "" },
  { label: "Queued", value: "queued" },
  { label: "Running", value: "running" },
  { label: "Succeeded", value: "succeeded" },
  { label: "Partial", value: "partial" },
  { label: "Failed", value: "failed" },
  { label: "Indeterminate", value: "indeterminate" },
  { label: "Cancelled", value: "cancelled" },
];

const fieldItems = [
  { label: "All state fields", value: "" },
  { label: "Completed", value: "trakkin.state:completed" },
  { label: "Watched", value: "trakkin.state:watched" },
  { label: "Progress", value: "trakkin.state:progress" },
  { label: "Rating", value: "trakkin.state:rating" },
  { label: "Rewatch count", value: "trakkin.state:rewatch-count" },
  { label: "Status", value: "trakkin.state:status" },
];

let runs = $state<ActivityRun[]>([]);
let loading = $state(true);
let reloading = $state(false);
let errorMessage = $state("");
let nextCursor = $state<string | null>(null);
let cursors = $state<Array<string | undefined>>([undefined]);
let pageIndex = $state(0);
let statusFilter = $state("");
let sourceFilter = $state("");
let fieldFilter = $state("");
let occurredAfter = $state("");
let occurredBefore = $state("");
let activeQuery = $state<ActivityQuery>({ limit: 25 });
let selectedAction = $state<ActivityAction | null>(null);
let detailOpen = $state(false);
let requestSequence = 0;
let pollTimer: ReturnType<typeof setTimeout> | undefined;
let workspaceId = $derived(page.params.workspaceId ?? "");
let focusedRunId = $derived(page.url.searchParams.get("runId") ?? "");

$effect(() => {
  const activeWorkspaceId = workspaceId;
  const activeFocusedRunId = focusedRunId;
  const initialQuery: ActivityQuery = { limit: 25, runId: activeFocusedRunId || undefined };
  const sequence = ++requestSequence;
  const controller = new AbortController();
  clearPoll();
  runs = [];
  nextCursor = null;
  cursors = [undefined];
  pageIndex = 0;
  activeQuery = initialQuery;
  selectedAction = null;
  detailOpen = false;
  untrack(() => void loadPage(activeWorkspaceId, activeQuery, undefined, 0, sequence, controller.signal));
  return () => {
    controller.abort();
    clearPoll();
  };
});

async function loadPage(
  activeWorkspaceId: string,
  query: ActivityQuery,
  cursor: string | undefined,
  targetPage: number,
  sequence = ++requestSequence,
  signal?: AbortSignal,
) {
  clearPoll();
  const replacing = targetPage !== pageIndex || runs.length === 0;
  if (replacing) loading = true;
  else reloading = true;
  errorMessage = "";
  try {
    const result = await listActivity(activeWorkspaceId, { ...query, cursor }, signal);
    if (sequence !== requestSequence) return;
    runs = result.runs;
    nextCursor = result.nextCursor ?? null;
    pageIndex = targetPage;
    if (query.runId && result.runs.some((run) => run.status === "queued" || run.status === "running")) {
      pollTimer = setTimeout(() => {
        pollTimer = undefined;
        void loadPage(activeWorkspaceId, query, cursor, targetPage, sequence, signal);
      }, 1_000);
    }
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (sequence === requestSequence) {
      errorMessage = error instanceof Error ? error.message : "Synchronization activity could not be loaded";
    }
  } finally {
    if (sequence === requestSequence) {
      loading = false;
      reloading = false;
    }
  }
}

function clearPoll() {
  if (!pollTimer) return;
  clearTimeout(pollTimer);
  pollTimer = undefined;
}

function applyFilters() {
  let occurredAtOrAfter: string | undefined;
  let occurredBeforeValue: string | undefined;
  try {
    occurredAtOrAfter = rfc3339(occurredAfter);
    occurredBeforeValue = rfc3339(occurredBefore);
  } catch {
    errorMessage = "Activity time filters are invalid";
    return;
  }
  if (occurredAtOrAfter && occurredBeforeValue && occurredAtOrAfter >= occurredBeforeValue) {
    errorMessage = "The activity start time must be before the end time";
    return;
  }
  activeQuery = {
    limit: 25,
    status: statusFilter || undefined,
    sourceId: sourceFilter.trim() || undefined,
    field: fieldFilter || undefined,
    occurredAtOrAfter,
    occurredBefore: occurredBeforeValue,
  };
  cursors = [undefined];
  void loadPage(workspaceId, activeQuery, undefined, 0);
}

function resetFilters() {
  statusFilter = "";
  sourceFilter = "";
  fieldFilter = "";
  occurredAfter = "";
  occurredBefore = "";
  activeQuery = { limit: 25 };
  cursors = [undefined];
  void loadPage(workspaceId, activeQuery, undefined, 0);
}

function nextPage() {
  if (!nextCursor) return;
  const targetPage = pageIndex + 1;
  cursors = [...cursors.slice(0, targetPage), nextCursor];
  void loadPage(workspaceId, activeQuery, nextCursor, targetPage);
}

function previousPage() {
  if (pageIndex === 0) return;
  const targetPage = pageIndex - 1;
  void loadPage(workspaceId, activeQuery, cursors[targetPage], targetPage);
}

function reloadCurrent() {
  void loadPage(workspaceId, activeQuery, cursors[pageIndex], pageIndex);
}

function inspectAction(action: ActivityAction) {
  selectedAction = action;
  detailOpen = true;
}

function rfc3339(value: string): string | undefined {
  if (!value) return undefined;
  const date = new Date(value);
  if (!Number.isFinite(date.valueOf())) throw new Error("invalid date");
  return date.toISOString();
}

function formatTimestamp(value: string | null | undefined): string {
  if (!value) return "Not started";
  const timestamp = new Date(value);
  if (!Number.isFinite(timestamp.valueOf())) return "Unknown";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(timestamp);
}

function shortIdentity(value: string): string {
  return `${value.slice(0, 8)}${value.length > 8 ? "..." : ""}`;
}

function fieldLabel(value: string): string {
  return (value.split(":").at(-1) ?? value)
    .split("-")
    .map((part) => `${part[0]?.toUpperCase() ?? ""}${part.slice(1)}`)
    .join(" ");
}

function statusTone(status: string): "neutral" | "accent" | "success" | "warning" | "danger" | "info" {
  if (["succeeded", "applied", "no_op"].includes(status)) return "success";
  if (["failed", "precondition_failed", "retryable_failure"].includes(status)) return "danger";
  if (["partial", "indeterminate", "cancelling"].includes(status)) return "warning";
  if (["queued", "running"].includes(status)) return "accent";
  if (status === "cancelled") return "neutral";
  return "info";
}
</script>

<svelte:head><title>Trakkin &middot; Activity</title></svelte:head>

<div class="flex h-full min-h-0 flex-col">
  <PageHeader title="Activity">
    {#snippet actions()}
      <IconButton
        disabled={reloading}
        label="Refresh synchronization activity"
        onclick={reloadCurrent}
        size="sm"
        title="Refresh synchronization activity"
      >
        <span class={["inline-flex", reloading && "animate-spin"]}>
          <RefreshCw
            aria-hidden="true"
            size={14}
          />
        </span>
      </IconButton>
    {/snippet}
  </PageHeader>

  <section
    aria-label="Activity filters"
    class="shrink-0 border-b border-border-strong bg-surface-raised/35 px-4 py-3 sm:px-6"
  >
    <div
      class="grid min-w-0 gap-2 md:grid-cols-2 xl:grid-cols-[minmax(10rem,0.8fr)_minmax(12rem,1fr)_minmax(11rem,0.9fr)_minmax(11rem,0.9fr)_minmax(11rem,0.9fr)_auto]"
    >
      <Select
        ariaLabel="Activity run status"
        items={statusItems}
        onValueChange={(value) => (statusFilter = value)}
        placeholder="All run statuses"
        triggerClass="w-full min-w-0"
        value={statusFilter}
      />
      <div class="block min-w-0">
        <TextInput
          aria-label="Receiver source ID"
          class="w-full"
          oninput={(event) => (sourceFilter = event.currentTarget.value)}
          placeholder="Receiver source ID"
          value={sourceFilter}
        />
      </div>
      <Select
        ariaLabel="Activity state field"
        items={fieldItems}
        onValueChange={(value) => (fieldFilter = value)}
        placeholder="All state fields"
        triggerClass="w-full min-w-0"
        value={fieldFilter}
      />
      <div class="relative block min-w-0">
        <Clock3
          aria-hidden="true"
          class="pointer-events-none absolute top-1/2 left-2.5 z-10 -translate-y-1/2 text-text-muted"
          size={14}
        />
        <TextInput
          aria-label="Activity from"
          class="w-full pl-8"
          oninput={(event) => (occurredAfter = event.currentTarget.value)}
          type="datetime-local"
          value={occurredAfter}
        />
      </div>
      <div class="relative block min-w-0">
        <TimerReset
          aria-hidden="true"
          class="pointer-events-none absolute top-1/2 left-2.5 z-10 -translate-y-1/2 text-text-muted"
          size={14}
        />
        <TextInput
          aria-label="Activity before"
          class="w-full pl-8"
          oninput={(event) => (occurredBefore = event.currentTarget.value)}
          type="datetime-local"
          value={occurredBefore}
        />
      </div>
      <div class="flex items-center justify-end gap-2 md:col-span-2 xl:col-span-1">
        <IconButton
          label="Reset activity filters"
          onclick={resetFilters}
          size="sm"
          title="Reset activity filters"
        >
          <RotateCcw
            aria-hidden="true"
            size={14}
          />
        </IconButton>
        <Button
          onclick={applyFilters}
          size="sm"
        >
          <Search
            aria-hidden="true"
            size={14}
          />
          Apply
        </Button>
      </div>
    </div>
  </section>

  <div class="flex min-h-0 flex-1 flex-col p-4 sm:p-6">
    {#if loading}
      <div
        class="flex min-h-48 flex-1 items-center justify-center gap-2 text-label text-text-muted"
        role="status"
      >
        <Spinner size={16} />
        Loading synchronization activity...
      </div>
    {:else if errorMessage && runs.length === 0}
      <Panel
        class="grid min-h-48 place-items-center px-4 py-6 text-center text-label text-danger"
        role="alert"
        tone="danger"
      >
        <div>
          <p>{errorMessage}</p>
          <Button
            class="mt-4"
            onclick={reloadCurrent}
            size="sm"
          >
            <RefreshCw
              aria-hidden="true"
              size={14}
            />
            Retry
          </Button>
        </div>
      </Panel>
    {:else if runs.length === 0}
      <Panel
        aria-label="Synchronization activity"
        class="grid min-h-56 flex-1 place-items-center p-6 text-center"
        element="section"
        tone="dashed"
      >
        <div>
          <Clock3
            aria-hidden="true"
            class="mx-auto text-text-disabled"
            size={24}
          />
          <h2 class="mt-3 text-heading font-semibold text-text-primary">No synchronization activity</h2>
          <p class="mt-1 text-label text-text-muted">No retained sync runs match the current filters.</p>
        </div>
      </Panel>
    {:else}
      <section
        aria-label="Synchronization activity"
        class="flex min-h-0 flex-1 flex-col overflow-hidden rounded-panel border border-border-strong bg-surface-panel shadow-[inset_0_1px_rgb(255_255_255/3%)]"
      >
        {#if errorMessage}
          <div
            class="shrink-0 border-b border-danger/20 bg-danger/6 px-4 py-2.5 text-label text-danger"
            role="alert"
          >
            {errorMessage}
          </div>
        {/if}
        <div class="min-h-0 flex-1 overflow-y-auto">
          {#each runs as run (run.runId)}
            <article
              aria-current={run.runId === focusedRunId ? "true" : undefined}
              aria-label={`Synchronization run ${run.runId}`}
              class={[
                "border-b border-border-strong last:border-b-0",
                run.runId === focusedRunId && "ring-1 ring-inset ring-accent/60",
              ]}
            >
              <header class="flex flex-wrap items-start justify-between gap-3 bg-surface-raised/45 px-4 py-3 sm:px-5">
                <div class="min-w-0">
                  <div class="flex flex-wrap items-center gap-2">
                    <h2 class="font-mono text-label font-semibold text-text-primary">Run {shortIdentity(run.runId)}</h2>
                    <Chip
                      dot
                      tone={statusTone(run.status)}
                      >{run.status}</Chip
                    >
                    {#if run.runId === focusedRunId}
                      <Chip tone="info">Just queued</Chip>
                    {/if}
                  </div>
                  <p class="mt-1 text-meta text-text-muted">
                    {formatTimestamp(run.createdAt)}
                    · {run.actions.length} action{run.actions.length === 1 ? "" : "s"}
                  </p>
                </div>
                <p class="font-mono text-meta text-text-disabled">Preview {shortIdentity(run.previewId)}</p>
              </header>

              <div class="divide-y divide-border-subtle">
                {#each run.actions as action (action.actionId)}
                  <button
                    aria-label={`Inspect ${fieldLabel(action.semanticField)} action ${action.actionId}`}
                    class="grid min-h-14 w-full min-w-0 grid-cols-[minmax(0,1fr)_auto_1rem] items-center gap-3 px-4 py-2.5 text-left outline-none transition-colors hover:bg-white/3 focus-visible:bg-white/4 sm:grid-cols-[minmax(10rem,1fr)_minmax(10rem,1fr)_8rem_1rem] sm:px-5"
                    onclick={() => inspectAction(action)}
                    type="button"
                  >
                    <span class="min-w-0">
                      <span class="block truncate text-label font-medium text-text-primary">
                        {fieldLabel(action.semanticField)}
                      </span>
                      <span class="mt-0.5 block truncate font-mono text-meta text-text-muted sm:hidden">
                        Source {shortIdentity(action.receiverSourceId)}
                      </span>
                    </span>
                    <span class="hidden truncate font-mono text-meta text-text-muted sm:block">
                      Source {shortIdentity(action.receiverSourceId)}
                    </span>
                    <span class="justify-self-end">
                      <Chip
                        dot
                        tone={statusTone(action.status)}
                        >{action.status.replaceAll("_", " ")}</Chip
                      >
                    </span>
                    <ArrowRight
                      aria-hidden="true"
                      class="text-text-disabled"
                      size={14}
                    />
                  </button>
                {/each}
              </div>
            </article>
          {/each}
        </div>

        <footer class="flex h-12 shrink-0 items-center justify-between border-t border-border-strong px-3 sm:px-4">
          <span class="text-meta text-text-muted">Page {pageIndex + 1}</span>
          <div class="flex items-center gap-2">
            <Button
              disabled={pageIndex === 0 || reloading}
              onclick={previousPage}
              size="sm"
              variant="ghost"
              >Previous</Button
            >
            <Button
              disabled={!nextCursor || reloading}
              onclick={nextPage}
              size="sm"
              variant="ghost"
              >Next</Button
            >
          </div>
        </footer>
      </section>
    {/if}
  </div>
</div>

<ActivityActionDrawer
  action={selectedAction}
  ontaskchange={reloadCurrent}
  {workspaceId}
  bind:open={detailOpen}
/>
