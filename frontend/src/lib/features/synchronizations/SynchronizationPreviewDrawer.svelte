<script lang="ts">
import { Activity, CheckCheck, RotateCw, X } from "@lucide/svelte";
import { untrack } from "svelte";
import { goto } from "$app/navigation";
import { Button, Chip, Dialog, IconButton, Panel, Spinner, Tabs } from "$lib";
import { ApiRequestError } from "$lib/api/client";
import {
  createSynchronizationPreview,
  type SynchronizationPreview,
  type SynchronizationPreviewEntry,
  type SynchronizationRunAdmission,
  startSynchronizationRun,
} from "$lib/api/synchronizations";
import type { WorkspaceSourceDirectoryEntry } from "$lib/features/workspaces/workspaceSourceDirectory";
import SynchronizationEntryRow from "./SynchronizationEntryRow.svelte";

type PreviewView = "proposed" | "findings" | "all";
type WorkflowStage = "preview" | "confirm" | "executing";

type Props = {
  open: boolean;
  workspaceId: string;
  policyId: string;
  sources: WorkspaceSourceDirectoryEntry[];
};

let { open = $bindable(false), workspaceId, policyId, sources }: Props = $props();

let preview = $state<SynchronizationPreview | null>(null);
let run = $state<SynchronizationRunAdmission | null>(null);
let entries = $state<SynchronizationPreviewEntry[]>([]);
let stage = $state<WorkflowStage>("preview");
let loading = $state(false);
let errorMessage = $state("");
let view = $state<PreviewView>("proposed");
let requestSequence = 0;

let proposedEntries = $derived(entries.filter((entry) => entry.category === "proposed"));
let findingEntries = $derived(entries.filter((entry) => entry.finding !== null && entry.finding !== undefined));
let visibleEntries = $derived(view === "proposed" ? proposedEntries : view === "findings" ? findingEntries : entries);
let sourceLabels = $derived(
  new Map(
    sources.map((source) => [source.source.id, `${source.source.displayName} · ${source.connection.displayName}`]),
  ),
);
$effect(() => {
  if (!open || !workspaceId) return;
  const sequence = ++requestSequence;
  const controller = new AbortController();
  untrack(() => void loadPreview(sequence, controller.signal));
  return () => controller.abort();
});

async function loadPreview(sequence: number, signal?: AbortSignal) {
  preview = null;
  run = null;
  entries = [];
  stage = "preview";
  loading = true;
  errorMessage = "";
  try {
    const created = await createSynchronizationPreview(workspaceId, policyId, signal);
    if (sequence !== requestSequence) return;
    preview = created;
    entries = created.entries;
    view = created.counts.proposed > 0 ? "proposed" : created.counts.findings > 0 ? "findings" : "all";
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (sequence === requestSequence) {
      errorMessage = error instanceof Error ? error.message : "Sync preview could not be created";
    }
  } finally {
    if (sequence === requestSequence) loading = false;
  }
}

function changeOpen(nextOpen: boolean) {
  open = nextOpen;
  if (nextOpen) return;
  requestSequence += 1;
  preview = null;
  run = null;
  entries = [];
  stage = "preview";
  errorMessage = "";
}

function retry() {
  const sequence = ++requestSequence;
  void loadPreview(sequence);
}

function beginConfirmation() {
  if (proposedEntries.length > 0) stage = "confirm";
}

async function confirmExecution() {
  if (!preview || proposedEntries.length === 0 || stage === "executing") return;
  const sequence = requestSequence;
  stage = "executing";
  errorMessage = "";
  try {
    const admitted = await startSynchronizationRun(workspaceId, preview.previewId, preview.previewHash);
    if (sequence !== requestSequence) return;
    run = admitted;
  } catch (error) {
    if (sequence !== requestSequence) return;
    errorMessage = executionErrorMessage(error);
    stage = "preview";
  }
}

function executionErrorMessage(error: unknown): string {
  if (error instanceof ApiRequestError) {
    if (error.code === "sync_preview_expired" || error.code === "sync_preview_stale") {
      return "This preview changed or expired. Create a fresh preview before trying again.";
    }
    if (error.code === "sync_preview_already_admitted") {
      return "This preview was already submitted. Create a fresh preview to continue.";
    }
  }
  return error instanceof Error ? error.message : "Selected updates could not be queued";
}

function viewActivity() {
  if (!run) return;
  const runId = run.runId;
  changeOpen(false);
  void goto(`/w/${workspaceId}/activity?runId=${encodeURIComponent(runId)}`);
}
</script>

<Dialog.Root
  onOpenChange={changeOpen}
  bind:open
>
  <Dialog.Drawer style={open ? "translate: none" : undefined}>
    <header class="flex shrink-0 items-start justify-between gap-4 border-b border-border-strong px-4 py-4 sm:px-5">
      <div class="min-w-0">
        <Dialog.Title class="text-title font-semibold text-text-primary">Sync preview</Dialog.Title>
        <Dialog.Description class="mt-1 text-label text-text-muted">
          {sources.length}
          connected source{sources.length === 1 ? "" : "s"}
          in this workspace
        </Dialog.Description>
      </div>
      <IconButton
        label="Close sync preview"
        onclick={() => changeOpen(false)}
        size="sm"
      >
        <X
          aria-hidden="true"
          size={15}
        />
      </IconButton>
    </header>

    {#if loading}
      <div
        class="flex min-h-64 flex-1 items-center justify-center gap-2 text-label text-text-muted"
        role="status"
      >
        <Spinner size={16} />
        Generating preview...
      </div>
    {:else if errorMessage}
      <div class="flex flex-1 flex-col items-center justify-center gap-4 p-6 text-center">
        <Panel
          class="max-w-md px-4 py-3 text-label text-danger"
          role="alert"
          tone="danger"
        >
          {errorMessage}
        </Panel>
        <Button
          onclick={retry}
          size="sm"
        >
          <RotateCw
            aria-hidden="true"
            size={14}
          />
          Retry
        </Button>
      </div>
    {:else if run}
      <div class="flex min-h-0 flex-1 flex-col">
        <div class="flex min-h-64 flex-1 flex-col items-center justify-center gap-3 px-6 text-center">
          <span class="flex size-9 items-center justify-center rounded-full bg-success-subtle text-success">
            <CheckCheck
              aria-hidden="true"
              size={18}
            />
          </span>
          <div>
            <h2 class="text-title font-semibold text-text-primary">Updates queued</h2>
            <p class="mt-1 text-label text-text-muted">
              {preview?.counts.proposed ?? 0}
              receiver update{preview?.counts.proposed === 1 ? "" : "s"}
              ready for execution
            </p>
          </div>
          <p class="font-mono text-meta text-text-disabled">Run {run.runId.slice(0, 8)}</p>
        </div>
        <footer class="flex shrink-0 justify-end gap-2 border-t border-border-strong px-4 py-3 sm:px-5">
          <Button
            onclick={() => changeOpen(false)}
            size="sm"
            >Close</Button
          >
          <Button
            onclick={viewActivity}
            size="sm"
            variant="primary"
          >
            <Activity
              aria-hidden="true"
              size={14}
            />
            View activity
          </Button>
        </footer>
      </div>
    {:else if preview}
      <div class="flex min-h-0 flex-1 flex-col">
        <section class="shrink-0 border-b border-border-subtle px-4 py-3 sm:px-5">
          <div class="flex flex-wrap gap-2">
            <Chip
              dot
              tone="accent"
              >{preview.counts.proposed}
              proposed</Chip
            >
            <Chip
              dot
              tone="neutral"
              >{preview.counts.noOp}
              unchanged</Chip
            >
            <Chip
              dot
              tone="warning"
              >{preview.counts.findings}
              findings</Chip
            >
          </div>
        </section>

        <div
          class="flex shrink-0 flex-wrap items-center justify-between gap-3 border-b border-border-strong px-4 py-2.5 sm:px-5"
        >
          <Tabs.Root
            class="w-auto"
            bind:value={view}
          >
            <Tabs.List aria-label="Sync preview entries">
              <Tabs.Trigger value="proposed">Proposed {proposedEntries.length}</Tabs.Trigger>
              <Tabs.Trigger value="findings">Findings {findingEntries.length}</Tabs.Trigger>
              <Tabs.Trigger value="all">All {entries.length}</Tabs.Trigger>
            </Tabs.List>
          </Tabs.Root>
        </div>

        <div
          class="hidden shrink-0 grid-cols-[minmax(9rem,1.15fr)_minmax(7rem,0.9fr)_minmax(7rem,0.9fr)_minmax(8rem,1fr)] gap-3 border-b border-border-subtle px-5 py-2 text-meta font-semibold uppercase text-text-disabled lg:grid"
        >
          <span>Field</span>
          <span>From</span>
          <span>Before</span>
          <span>After</span>
        </div>

        <div class="min-h-0 flex-1 overflow-y-auto">
          {#if visibleEntries.length}
            {#each visibleEntries as entry (entry.actionHash)}
              <SynchronizationEntryRow
                {entry}
                {sourceLabels}
              />
            {/each}
          {:else}
            <div
              class="flex min-h-48 flex-col items-center justify-center gap-2 px-6 text-center text-label text-text-muted"
            >
              <CheckCheck
                aria-hidden="true"
                size={18}
              />
              No entries in this view
            </div>
          {/if}
        </div>

        <footer
          class="flex shrink-0 items-center justify-between gap-3 border-t border-border-strong px-4 py-3 sm:px-5"
        >
          <p class="text-label text-text-muted">
            {proposedEntries.length}
            proposed update{proposedEntries.length === 1 ? "" : "s"}
          </p>
          <div class="flex items-center gap-2">
            {#if stage === "confirm"}
              <Button
                onclick={() => (stage = "preview")}
                size="sm"
                >Back</Button
              >
              <Button
                disabled={proposedEntries.length === 0}
                onclick={confirmExecution}
                size="sm"
                variant="primary"
              >
                Confirm and start
              </Button>
            {:else if stage === "executing"}
              <Button
                disabled
                size="sm"
                variant="primary"
              >
                <Spinner size={14} />
                Starting...
              </Button>
            {:else}
              <Button
                onclick={() => changeOpen(false)}
                size="sm"
                >Close</Button
              >
              <Button
                disabled={proposedEntries.length === 0}
                onclick={beginConfirmation}
                size="sm"
                variant="primary"
                >Continue</Button
              >
            {/if}
          </div>
        </footer>
      </div>
    {/if}
  </Dialog.Drawer>
</Dialog.Root>
