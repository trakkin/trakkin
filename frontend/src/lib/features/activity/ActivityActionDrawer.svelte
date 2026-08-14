<script lang="ts">
import { Ban, CircleAlert, RotateCw, X } from "@lucide/svelte";
import { untrack } from "svelte";
import { Button, Chip, Dialog, IconButton, Panel, Spinner } from "$lib";
import { type ActivityAction, type ActivityActionDetail, getActivityAction } from "$lib/api/activity";
import { cancelTask, getTask, type TaskRun } from "$lib/api/operations";

type Props = {
  action: ActivityAction | null;
  open: boolean;
  workspaceId: string;
  ontaskchange: () => void;
};

let { action, open = $bindable(false), workspaceId, ontaskchange }: Props = $props();

let detail = $state<ActivityActionDetail | null>(null);
let tasks = $state<TaskRun[]>([]);
let loading = $state(false);
let cancellingTaskId = $state("");
let errorMessage = $state("");
let requestSequence = 0;

$effect(() => {
  const selectedAction = action;
  const activeWorkspaceId = workspaceId;
  if (!open || !selectedAction) return;
  const sequence = ++requestSequence;
  const controller = new AbortController();
  untrack(() => void loadDetail(selectedAction, activeWorkspaceId, sequence, controller.signal));
  return () => controller.abort();
});

async function loadDetail(
  selectedAction: ActivityAction,
  activeWorkspaceId: string,
  sequence: number,
  signal?: AbortSignal,
) {
  loading = true;
  errorMessage = "";
  detail = null;
  tasks = [];
  try {
    const actionDetail = await getActivityAction(activeWorkspaceId, selectedAction.actionId, signal);
    if (!ownsActionRequest(sequence, activeWorkspaceId, selectedAction.actionId)) return;
    detail = actionDetail;
    const taskIds = [actionDetail.action.writeTask.taskId, actionDetail.action.reconciliationTask?.taskId].filter(
      (taskId): taskId is string => Boolean(taskId),
    );
    const taskResults = await Promise.allSettled(taskIds.map((taskId) => getTask(activeWorkspaceId, taskId, signal)));
    if (!ownsActionRequest(sequence, activeWorkspaceId, selectedAction.actionId)) return;
    tasks = taskResults.flatMap((result) => (result.status === "fulfilled" ? [result.value] : []));
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (ownsActionRequest(sequence, activeWorkspaceId, selectedAction.actionId)) {
      errorMessage = error instanceof Error ? error.message : "Action detail could not be loaded";
    }
  } finally {
    if (ownsActionRequest(sequence, activeWorkspaceId, selectedAction.actionId)) loading = false;
  }
}

function ownsActionRequest(sequence: number, activeWorkspaceId: string, actionId: string): boolean {
  return sequence === requestSequence && open && workspaceId === activeWorkspaceId && action?.actionId === actionId;
}

function changeOpen(nextOpen: boolean) {
  open = nextOpen;
  if (nextOpen) return;
  requestSequence += 1;
  detail = null;
  tasks = [];
  cancellingTaskId = "";
  errorMessage = "";
}

function retry() {
  if (!action) return;
  const sequence = ++requestSequence;
  void loadDetail(action, workspaceId, sequence);
}

async function cancel(taskId: string) {
  const selectedAction = action;
  if (cancellingTaskId || !selectedAction) return;
  const sequence = requestSequence;
  const activeWorkspaceId = workspaceId;
  const actionId = selectedAction.actionId;
  cancellingTaskId = taskId;
  errorMessage = "";
  try {
    const updated = await cancelTask(activeWorkspaceId, taskId);
    if (!ownsTaskCommand(sequence, activeWorkspaceId, actionId, taskId)) return;
    tasks = tasks.map((task) => (task.id === taskId ? updated : task));
    ontaskchange();
  } catch (error) {
    if (ownsTaskCommand(sequence, activeWorkspaceId, actionId, taskId)) {
      errorMessage = error instanceof Error ? error.message : "Task cancellation could not be requested";
    }
  } finally {
    if (ownsTaskCommand(sequence, activeWorkspaceId, actionId, taskId)) cancellingTaskId = "";
  }
}

function ownsTaskCommand(sequence: number, activeWorkspaceId: string, actionId: string, taskId: string): boolean {
  return ownsActionRequest(sequence, activeWorkspaceId, actionId) && cancellingTaskId === taskId;
}

function formatTimestamp(value: string | null | undefined): string {
  if (!value) return "Not recorded";
  const timestamp = new Date(value);
  if (!Number.isFinite(timestamp.valueOf())) return "Unknown";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(timestamp);
}

function shortIdentity(value: string): string {
  const digest = value.includes(":") ? (value.split(":").at(-1) ?? value) : value;
  return `${digest.slice(0, 12)}${digest.length > 12 ? "..." : ""}`;
}

function formatBytes(value: number[] | null | undefined): string {
  if (!value?.length) return "None";
  return value.map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function formatSubject(subject: NonNullable<ActivityActionDetail["provenance"]>["attempt"]["receiverSubject"]): string {
  return `${subject.key.namespace}:${formatBytes(subject.key.value)}`;
}

function statusTone(status: string): "neutral" | "accent" | "success" | "warning" | "danger" | "info" {
  if (["succeeded", "applied", "no_op", "confirmed_applied"].includes(status)) return "success";
  if (["failed", "precondition_failed", "retryable_failure"].includes(status)) return "danger";
  if (["partial", "indeterminate", "cancelling"].includes(status)) return "warning";
  if (["queued", "running"].includes(status)) return "accent";
  if (status === "cancelled") return "neutral";
  return "info";
}

function guidanceLabel(value: string): string {
  return value.replaceAll("_", " ");
}
</script>

<Dialog.Root
  onOpenChange={changeOpen}
  bind:open
>
  <Dialog.Drawer style={open ? "translate: none" : undefined}>
    <header class="flex shrink-0 items-start justify-between gap-4 border-b border-border-strong px-4 py-4 sm:px-5">
      <div class="min-w-0">
        <Dialog.Title class="text-title font-semibold text-text-primary">Sync action detail</Dialog.Title>
        <Dialog.Description class="mt-1 truncate font-mono text-meta text-text-muted">
          {action?.actionId ?? "Loading action"}
        </Dialog.Description>
      </div>
      <IconButton
        label="Close sync action detail"
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
        Loading action evidence...
      </div>
    {:else if errorMessage && !detail}
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
    {:else if detail}
      <div class="min-h-0 flex-1 overflow-y-auto">
        {#if errorMessage}
          <div class="border-b border-danger/20 bg-danger/6 px-4 py-3 sm:px-5">
            <p
              class="text-label text-danger"
              role="alert"
            >
              {errorMessage}
            </p>
          </div>
        {/if}

        <section class="border-b border-border-subtle px-4 py-4 sm:px-5">
          <div class="flex flex-wrap items-center gap-2">
            <Chip
              dot
              tone={statusTone(detail.action.status)}
              >{guidanceLabel(detail.action.status)}</Chip
            >
            <Chip tone="neutral">{detail.action.semanticField}</Chip>
          </div>
          <dl class="mt-4 grid gap-3 text-label sm:grid-cols-2">
            <div>
              <dt class="text-meta font-semibold uppercase text-text-disabled">Receiver source</dt>
              <dd class="mt-1 font-mono text-text-secondary">{detail.action.receiverSourceId}</dd>
            </div>
            <div>
              <dt class="text-meta font-semibold uppercase text-text-disabled">Updated</dt>
              <dd class="mt-1 text-text-secondary">{formatTimestamp(detail.action.updatedAt)}</dd>
            </div>
            <div>
              <dt class="text-meta font-semibold uppercase text-text-disabled">Action hash</dt>
              <dd class="mt-1 font-mono text-text-secondary">{shortIdentity(detail.action.actionHash)}</dd>
            </div>
            <div>
              <dt class="text-meta font-semibold uppercase text-text-disabled">Reconciliation</dt>
              <dd class="mt-1 capitalize text-text-secondary">{guidanceLabel(detail.reconciliationGuidance)}</dd>
            </div>
          </dl>
        </section>

        <section class="border-b border-border-subtle px-4 py-4 sm:px-5">
          <h2 class="text-label font-semibold text-text-primary">Tasks</h2>
          <div class="mt-3 divide-y divide-border-subtle border-y border-border-subtle">
            {#each tasks as task (task.id)}
              <div class="flex min-h-12 items-center justify-between gap-3 py-2.5">
                <div class="min-w-0">
                  <div class="flex items-center gap-2">
                    <span class="truncate text-label font-medium text-text-primary">{task.kind}</span>
                    <Chip
                      dot
                      tone={statusTone(task.status)}
                      >{task.status}</Chip
                    >
                  </div>
                  <p class="mt-1 truncate font-mono text-meta text-text-muted">{task.id}</p>
                </div>
                {#if task.cancellationSupported}
                  <Button
                    disabled={Boolean(cancellingTaskId)}
                    onclick={() => cancel(task.id)}
                    size="sm"
                    variant="danger"
                  >
                    {#if cancellingTaskId === task.id}
                      <Spinner size={14} />
                    {:else}
                      <Ban
                        aria-hidden="true"
                        size={14}
                      />
                    {/if}
                    Cancel
                  </Button>
                {/if}
              </div>
            {:else}
              <p class="py-3 text-label text-text-muted">Task execution evidence is no longer available.</p>
            {/each}
          </div>
        </section>

        {#if detail.provenance}
          {@const provenance = detail.provenance}
          <section class="border-b border-border-subtle px-4 py-4 sm:px-5">
            <h2 class="text-label font-semibold text-text-primary">Decision chain</h2>
            <dl class="mt-3 grid gap-3 text-label sm:grid-cols-2">
              <div>
                <dt class="text-meta font-semibold uppercase text-text-disabled">Policy</dt>
                <dd class="mt-1 text-text-secondary">Revision {provenance.policyRevision}</dd>
                <dd class="mt-0.5 font-mono text-meta text-text-muted">
                  {shortIdentity(provenance.policyContentHash)}
                </dd>
              </div>
              <div>
                <dt class="text-meta font-semibold uppercase text-text-disabled">Preview</dt>
                <dd class="mt-1 font-mono text-text-secondary">{shortIdentity(provenance.previewHash)}</dd>
              </div>
              <div>
                <dt class="text-meta font-semibold uppercase text-text-disabled">Reducer</dt>
                <dd class="mt-1 text-text-secondary">{guidanceLabel(provenance.reducer)}</dd>
              </div>
              <div>
                <dt class="text-meta font-semibold uppercase text-text-disabled">Membership</dt>
                <dd class="mt-1 text-text-secondary">{guidanceLabel(provenance.membershipEffect)}</dd>
              </div>
            </dl>
          </section>

          <section class="border-b border-border-subtle px-4 py-4 sm:px-5">
            <h2 class="text-label font-semibold text-text-primary">Contributors</h2>
            <div class="mt-3 divide-y divide-border-subtle border-y border-border-subtle">
              {#each provenance.contributors as contributor (contributor.evidenceId)}
                <div class="py-3 text-label">
                  <div class="flex flex-wrap items-center justify-between gap-2">
                    <span class="font-mono text-text-primary">{shortIdentity(contributor.evidenceId)}</span>
                    <Chip tone="neutral">{contributor.presence}</Chip>
                  </div>
                  <p class="mt-1 font-mono text-meta text-text-muted">
                    Publication {shortIdentity(contributor.statePublicationId)} · proof
                    {shortIdentity(contributor.mappingProofKey)}
                  </p>
                </div>
              {/each}
            </div>
          </section>

          <section class="border-b border-border-subtle px-4 py-4 sm:px-5">
            <h2 class="text-label font-semibold text-text-primary">Mapping proofs</h2>
            <div class="mt-3 divide-y divide-border-subtle border-y border-border-subtle">
              {#each provenance.mappingProofs as proof (proof.key)}
                <div class="py-3 text-label">
                  <p class="font-mono text-text-primary">{shortIdentity(proof.key)}</p>
                  <p class="mt-1 font-mono text-meta text-text-muted">
                    Revision {shortIdentity(proof.revisionHash)} · row {shortIdentity(proof.rowHash)}
                  </p>
                </div>
              {/each}
            </div>
          </section>

          <section class="border-b border-border-subtle px-4 py-4 sm:px-5">
            <h2 class="text-label font-semibold text-text-primary">Translation</h2>
            <dl class="mt-3 grid gap-3 text-label sm:grid-cols-2">
              <div>
                <dt class="text-meta font-semibold uppercase text-text-disabled">Exact value</dt>
                <dd class="mt-1 text-text-secondary">
                  {provenance.translation.exactValueNumerator}/{provenance.translation.exactValueDenominator}
                  {provenance.translation.unit}
                </dd>
              </div>
              <div>
                <dt class="text-meta font-semibold uppercase text-text-disabled">Intended value</dt>
                <dd class="mt-1 text-text-secondary">{provenance.translation.intendedValue}</dd>
              </div>
              <div>
                <dt class="text-meta font-semibold uppercase text-text-disabled">Quantizer</dt>
                <dd class="mt-1 text-text-secondary">{provenance.translation.quantizer}</dd>
              </div>
              <div>
                <dt class="text-meta font-semibold uppercase text-text-disabled">Lossy</dt>
                <dd class="mt-1 text-text-secondary">{provenance.translation.lossy ? "Yes" : "No"}</dd>
              </div>
            </dl>
          </section>

          <section class="border-b border-border-subtle px-4 py-4 sm:px-5">
            <h2 class="text-label font-semibold text-text-primary">Provider attempt</h2>
            <dl class="mt-3 grid gap-3 text-label sm:grid-cols-2">
              <div>
                <dt class="text-meta font-semibold uppercase text-text-disabled">Receiver</dt>
                <dd class="mt-1 break-all font-mono text-text-secondary">
                  {formatSubject(provenance.attempt.receiverSubject)}
                </dd>
              </div>
              <div>
                <dt class="text-meta font-semibold uppercase text-text-disabled">State</dt>
                <dd class="mt-1 text-text-secondary">{guidanceLabel(provenance.attempt.state)}</dd>
              </div>
              <div>
                <dt class="text-meta font-semibold uppercase text-text-disabled">Prepared</dt>
                <dd class="mt-1 text-text-secondary">{formatTimestamp(provenance.attempt.preparedAt)}</dd>
              </div>
              <div>
                <dt class="text-meta font-semibold uppercase text-text-disabled">Settled</dt>
                <dd class="mt-1 text-text-secondary">{formatTimestamp(provenance.attempt.settledAt)}</dd>
              </div>
            </dl>
            {#if provenance.attempt.outcome}
              <div class="mt-4 border-t border-border-subtle pt-3 text-label">
                <div class="flex flex-wrap items-center gap-2">
                  <Chip
                    dot
                    tone={statusTone(provenance.attempt.outcome.status)}
                    >{guidanceLabel(provenance.attempt.outcome.status)}</Chip
                  >
                  <span class="text-text-muted">{guidanceLabel(provenance.attempt.outcome.certainty)}</span>
                </div>
                <p class="mt-2 text-text-secondary">
                  Membership {guidanceLabel(provenance.attempt.outcome.membershipEffect)} · retry
                  {guidanceLabel(provenance.attempt.outcome.retryDisposition)}
                </p>
                {#if provenance.attempt.outcome.error}
                  <Panel
                    class="mt-3 px-3 py-2.5 text-label text-danger"
                    tone="danger"
                  >
                    <div class="flex items-start gap-2">
                      <CircleAlert
                        aria-hidden="true"
                        class="mt-0.5 shrink-0"
                        size={14}
                      />
                      <span>{provenance.attempt.outcome.error.safeMessage}</span>
                    </div>
                  </Panel>
                {/if}
              </div>
            {/if}
          </section>

          {#if provenance.reconciliation}
            <section class="px-4 py-4 sm:px-5">
              <h2 class="text-label font-semibold text-text-primary">Reconciliation</h2>
              <dl class="mt-3 grid gap-3 text-label sm:grid-cols-2">
                <div>
                  <dt class="text-meta font-semibold uppercase text-text-disabled">Result</dt>
                  <dd class="mt-1 text-text-secondary">{guidanceLabel(provenance.reconciliation.result)}</dd>
                </div>
                <div>
                  <dt class="text-meta font-semibold uppercase text-text-disabled">Outcome</dt>
                  <dd class="mt-1 text-text-secondary">{guidanceLabel(provenance.reconciliation.outcomeKind)}</dd>
                </div>
                <div>
                  <dt class="text-meta font-semibold uppercase text-text-disabled">Observed</dt>
                  <dd class="mt-1 text-text-secondary">{formatTimestamp(provenance.reconciliation.observedAt)}</dd>
                </div>
                <div>
                  <dt class="text-meta font-semibold uppercase text-text-disabled">Value</dt>
                  <dd class="mt-1 text-text-secondary">{provenance.reconciliation.observedValue ?? "Not observed"}</dd>
                </div>
              </dl>
            </section>
          {/if}
        {:else}
          <section class="px-4 py-6 sm:px-5">
            <Panel
              class="flex items-start gap-2.5 px-3 py-2.5 text-label text-warning"
              tone="warning"
            >
              <CircleAlert
                aria-hidden="true"
                class="mt-0.5 shrink-0"
                size={14}
              />
              Provenance has not been recorded for this action yet.
            </Panel>
          </section>
        {/if}
      </div>
    {/if}
  </Dialog.Drawer>
</Dialog.Root>
