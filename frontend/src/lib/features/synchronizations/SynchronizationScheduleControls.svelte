<script lang="ts">
import { RefreshCw, Save } from "@lucide/svelte";
import { untrack } from "svelte";
import { Button, Panel, Spinner, Switch, TextInput } from "$lib";
import { ApiRequestError } from "$lib/api/client";
import {
  enableSynchronizationSchedule,
  getSynchronizationSchedule,
  pauseSynchronizationSchedule,
  type SynchronizationSchedule,
} from "$lib/api/synchronizations";

type Props = {
  policyId: string;
  workspaceId: string;
};

const minimumIntervalMinutes = 1;
const maximumIntervalMinutes = 31 * 24 * 60;

let { policyId, workspaceId }: Props = $props();

let schedule = $state<SynchronizationSchedule | null>(null);
let intervalMinutes = $state(60);
let loading = $state(false);
let saving = $state(false);
let errorMessage = $state("");
let requestSequence = 0;

let intervalSeconds = $derived(Math.round(Number(intervalMinutes) * 60));
let intervalValid = $derived(
  Number.isInteger(intervalSeconds) &&
    intervalSeconds >= minimumIntervalMinutes * 60 &&
    intervalSeconds <= maximumIntervalMinutes * 60,
);
let enabled = $derived(schedule?.state === "enabled");
let intervalChanged = $derived(schedule !== null && schedule.intervalSeconds !== intervalSeconds);
let stateLabel = $derived(schedule === null ? "Not configured" : enabled ? "Enabled" : "Paused");

$effect(() => {
  if (!workspaceId || !policyId) return;
  const controller = new AbortController();
  untrack(() => reloadSchedule(controller.signal));
  return () => controller.abort();
});

function reloadSchedule(signal = new AbortController().signal): void {
  const sequence = ++requestSequence;
  schedule = null;
  intervalMinutes = 60;
  void loadSchedule(sequence, signal);
}

async function loadSchedule(sequence: number, signal: AbortSignal) {
  loading = true;
  saving = false;
  errorMessage = "";
  try {
    const loaded = await getSynchronizationSchedule(workspaceId, policyId, signal);
    if (sequence !== requestSequence) return;
    applySchedule(loaded);
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (sequence !== requestSequence) return;
    if (error instanceof ApiRequestError && error.code === "sync_schedule_not_found") {
      schedule = null;
      intervalMinutes = 60;
      return;
    }
    errorMessage = error instanceof Error ? error.message : "Recurring schedule could not be loaded";
  } finally {
    if (sequence === requestSequence) loading = false;
  }
}

async function changeEnabled(nextEnabled: boolean) {
  if (saving || nextEnabled === enabled || (nextEnabled && !intervalValid)) return;
  const sequence = requestSequence;
  saving = true;
  errorMessage = "";
  try {
    const updated = nextEnabled
      ? await enableSynchronizationSchedule(workspaceId, policyId, intervalSeconds)
      : await pauseSynchronizationSchedule(workspaceId, policyId);
    if (sequence === requestSequence) applySchedule(updated);
  } catch (error) {
    if (sequence === requestSequence) {
      errorMessage = error instanceof Error ? error.message : "Recurring schedule could not be updated";
    }
  } finally {
    if (sequence === requestSequence) saving = false;
  }
}

async function saveInterval() {
  if (!enabled || !intervalValid || !intervalChanged || saving) return;
  const sequence = requestSequence;
  saving = true;
  errorMessage = "";
  try {
    const updated = await enableSynchronizationSchedule(workspaceId, policyId, intervalSeconds);
    if (sequence === requestSequence) applySchedule(updated);
  } catch (error) {
    if (sequence === requestSequence) {
      errorMessage = error instanceof Error ? error.message : "Recurring interval could not be updated";
    }
  } finally {
    if (sequence === requestSequence) saving = false;
  }
}

function applySchedule(nextSchedule: SynchronizationSchedule) {
  schedule = nextSchedule;
  intervalMinutes = nextSchedule.intervalSeconds / 60;
}

function formatTimestamp(value: string | null | undefined): string {
  if (!value) return "No run scheduled";
  const timestamp = new Date(value);
  if (!Number.isFinite(timestamp.valueOf())) return "Unknown next run";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(timestamp);
}
</script>

<section class="border-b border-border-subtle py-4">
  <div class="flex items-start justify-between gap-4">
    <div class="min-w-0">
      <h2 class="text-label font-semibold text-text-primary">Recurring schedule</h2>
      <p
        aria-live="polite"
        class="mt-1 flex items-center gap-1.5 text-meta text-text-muted"
      >
        {#if loading}
          <Spinner size={12} />
          Loading schedule...
        {:else}
          {stateLabel}
          {#if enabled}
            <span aria-hidden="true">&middot;</span>
            <span>Next {formatTimestamp(schedule?.nextRunAt)}</span>
          {/if}
        {/if}
      </p>
    </div>
    <Switch
      aria-label="Recurring synchronization"
      checked={enabled}
      disabled={loading || saving || (!enabled && !intervalValid)}
      onCheckedChange={(checked) => void changeEnabled(checked)}
    />
  </div>

  <div class="mt-3 flex flex-wrap items-end gap-2">
    <label
      class="min-w-36 flex-1 text-meta font-medium text-text-secondary"
      for="synchronization-schedule-interval"
    >
      Interval in minutes
      <TextInput
        aria-invalid={!intervalValid}
        aria-label="Schedule interval in minutes"
        class="mt-1"
        disabled={loading || saving}
        id="synchronization-schedule-interval"
        max={maximumIntervalMinutes}
        min={minimumIntervalMinutes}
        step="any"
        type="number"
        bind:value={intervalMinutes}
      />
    </label>
    {#if enabled}
      <Button
        disabled={!intervalValid || !intervalChanged || saving}
        onclick={saveInterval}
        size="sm"
        variant="secondary"
      >
        {#if saving}
          <Spinner size={14} />
        {:else}
          <Save
            aria-hidden="true"
            size={14}
          />
        {/if}
        Save interval
      </Button>
    {/if}
  </div>

  {#if errorMessage}
    <Panel
      class="mt-3 flex items-center justify-between gap-3 px-3 py-2 text-meta text-danger"
      role="alert"
      tone="danger"
    >
      <p>{errorMessage}</p>
      <Button
        onclick={() => reloadSchedule()}
        size="sm"
        variant="ghost"
      >
        <RefreshCw
          aria-hidden="true"
          size={13}
        />
        Retry
      </Button>
    </Panel>
  {/if}
</section>
