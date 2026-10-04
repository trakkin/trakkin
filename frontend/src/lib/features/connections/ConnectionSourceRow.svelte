<script lang="ts">
import { RefreshCw } from "@lucide/svelte";
import { onDestroy } from "svelte";
import { Button, Checkbox } from "$lib";
import { request } from "$lib/api/client";
import type { ConnectionSource } from "$lib/api/connections";
import { pollCatalog, waitForTask } from "$lib/api/operations";
import type { Workspace } from "$lib/api/workspaces";

let {
  source,
  workspace,
  onworkspaceupdated,
}: {
  source: ConnectionSource;
  workspace?: Workspace;
  onworkspaceupdated: (workspace: Workspace, sourceId: string) => void;
} = $props();
let scanning = $state(false);
let attaching = $state(false);
let error = $state("");
let errorWorkspace = $state("");
let attachment = $derived(workspace?.sources.find((candidate) => candidate.sourceId === source.id));
const controller = new AbortController();
onDestroy(() => controller.abort());

async function changeAttachment(enabled: boolean) {
  const target = workspace;
  if (!target || attaching || (enabled && source.availability !== "available")) return;
  attaching = true;
  error = "";
  errorWorkspace = target.id;
  try {
    const updated = await request<Workspace>(`/api/v1/workspaces/${target.id}/sources/${source.id}`, {
      method: enabled ? "PUT" : "DELETE",
      signal: controller.signal,
    });
    if (!controller.signal.aborted) onworkspaceupdated(updated, source.id);
  } catch (cause) {
    if (!controller.signal.aborted)
      error = cause instanceof Error ? cause.message : "The attachment could not be updated";
  } finally {
    if (!controller.signal.aborted) attaching = false;
  }
}

async function scan() {
  const target = workspace;
  if (!target || !attachment || scanning || attaching || source.availability !== "available") return;
  scanning = true;
  error = "";
  errorWorkspace = target.id;
  try {
    const signal = controller.signal;
    const result = await waitForTask(target.id, await pollCatalog(target.id, source.id, signal), signal);
    if (result.status !== "succeeded") {
      throw new Error(
        result.status === "cancelled"
          ? "The catalog scan was cancelled."
          : "The catalog scan did not finish. Check the source connection and try again.",
      );
    }
  } catch (cause) {
    if (!controller.signal.aborted) error = cause instanceof Error ? cause.message : "The catalog scan failed";
  } finally {
    if (!controller.signal.aborted) scanning = false;
  }
}
</script>

<div class="grid gap-3 px-4 py-3 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-center sm:px-5">
  <div class="min-w-0">
    <p class="truncate text-label font-[510] text-text-secondary">{source.displayName}</p>
    <p class="mt-1 text-meta text-text-disabled">
      {source.kind.name.replaceAll("_", " ")}
      / {source.availability} / {source.accountDisplayName}
    </p>
  </div>
  <div class="flex flex-wrap items-center justify-end gap-3">
    <Button
      disabled={scanning || attaching || !workspace || !attachment || source.availability !== "available"}
      onclick={scan}
      size="sm"
      variant="ghost"
    >
      <RefreshCw
        aria-hidden="true"
        class={scanning ? "animate-spin" : ""}
        size={13}
      />Scan
    </Button>
    <div class="flex items-center gap-2 text-label text-text-muted">
      <Checkbox
        aria-label={`Attach ${source.displayName} to the workspace`}
        checked={Boolean(attachment)}
        disabled={attaching || !workspace || (!attachment && source.availability !== "available")}
        onCheckedChange={changeAttachment}
      />
      <span>Attached</span>
    </div>
  </div>
  {#if error && errorWorkspace === workspace?.id}
    <p
      class="text-label text-danger sm:col-span-2"
      role="alert"
    >
      {error}
    </p>
  {/if}
</div>
