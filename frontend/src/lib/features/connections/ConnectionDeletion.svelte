<script lang="ts">
import { onDestroy } from "svelte";
import { AlertDialog, Spinner } from "$lib";
import {
  type Connection,
  type ConnectionDeletionImpact,
  deleteConnection,
  getConnectionDeletionImpact,
} from "$lib/api/connections";

let {
  connection,
  onclose,
  ondeleted,
  onrestorefocus,
}: {
  connection: Connection | null;
  onclose: () => void;
  ondeleted: (id: string) => void;
  onrestorefocus: () => void;
} = $props();
let open = $state(false);
let impact = $state<ConnectionDeletionImpact>();
let loading = $state(false);
let deleting = $state(false);
let error = $state("");
let session = 0;
const controller = new AbortController();
onDestroy(() => controller.abort());

$effect(() => {
  const target = connection;
  const current = ++session;
  open = Boolean(target);
  impact = undefined;
  deleting = false;
  error = "";
  loading = Boolean(target);
  if (!target) return;
  const inspectionController = new AbortController();
  void getConnectionDeletionImpact(target.id, inspectionController.signal)
    .then((result) => {
      if (current === session && !controller.signal.aborted) impact = result;
    })
    .catch((cause: unknown) => {
      if (current === session && !inspectionController.signal.aborted && !controller.signal.aborted) {
        error = cause instanceof Error ? cause.message : "Deletion impact could not be loaded";
      }
    })
    .finally(() => {
      if (current === session && !controller.signal.aborted) loading = false;
    });
  return () => inspectionController.abort();
});

async function confirm() {
  const target = connection;
  if (!target || !impact || impact.activeTaskCount > 0 || deleting) return;
  const current = session;
  deleting = true;
  error = "";
  try {
    await deleteConnection(target.id, controller.signal);
    if (controller.signal.aborted) return;
    ondeleted(target.id);
    if (current === session) onclose();
  } catch (cause) {
    if (current === session && !controller.signal.aborted) {
      error = cause instanceof Error ? cause.message : "The connection could not be deleted";
    }
  } finally {
    if (current === session && !controller.signal.aborted) deleting = false;
  }
}
</script>

<AlertDialog.Root
  onOpenChange={(value) => { if (!value) onclose(); }}
  bind:open
>
  <AlertDialog.Content onCloseAutoFocus={(event) => { event.preventDefault(); onrestorefocus(); }}>
    <AlertDialog.Title class="text-title font-semibold text-text-primary"
      >Delete connection permanently?</AlertDialog.Title
    >
    <AlertDialog.Description class="mt-1 text-label leading-5 text-text-muted">
      {connection?.displayName ?? "This connection"}
      and its stored authentication data will be permanently removed.
    </AlertDialog.Description>
    {#if loading}
      <div
        class="mt-4 flex items-center gap-2 text-label text-text-muted"
        role="status"
      >
        <Spinner size={14} />Checking deletion impact...
      </div>
    {:else if impact}
      <p class="mt-4 text-label leading-5 text-text-secondary">
        This removes {impact.sourceCount} sources, {impact.workspaceAttachmentCount} workspace attachments, and
        {impact.taskCount}
        durable tasks.
        {#if impact.activeTaskCount > 0}
          <span class="text-danger">Active work must finish first.</span>
        {/if}
      </p>
    {/if}
    {#if error}
      <p
        class="mt-4 text-label text-danger"
        role="alert"
      >
        {error}
      </p>
    {/if}
    <div class="mt-5 flex justify-end gap-2">
      <AlertDialog.Cancel
        class="inline-flex h-8 items-center rounded-md border border-border-subtle px-3 text-label text-text-secondary hover:bg-white/4"
        >Cancel</AlertDialog.Cancel
      >
      <AlertDialog.Action
        class="inline-flex h-8 items-center gap-1.5 rounded-md border border-danger/30 bg-danger/8 px-3 text-label text-danger hover:bg-danger/12 disabled:pointer-events-none disabled:opacity-35"
        disabled={!impact || impact.activeTaskCount > 0 || deleting}
        onclick={(event) => { event.preventDefault(); void confirm(); }}
      >
        {#if deleting}
          <Spinner size={13} />
        {/if}
        Delete permanently
      </AlertDialog.Action>
    </div>
  </AlertDialog.Content>
</AlertDialog.Root>
