<script lang="ts">
import { Plus, RefreshCw, Server, X } from "@lucide/svelte";
import { onDestroy, onMount } from "svelte";
import { page } from "$app/state";
import { Button, EmptyState, PageHeader, Panel, Spinner } from "$lib";
import { type Connection, type ConnectionSource, listConnectionSources, listConnections } from "$lib/api/connections";
import { listProviders } from "$lib/api/providers";
import { listWorkspaces, type Workspace } from "$lib/api/workspaces";
import ConnectionCreation from "./ConnectionCreation.svelte";
import ConnectionDeletion from "./ConnectionDeletion.svelte";
import ConnectionEntry from "./ConnectionEntry.svelte";

type DirectoryEntry = { connection: Connection; sources: ConnectionSource[] };
let entries = $state<DirectoryEntry[]>([]);
let workspaces = $state<Workspace[]>([]);
let providers = $state<{ value: string; label: string }[]>([]);
let showComposer = $state(false);
let expandedId = $state("");
let renamingId = $state("");
let deleting = $state<Connection | null>(null);
let deletionTrigger: HTMLElement | null = null;
let loading = $state(true);
let loaded = $state(false);
let error = $state("");
let sequence = 0;
let revision = 0;
const controller = new AbortController();
let workspace = $derived(
  workspaces.find((candidate) => candidate.id === (page.url.searchParams.get("workspace") ?? workspaces[0]?.id)),
);

onMount(() => {
  void loadDirectory();
});
onDestroy(() => {
  sequence += 1;
  controller.abort();
});

async function loadDirectory() {
  const current = ++sequence;
  loading = true;
  error = "";
  try {
    const [connections, nextWorkspaces, nextProviders] = await Promise.all([
      listConnections(controller.signal),
      listWorkspaces(controller.signal),
      listProviders(controller.signal),
    ]);
    const results = await Promise.allSettled(
      connections.map(async (connection) => ({
        connection,
        sources: await listConnectionSources(connection.id, controller.signal),
      })),
    );
    if (controller.signal.aborted || current !== sequence) return;
    entries = connections.map((connection, index) => {
      const result = results[index];
      return result.status === "fulfilled" ? result.value : { connection, sources: [] };
    });
    workspaces = nextWorkspaces;
    providers = nextProviders.map((provider) => ({
      value: provider.providerId,
      label: `${provider.name ?? provider.providerId} ${provider.version}`,
    }));
    showComposer = connections.length === 0;
    loaded = true;
  } catch (cause) {
    if (!controller.signal.aborted && current === sequence)
      error = cause instanceof Error ? cause.message : "Connections could not be loaded";
  } finally {
    if (!controller.signal.aborted && current === sequence) loading = false;
  }
}

function created(connection: Connection, reveal: boolean) {
  revision += 1;
  entries = [...entries, { connection, sources: [] }];
  if (reveal) expandedId = connection.id;
}

function configured(connection: Connection) {
  revision += 1;
  entries = entries.map((entry) =>
    entry.connection.id === connection.id
      ? { ...entry, connection: { ...entry.connection, status: connection.status } }
      : entry,
  );
}

function renamed(connection: Connection) {
  revision += 1;
  entries = entries.map((entry) =>
    entry.connection.id === connection.id
      ? { ...entry, connection: { ...entry.connection, displayName: connection.displayName } }
      : entry,
  );
}

function deleted(id: string) {
  revision += 1;
  entries = entries.filter((entry) => entry.connection.id !== id);
  if (expandedId === id) expandedId = "";
  if (renamingId === id) renamingId = "";
}

async function refreshConnections(entrySignal: AbortSignal) {
  const snapshotRevision = revision;
  const current = ++sequence;
  const signal = AbortSignal.any([controller.signal, entrySignal]);
  const connections = await listConnections(signal);
  if (signal.aborted || current !== sequence || snapshotRevision !== revision) return;
  entries = connections.map((connection) => ({
    connection,
    sources: entries.find((entry) => entry.connection.id === connection.id)?.sources ?? [],
  }));
}

function workspaceUpdated(updated: Workspace, sourceId: string) {
  const attachment = updated.sources.find((source) => source.sourceId === sourceId);
  workspaces = workspaces.map((candidate) =>
    candidate.id === updated.id
      ? {
          ...candidate,
          sources: [
            ...candidate.sources.filter((source) => source.sourceId !== sourceId),
            ...(attachment ? [attachment] : []),
          ],
        }
      : candidate,
  );
  window.dispatchEvent(new Event("trakkin:workspaces-changed"));
}

function restoreDeletionFocus() {
  const target = deletionTrigger?.isConnected
    ? deletionTrigger
    : document.getElementById("connections-composer-toggle");
  target?.focus();
}

function entryAction(connection: Connection, action: "expand" | "rename" | "cancelRename" | "delete") {
  switch (action) {
    case "expand":
      expandedId = expandedId === connection.id ? "" : connection.id;
      break;
    case "rename":
      renamingId = connection.id;
      break;
    case "cancelRename":
      if (renamingId === connection.id) renamingId = "";
      break;
    case "delete":
      deletionTrigger = document.activeElement instanceof HTMLElement ? document.activeElement : null;
      deleting = connection;
      break;
  }
}
</script>

<div class="flex h-full min-h-0 flex-col">
  <PageHeader title="Connections">
    {#snippet actions()}
      {#if loaded}
        <Button
          aria-expanded={showComposer}
          id="connections-composer-toggle"
          onclick={() => (showComposer = !showComposer)}
          size="sm"
          variant={showComposer ? "ghost" : "secondary"}
        >
          {#if showComposer}
            <X
              aria-hidden="true"
              size={14}
            />Close
          {:else}
            <Plus
              aria-hidden="true"
              size={14}
            />New connection
          {/if}
        </Button>
      {/if}
    {/snippet}
  </PageHeader>
  <div class="min-h-0 flex-1 overflow-auto">
    <div class="mx-auto grid w-full max-w-5xl gap-4 p-4 sm:p-6">
      {#if loaded}
        <div class="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p class="text-label text-text-muted">
              {entries.length} {entries.length === 1 ? "connection" : "connections"}
            </p>
            {#if workspace}
              <p class="mt-0.5 text-meta text-text-disabled">Attaching sources to {workspace.name}</p>
            {/if}
          </div>
        </div>
      {/if}
      {#if error}
        <Panel
          class="grid min-h-48 place-items-center px-4 py-6 text-center text-label text-danger"
          role="alert"
          tone="danger"
        >
          <div>
            <p>{error}</p>
            <Button
              class="mt-4"
              onclick={loadDirectory}
              size="sm"
              ><RefreshCw
                aria-hidden="true"
                size={14}
              />Retry</Button
            >
          </div>
        </Panel>
      {/if}
      {#if loaded}
        <ConnectionCreation
          onclose={() => (showComposer = false)}
          oncreated={created}
          open={showComposer}
          {providers}
        />
      {/if}
      {#if loading && !loaded}
        <div
          class="flex min-h-48 items-center justify-center gap-2 text-label text-text-muted"
          role="status"
        >
          <Spinner size={16} />Loading connections...
        </div>
      {:else if loaded && entries.length === 0 && !showComposer}
        <EmptyState
          icon={Server}
          title="No connections"
        >
          {#snippet actions()}
            <Button
              onclick={() => (showComposer = true)}
              size="sm"
              ><Plus
                aria-hidden="true"
                size={14}
              />New connection</Button
            >
          {/snippet}
        </EmptyState>
      {:else if loaded}
        <div class="grid gap-3">
          {#each entries as entry (entry.connection.id)}
            <ConnectionEntry
              connection={entry.connection}
              expanded={expandedId === entry.connection.id}
              initialSources={entry.sources}
              onaction={(action) => entryAction(entry.connection, action)}
              onconfigured={configured}
              onrefresh={refreshConnections}
              onrenamed={renamed}
              onworkspaceupdated={workspaceUpdated}
              renaming={renamingId === entry.connection.id}
              {workspace}
            />
          {/each}
        </div>
      {/if}
    </div>
  </div>
</div>
<ConnectionDeletion
  connection={deleting}
  onclose={() => (deleting = null)}
  ondeleted={deleted}
  onrestorefocus={restoreDeletionFocus}
/>
