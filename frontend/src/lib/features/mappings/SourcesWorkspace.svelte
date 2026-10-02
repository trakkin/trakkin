<script lang="ts">
import { ArrowDown, ArrowUp, Ellipsis, FilePenLine, GripVertical, Plus, RefreshCw, Trash2, X } from "@lucide/svelte";
import { onDestroy, onMount } from "svelte";
import {
  AlertDialog,
  Button,
  Dialog,
  DropdownMenu,
  IconButton,
  Panel,
  Spinner,
  Switch,
  Tabs,
  Textarea,
  TextInput,
} from "$lib";
import {
  createRemoteMappingSource,
  createUploadedMappingSource,
  getMappingState,
  type MappingSource,
  type MappingState,
  refreshRemoteMappingSource,
  removeMappingSource,
  reorderMappingSources,
  updateMappingSource,
} from "$lib/api/mappings";
import MappingsHeader from "./MappingsHeader.svelte";

let mappingState = $state<MappingState>();
let loading = $state(true);
let reloading = $state(false);
let errorMessage = $state("");
let pending = $state<Record<string, boolean>>({});
let editorOpen = $state(false);
let editorSource = $state<MappingSource>();
let editorSaving = $state(false);
let editorError = $state("");
let sourceKey = $state("");
let displayName = $state("");
let enabled = $state(true);
let sourceKind = $state<"upload" | "remote">("upload");
let remoteUrl = $state("");
let sourceContent = $state("");
let removeOpen = $state(false);
let removalSource = $state<MappingSource>();
let removalError = $state("");
let draggedSourceId = $state("");
let dropTargetSourceId = $state("");
let loadSequence = 0;
const lifecycleController = new AbortController();

let importedSources = $derived(mappingState?.sources.filter((source) => source.kind !== "local") ?? []);
let mutationPending = $derived(Object.values(pending).some(Boolean));
let changesPending = $derived(editorSaving || mutationPending);

onMount(() => {
  void loadState(lifecycleController.signal);
});

onDestroy(() => {
  loadSequence += 1;
  lifecycleController.abort();
});

async function loadState(signal?: AbortSignal): Promise<void> {
  const sequence = ++loadSequence;
  loading = !mappingState;
  reloading = Boolean(mappingState);
  errorMessage = "";
  try {
    const snapshot = await getMappingState(signal);
    if (sequence === loadSequence) mappingState = snapshot;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (sequence === loadSequence) errorMessage = messageFor(error, "Mapping sources could not be loaded");
  } finally {
    if (sequence === loadSequence) {
      loading = false;
      reloading = false;
    }
  }
}

async function runMutation(
  key: string,
  mutation: (signal: AbortSignal) => Promise<MappingState>,
  onError?: (message: string) => void,
): Promise<boolean> {
  const signal = lifecycleController.signal;
  if (!mappingState || signal.aborted || reloading || changesPending) return false;
  supersedeLoads();
  pending = { ...pending, [key]: true };
  errorMessage = "";
  try {
    mappingState = await mutation(signal);
    return true;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return false;
    const message = messageFor(error, "Mapping source operation failed");
    await loadState(signal);
    if (signal.aborted) return false;
    if (onError) onError(message);
    else errorMessage = message;
    return false;
  } finally {
    if (!signal.aborted) pending = { ...pending, [key]: false };
  }
}

async function toggleSource(source: MappingSource, nextEnabled: boolean): Promise<void> {
  if (source.kind === "local" || source.enabled === nextEnabled) return;
  await runMutation(`source:${source.id}:toggle`, (signal) =>
    updateMappingSource(source.id, { displayName: source.displayName, enabled: nextEnabled }, signal),
  );
}

async function moveSource(source: MappingSource, offset: -1 | 1): Promise<void> {
  const index = importedSources.findIndex((candidate) => candidate.id === source.id);
  const target = index + offset;
  if (index < 0 || target < 0 || target >= importedSources.length) return;
  const sourceIds = importedSources.map((candidate) => candidate.id);
  [sourceIds[index], sourceIds[target]] = [sourceIds[target], sourceIds[index]];
  await runMutation(`source:${source.id}:move`, (signal) => reorderMappingSources({ sourceIds }, signal));
}

function beginDrag(event: DragEvent, source: MappingSource): void {
  if (changesPending) return;
  draggedSourceId = source.id;
  dropTargetSourceId = "";
  event.dataTransfer?.setData("text/plain", source.id);
  if (event.dataTransfer) event.dataTransfer.effectAllowed = "move";
}

function allowDrop(event: DragEvent, target: MappingSource): void {
  if (!draggedSourceId || draggedSourceId === target.id || target.kind === "local") return;
  event.preventDefault();
  dropTargetSourceId = target.id;
  if (event.dataTransfer) event.dataTransfer.dropEffect = "move";
}

async function dropSource(event: DragEvent, target: MappingSource): Promise<void> {
  event.preventDefault();
  const sourceId = event.dataTransfer?.getData("text/plain") || draggedSourceId;
  finishDrag();
  if (changesPending || !sourceId || sourceId === target.id || target.kind === "local") return;
  const sourceIds = importedSources.map((source) => source.id);
  const sourceIndex = sourceIds.indexOf(sourceId);
  const targetIndex = sourceIds.indexOf(target.id);
  if (sourceIndex < 0 || targetIndex < 0) return;
  const movingDown = sourceIndex < targetIndex;
  const [moved] = sourceIds.splice(sourceIndex, 1);
  const adjustedTarget = sourceIds.indexOf(target.id);
  sourceIds.splice(adjustedTarget + (movingDown ? 1 : 0), 0, moved);
  await runMutation(`source:${sourceId}:move`, (signal) => reorderMappingSources({ sourceIds }, signal));
}

function finishDrag(): void {
  draggedSourceId = "";
  dropTargetSourceId = "";
}

function openCreateSource(): void {
  editorSource = undefined;
  sourceKey = "";
  displayName = "";
  enabled = true;
  sourceKind = "upload";
  remoteUrl = "";
  sourceContent = "";
  editorError = "";
  editorOpen = true;
}

function openSourceEditor(source: MappingSource): void {
  if (source.kind === "local") return;
  editorSource = source;
  sourceKey = source.sourceKey;
  displayName = source.displayName;
  enabled = source.enabled;
  sourceKind = source.kind;
  remoteUrl = source.remoteUrl ?? "";
  sourceContent = "";
  editorError = "";
  editorOpen = true;
}

async function saveSource(): Promise<void> {
  if (!mappingState || reloading || changesPending) return;
  supersedeLoads();
  editorSaving = true;
  editorError = "";
  try {
    if (editorSource) {
      mappingState = await updateMappingSource(
        editorSource.id,
        { displayName: displayName.trim(), enabled },
        lifecycleController.signal,
      );
    } else if (sourceKind === "remote") {
      mappingState = await createRemoteMappingSource(
        {
          sourceKey: sourceKey.trim(),
          displayName: displayName.trim(),
          remoteUrl: remoteUrl.trim(),
          enabled,
        },
        lifecycleController.signal,
      );
    } else {
      mappingState = await createUploadedMappingSource(
        { sourceKey: sourceKey.trim(), displayName: displayName.trim(), enabled },
        sourceContent,
        lifecycleController.signal,
      );
    }
    editorOpen = false;
  } catch (error) {
    editorError = messageFor(error, "Mapping source could not be saved");
    await loadState(lifecycleController.signal);
  } finally {
    editorSaving = false;
  }
}

async function refreshSource(source: MappingSource): Promise<void> {
  if (source.kind !== "remote") return;
  await runMutation(`source:${source.id}:refresh`, (signal) => refreshRemoteMappingSource(source.id, signal));
}

function requestRemoval(source: MappingSource): void {
  if (source.kind === "local") return;
  removalSource = source;
  removalError = "";
  removeOpen = true;
}

async function confirmRemoval(): Promise<void> {
  const source = removalSource;
  if (!source) return;
  const succeeded = await runMutation(
    `source:${source.id}:remove`,
    (signal) => removeMappingSource(source.id, signal),
    (message) => (removalError = message),
  );
  if (succeeded) {
    removeOpen = false;
    removalSource = undefined;
  }
}

function changeEditorOpen(open: boolean): void {
  if (editorSaving) return;
  editorOpen = open;
  if (!open) editorSource = undefined;
}

function changeRemoveOpen(open: boolean): void {
  if (removalPending()) return;
  removeOpen = open;
  if (!open) removalSource = undefined;
}

function supersedeLoads(): void {
  loadSequence += 1;
  loading = false;
  reloading = false;
}

function removalPending(): boolean {
  return Boolean(removalSource && pending[`source:${removalSource.id}:remove`]);
}

function messageFor(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}
</script>

<svelte:head><title>Trakkin &middot; Mapping sources</title></svelte:head>

<div class="flex h-full min-h-0 flex-col">
  <MappingsHeader>
    {#snippet actions()}
      <IconButton
        disabled={loading || reloading || changesPending}
        label="Refresh sources"
        onclick={() => void loadState()}
        size="sm"
        title="Refresh sources"
      >
        <span class={["inline-flex", reloading && "animate-spin"]}>
          <RefreshCw
            aria-hidden="true"
            size={14}
          />
        </span>
      </IconButton>
      <Button
        disabled={loading || reloading || changesPending}
        onclick={openCreateSource}
        size="sm"
        variant="primary"
      >
        <Plus
          aria-hidden="true"
          size={14}
        />
        Add source
      </Button>
    {/snippet}
  </MappingsHeader>

  {#if loading && !mappingState}
    <div
      class="flex min-h-48 flex-1 items-center justify-center gap-2 text-label text-text-muted"
      role="status"
    >
      <Spinner size={16} />
      Loading sources...
    </div>
  {:else if !mappingState}
    <div class="flex min-h-0 flex-1 p-4 sm:p-6">
      <Panel
        class="grid min-h-48 w-full place-items-center px-4 py-6 text-center text-label text-danger"
        tone="danger"
      >
        <div>
          <p>{errorMessage || "Mapping sources are unavailable"}</p>
          <Button
            class="mt-4"
            onclick={() => void loadState()}
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
    </div>
  {:else}
    <div class="min-h-0 flex-1 overflow-y-auto">
      <section aria-labelledby="source-chain-heading">
        <header class="flex items-center justify-between gap-4 border-b border-border-subtle px-4 py-3 sm:px-6">
          <h2
            class="text-label font-semibold text-text-primary"
            id="source-chain-heading"
          >
            Source precedence
          </h2>
          <span class="text-meta text-text-muted">{mappingState.sources.length} sources</span>
        </header>
        <ol class="divide-y divide-border-subtle">
          {#each mappingState.sources as source, sourceIndex (source.id)}
            {@const importedIndex = importedSources.findIndex((candidate) => candidate.id === source.id)}
            <li
              class={[
                "grid min-w-0 grid-cols-[2rem_2rem_minmax(0,1fr)_auto] items-center gap-2 px-4 py-3 transition-colors sm:px-6",
                !source.enabled && "opacity-60",
                dropTargetSourceId === source.id && "bg-accent/6",
              ]}
            >
              {#if source.kind === "local"}
                <span class="size-8"></span>
              {:else}
                <button
                  aria-label={`Reorder ${source.displayName}`}
                  class="flex size-8 cursor-grab items-center justify-center rounded-control text-text-faint outline-none hover:bg-white/4 hover:text-text-muted focus-visible:ring-2 focus-visible:ring-accent/20 active:cursor-grabbing"
                  disabled={changesPending}
                  draggable={!changesPending}
                  ondragend={finishDrag}
                  ondragover={(event) => allowDrop(event, source)}
                  ondragstart={(event) => beginDrag(event, source)}
                  ondrop={(event) => void dropSource(event, source)}
                  type="button"
                >
                  <GripVertical
                    aria-hidden="true"
                    size={15}
                  />
                </button>
              {/if}
              <span
                class="flex size-7 items-center justify-center rounded-control border border-border-subtle bg-white/3 font-mono text-meta text-text-muted"
              >
                {sourceIndex + 1}
              </span>
              <div class="min-w-0">
                <div class="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
                  <p class="truncate text-label font-medium text-text-primary">{source.displayName}</p>
                  <span
                    class={[
                      "shrink-0 rounded-control border px-1.5 py-0.5 text-[0.65rem]",
                      source.kind === "local"
                        ? "border-accent/20 bg-accent/7 text-accent"
                        : source.kind === "remote"
                          ? "border-success/20 bg-success/7 text-success"
                          : "border-border-subtle bg-white/3 text-text-muted",
                    ]}
                  >
                    {source.kind.toUpperCase()}
                  </span>
                  {#if source.lastRefreshError}
                    <span
                      class="text-meta text-danger"
                      title={source.lastRefreshError}
                      >Refresh failed</span
                    >
                  {/if}
                </div>
                <p
                  class="mt-0.5 truncate font-mono text-meta text-text-muted"
                  title={source.remoteUrl ?? source.sourceKey}
                >
                  {source.remoteUrl ?? source.sourceKey}
                </p>
              </div>
              <div class="flex h-8 shrink-0 items-center justify-end gap-1">
                {#if source.kind === "remote"}
                  <IconButton
                    disabled={changesPending}
                    label={`Refresh ${source.displayName}`}
                    onclick={() => void refreshSource(source)}
                    size="sm"
                    title="Refresh source"
                    variant="ghost"
                  >
                    <RefreshCw
                      aria-hidden="true"
                      size={13}
                    />
                  </IconButton>
                {/if}
                {#if source.kind !== "local"}
                  <Switch
                    aria-label={`Enable ${source.displayName}`}
                    checked={source.enabled}
                    disabled={changesPending}
                    onCheckedChange={(nextEnabled) => void toggleSource(source, nextEnabled)}
                  />
                  <DropdownMenu.Root>
                    <DropdownMenu.Trigger>
                      {#snippet child({ props })}
                        <IconButton
                          {...props}
                          disabled={changesPending}
                          label={`Manage ${source.displayName}`}
                          size="sm"
                          title="Manage source"
                          variant="ghost"
                        >
                          <Ellipsis
                            aria-hidden="true"
                            size={14}
                          />
                        </IconButton>
                      {/snippet}
                    </DropdownMenu.Trigger>
                    <DropdownMenu.Content align="end">
                      <DropdownMenu.Item
                        class="flex h-8 cursor-default items-center gap-2 rounded-control px-2 outline-none data-disabled:opacity-35 data-highlighted:bg-white/5"
                        disabled={importedIndex <= 0}
                        onclick={() => void moveSource(source, -1)}
                      >
                        <ArrowUp
                          aria-hidden="true"
                          size={14}
                        />
                        Move up
                      </DropdownMenu.Item>
                      <DropdownMenu.Item
                        class="flex h-8 cursor-default items-center gap-2 rounded-control px-2 outline-none data-disabled:opacity-35 data-highlighted:bg-white/5"
                        disabled={importedIndex < 0 || importedIndex >= importedSources.length - 1}
                        onclick={() => void moveSource(source, 1)}
                      >
                        <ArrowDown
                          aria-hidden="true"
                          size={14}
                        />
                        Move down
                      </DropdownMenu.Item>
                      <hr class="my-1 border-border-subtle">
                      <DropdownMenu.Item
                        class="flex h-8 cursor-default items-center gap-2 rounded-control px-2 outline-none data-highlighted:bg-white/5"
                        onclick={() => openSourceEditor(source)}
                      >
                        <FilePenLine
                          aria-hidden="true"
                          size={14}
                        />
                        Edit source
                      </DropdownMenu.Item>
                      <DropdownMenu.Item
                        class="flex h-8 cursor-default items-center gap-2 rounded-control px-2 text-danger outline-none data-highlighted:bg-danger/8"
                        onclick={() => requestRemoval(source)}
                      >
                        <Trash2
                          aria-hidden="true"
                          size={14}
                        />
                        Remove source
                      </DropdownMenu.Item>
                    </DropdownMenu.Content>
                  </DropdownMenu.Root>
                {/if}
              </div>
            </li>
          {/each}
        </ol>
      </section>
    </div>
  {/if}

  {#if mappingState && errorMessage}
    <div class="pointer-events-none fixed right-4 bottom-4 left-4 z-40 flex justify-center">
      <Panel
        class="pointer-events-auto max-w-xl px-4 py-3 text-label text-danger shadow-[0_18px_44px_rgb(0_0_0/45%)]"
        role="alert"
        tone="danger"
        >{errorMessage}</Panel
      >
    </div>
  {/if}

  <Dialog.Root
    onOpenChange={changeEditorOpen}
    bind:open={editorOpen}
  >
    <Dialog.Drawer
      class="sm:max-w-xl"
      style={editorOpen ? "translate: none" : undefined}
    >
      <form
        class="flex min-h-0 flex-1 flex-col"
        onsubmit={(event) => { event.preventDefault(); void saveSource(); }}
      >
        <header class="flex shrink-0 items-start justify-between gap-4 border-b border-border-strong px-4 py-4 sm:px-5">
          <div class="min-w-0">
            <Dialog.Title class="text-title font-semibold text-text-primary"
              >{editorSource ? `Edit ${editorSource.displayName}` : "Add mapping source"}</Dialog.Title
            >
            <Dialog.Description class="mt-1 text-label text-text-muted"
              >Changes activate after the complete source chain validates.</Dialog.Description
            >
          </div>
          <IconButton
            disabled={editorSaving}
            label="Close source editor"
            onclick={() => changeEditorOpen(false)}
            size="sm"
            type="button"
          >
            <X
              aria-hidden="true"
              size={15}
            />
          </IconButton>
        </header>
        <div class="min-h-0 flex-1 overflow-y-auto px-4 py-5 sm:px-5">
          {#if !editorSource}
            <div class="mb-5 grid gap-1.5 text-label text-text-muted">
              <span>Source type</span>
              <Tabs.Root
                variant="segmented"
                bind:value={sourceKind}
              >
                <Tabs.List>
                  <Tabs.Trigger value="upload">Upload</Tabs.Trigger>
                  <Tabs.Trigger value="remote">Remote URL</Tabs.Trigger>
                </Tabs.List>
              </Tabs.Root>
            </div>
          {/if}
          <div class="grid gap-4 sm:grid-cols-2">
            <label
              class="grid gap-1.5 text-label text-text-muted"
              for="mapping-source-key"
            >
              Source key
              <TextInput
                disabled={Boolean(editorSource)}
                id="mapping-source-key"
                placeholder="community.example"
                required
                bind:value={sourceKey}
              />
            </label>
            <label
              class="grid gap-1.5 text-label text-text-muted"
              for="mapping-source-name"
            >
              Display name
              <TextInput
                id="mapping-source-name"
                placeholder="Community mappings"
                required
                bind:value={displayName}
              />
            </label>
          </div>
          {#if sourceKind === "remote"}
            <label
              class="mt-4 grid gap-1.5 text-label text-text-muted"
              for="mapping-source-url"
            >
              Remote URL
              <TextInput
                disabled={Boolean(editorSource)}
                id="mapping-source-url"
                placeholder="https://example.org/mappings.trakkin"
                required
                type="url"
                bind:value={remoteUrl}
              />
            </label>
          {/if}
          <label
            class="mt-4 flex items-center justify-between gap-3 rounded-control border border-border-subtle bg-white/2 px-3 py-2 text-label text-text-secondary"
            for="mapping-source-enabled"
          >
            Include source in the active chain
            <Switch
              aria-label="Enable source"
              id="mapping-source-enabled"
              bind:checked={enabled}
            />
          </label>
          {#if !editorSource && sourceKind === "upload"}
            <div class="mt-5 grid gap-1.5 text-label text-text-muted">
              <label for="mapping-source-content">Mapping content</label>
              <Textarea
                class="min-h-72 font-mono text-[0.78rem] leading-5"
                id="mapping-source-content"
                placeholder="co.anilist://anime/1 <=> org.themoviedb://tv/2"
                spellcheck="false"
                bind:value={sourceContent}
              />
            </div>
          {/if}
          {#if editorError}
            <p
              class="mt-3 text-label text-danger"
              role="alert"
            >
              {editorError}
            </p>
          {/if}
        </div>
        <footer
          class="flex shrink-0 items-center justify-end gap-2 border-t border-border-strong bg-surface-raised px-4 py-3 sm:px-5"
        >
          <Button
            disabled={editorSaving}
            onclick={() => changeEditorOpen(false)}
            type="button"
            variant="ghost"
            >Cancel</Button
          >
          <Button
            disabled={editorSaving}
            type="submit"
            variant="primary"
          >
            {#if editorSaving}
              <Spinner size={14} />
            {/if}
            {editorSource ? "Save source" : "Add source"}
          </Button>
        </footer>
      </form>
    </Dialog.Drawer>
  </Dialog.Root>

  <AlertDialog.Root
    onOpenChange={changeRemoveOpen}
    bind:open={removeOpen}
  >
    <AlertDialog.Content>
      <AlertDialog.Title class="text-title font-semibold text-text-primary">Remove mapping source?</AlertDialog.Title>
      <AlertDialog.Description class="mt-1 text-label leading-5 text-text-muted">
        {removalSource?.displayName ?? "This source"}
        and its retained content will be removed after the replacement chain activates.
      </AlertDialog.Description>
      {#if removalError}
        <p
          class="mt-4 text-label text-danger"
          role="alert"
        >
          {removalError}
        </p>
      {/if}
      <div class="mt-5 flex justify-end gap-2">
        <AlertDialog.Cancel
          class="inline-flex h-8 items-center rounded-md border border-border-subtle px-3 text-label text-text-secondary hover:bg-white/4 disabled:pointer-events-none disabled:opacity-35"
          disabled={removalPending()}
          >Cancel</AlertDialog.Cancel
        >
        <AlertDialog.Action
          class="inline-flex h-8 items-center gap-1.5 rounded-md border border-danger/30 bg-danger/8 px-3 text-label text-danger hover:bg-danger/12 disabled:pointer-events-none disabled:opacity-35"
          disabled={removalPending()}
          onclick={(event) => { event.preventDefault(); void confirmRemoval(); }}
        >
          {#if removalPending()}
            <Spinner size={13} />
          {/if}
          Remove
        </AlertDialog.Action>
      </div>
    </AlertDialog.Content>
  </AlertDialog.Root>
</div>
