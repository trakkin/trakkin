<script lang="ts">
import {
  ArrowLeft,
  ArrowRight,
  Ban,
  ChevronDown,
  CircleAlert,
  CircleCheck,
  CloudDownload,
  Database,
  HardDrive,
  Pencil,
  Plus,
  RefreshCw,
  RotateCcw,
  Search,
  Trash2,
  Undo2,
  X,
} from "@lucide/svelte";
import { onDestroy, onMount } from "svelte";
import { AlertDialog, Button, Dialog, IconButton, PageHeader, PageToolbar, Spinner, Textarea, TextInput } from "$lib";
import { ApiRequestError } from "$lib/api/client";
import {
  decideMapping,
  deleteMapping,
  getMappingSources,
  getMappingState,
  MAPPING_PAGE_LIMIT,
  type MappingPage,
  type MappingResult,
  type MappingSource,
  type MappingState,
  previewMapping,
  queryMappings,
  revertMapping,
  saveMapping,
} from "$lib/api/mappings";

let mappingState = $state<MappingState>();
let mappingSources = $state<MappingSource[]>([]);
let loading = $state(true);
let errorMessage = $state("");
let queryText = $state("");
let search = $state("");
let pages = $state<MappingPage[]>([]);
let pageIndex = $state(0);
let queryLoading = $state(false);
let queryError = $state("");
let busy = $state(false);
let editorOpen = $state(false);
let editing = $state<MappingResult>();
let editorText = $state("");
let editorFingerprint = $state("");
let editorError = $state("");
let editorPreview = $state<MappingResult>();
let previewText = $state("");
let previewLoading = $state(false);
let previewSequence = 0;
let removeOpen = $state(false);
let removeTarget = $state<MappingResult>();
let removeFingerprint = $state("");
let removeAction = $state<"delete" | "revert">("delete");
let returnFocus: HTMLElement | undefined;
let loadSequence = 0;
let querySequence = 0;
const lifecycle = new AbortController();
let currentPage = $derived(pages[pageIndex]);
let canSave = $derived(
  !!editorPreview && previewText === editorText.trim() && editorPreview.status !== "invalid" && !previewLoading,
);

onMount(() => {
  void loadState();
});
onDestroy(() => {
  loadSequence += 1;
  querySequence += 1;
  lifecycle.abort();
});

$effect(() => {
  const text = queryText.trim();
  const timer = setTimeout(() => {
    search = text;
  }, 250);
  return () => clearTimeout(timer);
});

$effect(() => {
  const fingerprint = mappingState?.fingerprint;
  const filter = search;
  querySequence += 1;
  pages = [];
  pageIndex = 0;
  queryError = "";
  if (!fingerprint) {
    queryLoading = false;
    return;
  }
  const controller = new AbortController();
  void loadFirstPage(fingerprint, filter, controller.signal);
  return () => controller.abort();
});

async function loadState(): Promise<void> {
  const sequence = ++loadSequence;
  loading = true;
  errorMessage = "";
  try {
    const [next, sources] = await Promise.all([getMappingState(lifecycle.signal), getMappingSources(lifecycle.signal)]);
    if (sequence === loadSequence) {
      mappingState = next;
      mappingSources = sources.sources;
    }
  } catch (error) {
    if (sequence === loadSequence && !lifecycle.signal.aborted) errorMessage = message(error);
  } finally {
    if (sequence === loadSequence) loading = false;
  }
}

async function loadFirstPage(fingerprint: string, filter: string, signal: AbortSignal): Promise<void> {
  const sequence = ++querySequence;
  queryLoading = true;
  try {
    const page = await queryMappings(
      { expectedFingerprint: fingerprint, search: filter, limit: MAPPING_PAGE_LIMIT },
      signal,
    );
    if (sequence === querySequence) pages = [page];
  } catch (error) {
    if (sequence === querySequence && !signal.aborted) handleQueryError(error);
  } finally {
    if (sequence === querySequence) queryLoading = false;
  }
}

async function nextPage(): Promise<void> {
  const page = currentPage;
  if (!page?.nextCursor || queryLoading) return;
  if (pages[pageIndex + 1]) {
    pageIndex += 1;
    return;
  }
  const sequence = querySequence;
  queryLoading = true;
  queryError = "";
  try {
    const next = await queryMappings(
      { expectedFingerprint: page.fingerprint, search, cursor: page.nextCursor, limit: MAPPING_PAGE_LIMIT },
      lifecycle.signal,
    );
    if (sequence === querySequence) {
      pages = [...pages, next];
      pageIndex += 1;
    }
  } catch (error) {
    if (sequence === querySequence && !lifecycle.signal.aborted) handleQueryError(error);
  } finally {
    if (sequence === querySequence) queryLoading = false;
  }
}

function handleQueryError(error: unknown): void {
  if (error instanceof ApiRequestError && error.code === "mappings_changed") {
    void loadState();
  } else {
    queryError = message(error);
  }
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : "Mappings are unavailable";
}

function rememberFocus(): void {
  returnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : undefined;
}

function restoreFocus(event: Event): void {
  event.preventDefault();
  returnFocus?.focus();
}

function openEditor(claim?: MappingResult): void {
  if (!mappingState || busy) return;
  rememberFocus();
  editing = claim;
  editorText = claim?.canonicalRecord ?? "";
  editorFingerprint = mappingState.fingerprint;
  editorError = "";
  editorPreview = undefined;
  previewText = "";
  previewSequence += 1;
  previewLoading = false;
  editorOpen = true;
}

async function previewEditor(): Promise<void> {
  const text = editorText.trim();
  if (!text || busy) return;
  const sequence = ++previewSequence;
  previewLoading = true;
  editorError = "";
  try {
    const result = await previewMapping(text, lifecycle.signal);
    if (sequence === previewSequence && editorOpen && text === editorText.trim()) {
      editorPreview = result;
      previewText = text;
    }
  } catch (error) {
    if (sequence === previewSequence && !lifecycle.signal.aborted) editorError = message(error);
  } finally {
    if (sequence === previewSequence) previewLoading = false;
  }
}

async function submitEditor(event: SubmitEvent): Promise<void> {
  event.preventDefault();
  if (!canSave || busy) return;
  busy = true;
  editorError = "";
  try {
    mappingState = await saveMapping(
      { expectedFingerprint: editorFingerprint, record: editorText, replaces: editing?.claimId },
      lifecycle.signal,
    );
    editorOpen = false;
  } catch (error) {
    editorError = message(error);
    if (error instanceof ApiRequestError && error.code === "mappings_changed") {
      editorPreview = undefined;
      previewText = "";
      await loadState();
      editorFingerprint = mappingState?.fingerprint ?? editorFingerprint;
    }
  } finally {
    busy = false;
  }
}

async function changeDecision(claim: MappingResult): Promise<void> {
  if (!mappingState || busy) return;
  busy = true;
  queryError = "";
  try {
    mappingState = await decideMapping(claim.claimId, mappingState.fingerprint, !claim.rejected, lifecycle.signal);
  } catch (error) {
    handleQueryError(error);
  } finally {
    busy = false;
  }
}

function confirmRemove(claim: MappingResult, action: "delete" | "revert" = "delete"): void {
  if (!mappingState || busy) return;
  rememberFocus();
  removeTarget = claim;
  removeFingerprint = mappingState.fingerprint;
  removeAction = action;
  removeOpen = true;
}

async function removeMapping(): Promise<void> {
  if (!removeFingerprint || !removeTarget || busy) return;
  busy = true;
  queryError = "";
  try {
    mappingState = await (removeAction === "revert" ? revertMapping : deleteMapping)(
      removeTarget.claimId,
      removeFingerprint,
      lifecycle.signal,
    );
    removeOpen = false;
  } catch (error) {
    handleQueryError(error);
  } finally {
    busy = false;
  }
}

function statusLabel(status: MappingResult["status"]): string {
  return status === "rejected" ? "Suppressed" : status[0].toUpperCase() + status.slice(1);
}
</script>

<svelte:head><title>Trakkin &middot; Mappings</title></svelte:head>

<div class="flex h-full min-h-0 flex-col">
  <PageHeader title="Mappings">
    {#snippet actions()}
      <IconButton
        href="/mappings/sources"
        label="Mapping sources"
        size="sm"
        ><Database
          aria-hidden="true"
          size={14}
        /></IconButton
      >
      <IconButton
        disabled={!mappingState || busy}
        label="Create mapping"
        onclick={() => openEditor()}
        size="sm"
        ><Plus
          aria-hidden="true"
          size={14}
        /></IconButton
      >
      <IconButton
        disabled={loading || busy}
        label="Refresh mappings"
        onclick={() => void loadState()}
        size="sm"
      >
        <RefreshCw
          aria-hidden="true"
          size={14}
        />
      </IconButton>
    {/snippet}
  </PageHeader>
  {#if errorMessage}
    <p
      class="border-b border-danger/20 px-4 py-3 text-label text-danger"
      role="alert"
    >
      {errorMessage}
    </p>
  {/if}
  {#if loading && !mappingState}
    <div
      class="flex min-h-48 flex-1 items-center justify-center gap-2 text-label text-text-muted"
      role="status"
    >
      <Spinner size={16} />Loading mappings...
    </div>
  {:else if !mappingState}
    <div
      class="grid min-h-48 flex-1 place-items-center text-label text-text-muted"
      role="status"
    >
      Mappings unavailable
    </div>
  {:else}
    <dl
      aria-label="Mapping records"
      class="grid grid-cols-2 gap-3 border-b border-border-subtle px-4 py-3 text-meta sm:grid-cols-4 sm:px-6"
    >
      <div>
        <dt class="text-text-muted">Records</dt>
        <dd>{mappingState.records.toLocaleString()}</dd>
      </div>
      <div>
        <dt class="text-text-muted">Active</dt>
        <dd>{mappingState.active.toLocaleString()}</dd>
      </div>
      <div>
        <dt class="text-text-muted">Suppressed</dt>
        <dd>{mappingState.rejected.toLocaleString()}</dd>
      </div>
      <div>
        <dt class="text-text-muted">Indexed units</dt>
        <dd>{mappingState.units.toLocaleString()}</dd>
      </div>
    </dl>
    <PageToolbar label="Mapping filters">
      <div class="relative min-w-0">
        <Search
          aria-hidden="true"
          class="pointer-events-none absolute top-1/2 left-2.5 -translate-y-1/2 text-text-faint"
          size={14}
        />
        <TextInput
          aria-label="Search mappings"
          class="w-full pl-8"
          placeholder="Search mappings"
          bind:value={queryText}
        />
      </div>
    </PageToolbar>
    {#if queryError}
      <p
        class="px-4 py-3 text-label text-danger"
        role="alert"
      >
        {queryError}
      </p>
    {/if}
    {#if queryLoading && !currentPage}
      <div
        class="flex min-h-40 flex-1 items-center justify-center gap-2 text-label text-text-muted"
        role="status"
      >
        <Spinner size={16} />Loading mappings...
      </div>
    {:else if !currentPage?.items.length}
      <div
        class="grid min-h-48 flex-1 place-items-center text-label text-text-muted"
        role="status"
      >
        No mappings found
      </div>
    {:else}
      <div class="min-h-0 flex-1 overflow-y-auto">
        <div
          aria-hidden="true"
          class="sticky top-0 z-10 hidden grid-cols-[minmax(0,1fr)_8rem_10rem_7rem] gap-4 border-b border-l-2 border-border-strong border-l-transparent bg-surface-panel px-6 py-2 text-meta text-text-muted lg:grid"
        >
          <span>Relationship</span>
          <span>State</span>
          <span>Sources</span>
          <span class="text-right">Actions</span>
        </div>
        <ul
          aria-label="Mapping claims"
          class="divide-y divide-border-strong/60"
        >
          {#each currentPage.items as claim (claim.claimId)}
            {@const note = claim.metadata.find((annotation) => annotation.startsWith("#@note "))?.slice(7)}
            <li
              class="min-w-0 border-l-2 border-transparent px-4 py-2 even:bg-surface-raised/40 hover:bg-surface-selected/50 focus-within:bg-surface-selected/50 data-[rejected=true]:border-l-text-disabled data-[status=conflicting]:border-l-danger data-[status=invalid]:border-l-danger sm:px-6"
              data-rejected={claim.rejected}
              data-status={claim.status}
            >
              <div
                class="grid min-w-0 grid-cols-[minmax(0,1fr)_auto] items-start gap-x-4 gap-y-2 lg:grid-cols-[minmax(0,1fr)_8rem_10rem_7rem]"
              >
                <div class="col-span-2 min-w-0 lg:col-span-1">
                  <p
                    class="wrap-anywhere font-mono text-label leading-5"
                    class:text-text-muted={claim.rejected}
                  >
                    {claim.canonicalStatement}
                  </p>
                  {#if claim.diagnostics.length}
                    <p class="mt-0.5 line-clamp-1 wrap-anywhere text-meta text-warning">{claim.diagnostics[0]}</p>
                  {/if}
                  <details class="group/details mt-0.5 text-meta text-text-muted">
                    <summary
                      class="flex min-w-0 cursor-pointer list-none items-center gap-1 rounded-control outline-none hover:text-text-primary focus-visible:ring-2 focus-visible:ring-accent [&::-webkit-details-marker]:hidden"
                    >
                      <ChevronDown
                        aria-hidden="true"
                        class="shrink-0 transition-transform group-open/details:rotate-180"
                        size={12}
                      />
                      <span class="shrink-0">Details</span>
                      {#if note}
                        <span
                          class="ml-2 truncate text-text-secondary"
                          title={note}
                          >{note}</span
                        >
                      {/if}
                    </summary>
                    <dl class="mt-3 space-y-3 border-l border-border-strong pl-3">
                      <div>
                        <dt class="mb-1 text-text-muted">Claim ID</dt>
                        <dd class="break-all font-mono text-text-secondary">{claim.claimId}</dd>
                      </div>
                      {#if claim.metadata.length}
                        <div>
                          <dt class="mb-1 text-text-muted">Annotations ({claim.metadata.length})</dt>
                          <dd>
                            <pre
                              class="wrap-anywhere whitespace-pre-wrap font-mono text-text-secondary"
                            >{claim.metadata.join("\n")}</pre>
                          </dd>
                        </div>
                      {/if}
                      {#if claim.diagnostics.length}
                        <div>
                          <dt class="mb-1 text-text-muted">Diagnostics</dt>
                          <dd>
                            <ul class="space-y-1 wrap-anywhere text-warning">
                              {#each claim.diagnostics as diagnostic}
                                <li>{diagnostic}</li>
                              {/each}
                            </ul>
                          </dd>
                        </div>
                      {/if}
                    </dl>
                  </details>
                </div>
                <div class="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-2 pt-0.5 lg:contents">
                  <span
                    class="inline-flex items-center gap-1.5 text-meta leading-5"
                    class:text-danger={!claim.rejected && (claim.status === "conflicting" || claim.status === "invalid")}
                    class:text-success={!claim.rejected && claim.status === "resolved"}
                    class:text-text-muted={claim.rejected}
                    class:text-warning={!claim.rejected && claim.status === "unresolved"}
                  >
                    {#if claim.rejected}
                      <Ban
                        aria-hidden="true"
                        class="shrink-0"
                        size={13}
                      />
                    {:else if claim.status === "resolved"}
                      <CircleCheck
                        aria-hidden="true"
                        class="shrink-0"
                        size={13}
                      />
                    {:else}
                      <CircleAlert
                        aria-hidden="true"
                        class="shrink-0"
                        size={13}
                      />
                    {/if}
                    {claim.rejected ? "Suppressed" : statusLabel(claim.status)}
                  </span>
                  <div
                    class="flex min-w-0 flex-wrap gap-x-3 gap-y-1 text-meta leading-5 text-text-secondary lg:flex-col lg:gap-0"
                  >
                    {#each claim.sources as source}
                      <span class="inline-flex min-w-0 items-start gap-1.5">
                        {#if source === "local"}
                          <HardDrive
                            aria-hidden="true"
                            class="mt-1 shrink-0 text-text-muted"
                            size={12}
                          />
                        {:else}
                          <CloudDownload
                            aria-hidden="true"
                            class="mt-1 shrink-0 text-text-muted"
                            size={12}
                          />
                        {/if}
                        <span class="wrap-anywhere"
                          >{mappingSources.find((origin) => origin.id === source)?.displayName ?? source}</span
                        >
                      </span>
                    {/each}
                  </div>
                </div>
                <div class="flex shrink-0 justify-end gap-0.5">
                  <IconButton
                    disabled={busy}
                    label={claim.sources.some((source) => source !== "local") ? "Replace mapping" : "Edit mapping"}
                    onclick={() => openEditor(claim)}
                    size="sm"
                    ><Pencil
                      aria-hidden="true"
                      size={14}
                    /></IconButton
                  >
                  <IconButton
                    disabled={busy}
                    label={claim.rejected ? "Restore mapping" : "Suppress mapping"}
                    onclick={() => void changeDecision(claim)}
                    size="sm"
                  >
                    {#if claim.rejected}
                      <Undo2
                        aria-hidden="true"
                        size={14}
                      />
                    {:else}
                      <Ban
                        aria-hidden="true"
                        size={14}
                      />
                    {/if}
                  </IconButton>
                  <IconButton
                    disabled={busy || claim.sources.some((source) => source !== "local")}
                    label="Delete mapping"
                    onclick={() => confirmRemove(claim)}
                    size="sm"
                    ><Trash2
                      aria-hidden="true"
                      size={14}
                    /></IconButton
                  >
                  {#if claim.sources.includes("local") && claim.sources.some((source) => source !== "local")}
                    <IconButton
                      disabled={busy}
                      label="Revert to upstream"
                      onclick={() => confirmRemove(claim, "revert")}
                      size="sm"
                      ><RotateCcw
                        aria-hidden="true"
                        size={14}
                      /></IconButton
                    >
                  {/if}
                </div>
              </div>
            </li>
          {/each}
        </ul>
      </div>
    {/if}
    <div
      class="flex shrink-0 items-center justify-between border-t border-border-subtle px-4 py-2 text-meta text-text-muted"
    >
      <span>Page {pageIndex + 1}</span>
      <div class="flex gap-1">
        <IconButton
          disabled={queryLoading || pageIndex === 0}
          label="Previous page"
          onclick={() => {
  pageIndex -= 1;
}}
          size="sm"
          ><ArrowLeft
            aria-hidden="true"
            size={14}
          /></IconButton
        >
        <IconButton
          disabled={queryLoading || !currentPage?.nextCursor}
          label="Next page"
          onclick={() => void nextPage()}
          size="sm"
          ><ArrowRight
            aria-hidden="true"
            size={14}
          /></IconButton
        >
      </div>
    </div>
  {/if}
</div>

<Dialog.Root bind:open={editorOpen}>
  <Dialog.Drawer
    escapeKeydownBehavior={busy ? "ignore" : "close"}
    interactOutsideBehavior={busy ? "ignore" : "close"}
    onCloseAutoFocus={restoreFocus}
  >
    <form
      class="flex h-full min-h-0 flex-col"
      onsubmit={submitEditor}
    >
      <div class="flex shrink-0 items-center justify-between border-b border-border-subtle px-5 py-4">
        <Dialog.Title class="text-title font-semibold">{editing ? "Edit mapping" : "New mapping"}</Dialog.Title>
        <IconButton
          disabled={busy}
          label="Close mapping editor"
          onclick={() => (editorOpen = false)}
          size="sm"
          ><X
            aria-hidden="true"
            size={16}
          /></IconButton
        >
      </div>
      <Dialog.Description class="sr-only">Canonical mapping record</Dialog.Description>
      <div class="min-h-0 flex-1 space-y-4 overflow-y-auto p-5">
        {#if editing}
          <p class="break-all font-mono text-meta text-text-muted">{editing.claimId}</p>
        {/if}
        <div class="space-y-2">
          <label
            class="text-label"
            for="mapping-record"
            >Mapping record</label
          >
          <Textarea
            class="w-full font-mono text-label"
            id="mapping-record"
            rows={10}
            spellcheck={false}
            bind:value={editorText}
          />
        </div>
        {#if editorError}
          <p
            class="wrap-break-word text-label text-danger"
            role="alert"
          >
            {editorError}
          </p>
        {/if}
        {#if editorPreview && previewText === editorText.trim()}
          <section
            aria-label="Mapping preview"
            class="space-y-2 border-t border-border-subtle pt-4"
          >
            <p class="text-label">{statusLabel(editorPreview.status)}</p>
            <p class="break-all font-mono text-meta text-text-muted">{editorPreview.canonicalStatement}</p>
            {#each editorPreview.diagnostics as diagnostic}
              <p class="wrap-break-word text-meta text-text-muted">{diagnostic}</p>
            {/each}
          </section>
        {/if}
      </div>
      <div class="flex shrink-0 justify-end gap-2 border-t border-border-subtle p-4">
        <Button
          disabled={busy}
          onclick={() => (editorOpen = false)}
          >Cancel</Button
        >
        <Button
          disabled={busy || previewLoading || !editorText.trim()}
          onclick={() => void previewEditor()}
          >{#if previewLoading}
            <Spinner size={14} />
          {/if}
          Preview</Button
        >
        <Button
          disabled={busy || !canSave}
          type="submit"
          >{#if busy}
            <Spinner size={14} />
          {/if}
          {editing && editing.claimId !== editorPreview?.claimId ? "Replace" : "Save"}</Button
        >
      </div>
    </form>
  </Dialog.Drawer>
</Dialog.Root>

<AlertDialog.Root bind:open={removeOpen}>
  <AlertDialog.Content onCloseAutoFocus={restoreFocus}>
    {#snippet header()}
      <AlertDialog.Title class="wrap-anywhere text-heading font-medium"
        >{removeAction === "revert" ? "Revert local decision?" : "Delete mapping?"}</AlertDialog.Title
      >
    {/snippet}
    <AlertDialog.Description class="text-label leading-6 text-text-muted">
      {removeAction === "revert"
  ? "Your Local decision will be removed. Enabled upstream sources will determine this mapping again."
  : "This mapping will be removed from your Local repository."}
    </AlertDialog.Description>
    <p
      class="mt-4 wrap-anywhere border-l-2 border-border-strong pl-3 font-mono text-label leading-6 text-text-secondary"
    >
      {removeTarget?.canonicalStatement}
    </p>
    {#snippet footer()}
      <AlertDialog.Cancel disabled={busy}>Cancel</AlertDialog.Cancel>
      <AlertDialog.Action
        disabled={busy}
        onclick={() => void removeMapping()}
        variant={removeAction === "revert" ? "primary" : "danger"}
        >{removeAction === "revert" ? "Revert" : "Delete"}</AlertDialog.Action
      >
    {/snippet}
  </AlertDialog.Content>
</AlertDialog.Root>
