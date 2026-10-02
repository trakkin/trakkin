<script lang="ts">
import {
  ArrowLeft,
  ArrowRight,
  Braces,
  ChevronDown,
  ChevronRight,
  CopyPlus,
  Pencil,
  Plus,
  RefreshCw,
  Search,
  Trash2,
  X,
} from "@lucide/svelte";
import { onDestroy, onMount } from "svelte";
import {
  AlertDialog,
  Button,
  Dialog,
  EmptyState,
  IconButton,
  PageToolbar,
  Panel,
  Select,
  Spinner,
  Tabs,
  Textarea,
  TextInput,
} from "$lib";
import { ApiRequestError } from "$lib/api/client";
import {
  createLocalMapping,
  getMappingState,
  MAPPING_PAGE_LIMIT,
  type MappingPage,
  type MappingResult,
  type MappingState,
  queryMappings,
  removeLocalMapping,
  updateLocalMapping,
} from "$lib/api/mappings";
import MappingsHeader from "./MappingsHeader.svelte";

let mappingState = $state<MappingState>();
let loading = $state(true);
let reloading = $state(false);
let errorMessage = $state("");
let queryText = $state("");
let normalizedSearch = $state("");
let sourceFilter = $state("all");
let occurrenceScope = $state<"active" | "all">("active");
let pages = $state<MappingPage[]>([]);
let pageIndex = $state(0);
let queryLoading = $state(false);
let queryError = $state("");
let expandedResultKey = $state("");
let editorOpen = $state(false);
let editorResult = $state<MappingResult>();
let editorMode = $state<"create" | "edit" | "override">("create");
let mappingRecord = $state("");
let editorSaving = $state(false);
let editorError = $state("");
let removeOpen = $state(false);
let removalMapping = $state<MappingResult>();
let removalError = $state("");
let removalPending = $state(false);
let loadSequence = 0;
let querySequence = 0;
const lifecycleController = new AbortController();

let sourceItems = $derived([
  { value: "all", label: "All sources" },
  ...(mappingState?.sources.map((source) => ({ value: source.sourceKey, label: source.displayName })) ?? []),
]);
let sourceNames = $derived(
  new Map(mappingState?.sources.map((source) => [source.sourceKey, source.displayName]) ?? []),
);
let currentPage = $derived(pages[pageIndex]);
let changesPending = $derived(editorSaving || removalPending);

onMount(() => {
  void loadState(lifecycleController.signal);
});

onDestroy(() => {
  loadSequence += 1;
  querySequence += 1;
  lifecycleController.abort();
});

$effect(() => {
  const normalized = queryText.trim().split(/\s+/).join(" ");
  const timeout = setTimeout(() => {
    if (normalizedSearch !== normalized) normalizedSearch = normalized;
  }, 250);
  return () => clearTimeout(timeout);
});

$effect(() => {
  const fingerprint = mappingState?.activeChainFingerprint;
  const search = normalizedSearch;
  const activeSource = sourceFilter;
  const scope = occurrenceScope;
  if (!fingerprint) {
    pages = [];
    pageIndex = 0;
    queryLoading = false;
    return;
  }
  const controller = new AbortController();
  void loadFirstPage(fingerprint, search, activeSource, scope === "all", controller.signal);
  return () => controller.abort();
});

async function loadState(signal?: AbortSignal): Promise<void> {
  const sequence = ++loadSequence;
  loading = !mappingState;
  reloading = Boolean(mappingState);
  errorMessage = "";
  try {
    const snapshot = await getMappingState(signal);
    if (sequence !== loadSequence) return;
    mappingState = snapshot;
    if (sourceFilter !== "all" && !snapshot.sources.some((source) => source.sourceKey === sourceFilter)) {
      sourceFilter = "all";
    }
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (sequence === loadSequence) errorMessage = messageFor(error, "Mappings could not be loaded");
  } finally {
    if (sequence === loadSequence) {
      loading = false;
      reloading = false;
    }
  }
}

async function loadFirstPage(
  expectedChainFingerprint: string,
  search: string,
  activeSource: string,
  includeShadowed: boolean,
  signal?: AbortSignal,
): Promise<void> {
  const sequence = ++querySequence;
  queryLoading = true;
  queryError = "";
  expandedResultKey = "";
  try {
    const page = await queryMappings(
      {
        expectedChainFingerprint,
        limit: MAPPING_PAGE_LIMIT,
        ...(search ? { search } : {}),
        ...(activeSource !== "all" ? { sourceKey: activeSource } : {}),
        ...(includeShadowed ? { includeShadowed: true } : {}),
      },
      signal,
    );
    if (sequence !== querySequence) return;
    pages = [page];
    pageIndex = 0;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (sequence !== querySequence) return;
    handleQueryError(error, "Mappings could not be loaded");
    pages = [];
    pageIndex = 0;
  } finally {
    if (sequence === querySequence) queryLoading = false;
  }
}

async function showNextPage(): Promise<void> {
  const page = currentPage;
  if (!page?.nextCursor || queryLoading) return;
  const cached = pages[pageIndex + 1];
  if (cached) {
    pageIndex += 1;
    expandedResultKey = "";
    return;
  }
  const sequence = querySequence;
  queryLoading = true;
  queryError = "";
  try {
    const next = await queryMappings({
      expectedChainFingerprint: page.chainFingerprint,
      cursor: page.nextCursor,
      limit: MAPPING_PAGE_LIMIT,
      ...(normalizedSearch ? { search: normalizedSearch } : {}),
      ...(sourceFilter !== "all" ? { sourceKey: sourceFilter } : {}),
      ...(occurrenceScope === "all" ? { includeShadowed: true } : {}),
    });
    if (sequence !== querySequence) return;
    pages = [...pages.slice(0, pageIndex + 1), next];
    pageIndex += 1;
    expandedResultKey = "";
  } catch (error) {
    if (sequence === querySequence) handleQueryError(error, "The next mapping page could not be loaded");
  } finally {
    if (sequence === querySequence) queryLoading = false;
  }
}

function showPreviousPage(): void {
  if (queryLoading || pageIndex === 0) return;
  pageIndex -= 1;
  expandedResultKey = "";
}

function handleQueryError(error: unknown, fallback: string): void {
  if (error instanceof ApiRequestError && error.code === "mapping_chain_changed") {
    void loadState(lifecycleController.signal);
    queryError = "The mapping chain changed. Results are being reloaded.";
  } else {
    queryError = messageFor(error, fallback);
  }
}

function openCreateMapping(): void {
  editorResult = undefined;
  editorMode = "create";
  mappingRecord = "";
  editorError = "";
  editorOpen = true;
}

function openMappingEditor(result: MappingResult): void {
  if (result.sourceKey !== "local") return;
  editorResult = result;
  editorMode = "edit";
  mappingRecord = result.canonicalRecord;
  editorError = "";
  editorOpen = true;
}

function openLocalOverride(result: MappingResult): void {
  if (result.sourceKey === "local") return;
  editorResult = undefined;
  editorMode = "override";
  mappingRecord = result.canonicalRecord;
  editorError = "";
  editorOpen = true;
}

async function saveMapping(): Promise<void> {
  if (reloading || changesPending) return;
  supersedeLoads();
  editorSaving = true;
  editorError = "";
  try {
    mappingState =
      editorMode === "edit" && editorResult
        ? await updateLocalMapping(editorResult.statementId, { record: mappingRecord }, lifecycleController.signal)
        : await createLocalMapping({ record: mappingRecord }, lifecycleController.signal);
    editorOpen = false;
  } catch (error) {
    editorError = messageFor(error, "Local mapping could not be saved");
    await loadState(lifecycleController.signal);
  } finally {
    editorSaving = false;
  }
}

function requestRemoval(result: MappingResult): void {
  if (result.sourceKey !== "local") return;
  removalMapping = result;
  removalError = "";
  removeOpen = true;
}

async function confirmRemoval(): Promise<void> {
  const result = removalMapping;
  if (!result || reloading || changesPending) return;
  supersedeLoads();
  removalPending = true;
  removalError = "";
  try {
    mappingState = await removeLocalMapping(result.statementId, lifecycleController.signal);
    removeOpen = false;
    removalMapping = undefined;
  } catch (error) {
    removalError = messageFor(error, "Local mapping could not be deleted");
    await loadState(lifecycleController.signal);
  } finally {
    removalPending = false;
  }
}

function changeEditorOpen(open: boolean): void {
  if (editorSaving) return;
  editorOpen = open;
  if (!open) editorResult = undefined;
}

function changeRemoveOpen(open: boolean): void {
  if (removalPending) return;
  removeOpen = open;
  if (!open) removalMapping = undefined;
}

function supersedeLoads(): void {
  loadSequence += 1;
  loading = false;
  reloading = false;
}

function toggleExpanded(result: MappingResult): void {
  const key = resultKey(result);
  expandedResultKey = expandedResultKey === key ? "" : key;
}

function relationParts(result: MappingResult): { left: string; right: string } | undefined {
  const marker = ` ${result.operator} `;
  const operatorIndex = result.canonicalStatement.indexOf(marker);
  if (operatorIndex < 0) return undefined;
  return {
    left: result.canonicalStatement.slice(0, operatorIndex),
    right: result.canonicalStatement.slice(operatorIndex + marker.length),
  };
}

function editorTitle(): string {
  if (editorMode === "edit") return "Edit local mapping";
  if (editorMode === "override") return "Override mapping locally";
  return "Add local mapping";
}

function sourceName(sourceKey: string): string {
  return sourceNames.get(sourceKey) ?? sourceKey;
}

function annotationRegionId(result: MappingResult): string {
  return `mapping-annotations-${result.statementId}-${result.layerPosition}-${result.recordOrdinal}`;
}

function messageFor(error: unknown, fallback: string): string {
  if (error instanceof ApiRequestError && error.code === "mapping_chain_changed") {
    return "The mapping chain changed. The latest state has been loaded; try again.";
  }
  return error instanceof Error ? error.message : fallback;
}

function resultKey(result: MappingResult): string {
  return `${result.sourceKey}:${result.statementId}:${result.layerPosition}:${result.recordOrdinal}`;
}
</script>

<svelte:head><title>Trakkin &middot; Mappings</title></svelte:head>

<div class="flex h-full min-h-0 flex-col">
  <MappingsHeader>
    {#snippet actions()}
      <IconButton
        disabled={loading || reloading || changesPending}
        label="Refresh mappings"
        onclick={() => void loadState()}
        size="sm"
        title="Refresh mappings"
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
        onclick={openCreateMapping}
        size="sm"
        variant="primary"
      >
        <Plus
          aria-hidden="true"
          size={14}
        />
        Add mapping
      </Button>
    {/snippet}
  </MappingsHeader>

  {#if loading && !mappingState}
    <div
      class="flex min-h-48 flex-1 items-center justify-center gap-2 text-label text-text-muted"
      role="status"
    >
      <Spinner size={16} />
      Loading mappings...
    </div>
  {:else if !mappingState}
    <div class="flex min-h-0 flex-1 p-4 sm:p-6">
      <Panel
        class="grid min-h-48 w-full place-items-center px-4 py-6 text-center text-label text-danger"
        tone="danger"
      >
        <div>
          <p>{errorMessage || "Mappings are unavailable"}</p>
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
    <section
      aria-label="Mappings"
      class="flex min-h-0 flex-1 flex-col"
    >
      <PageToolbar label="Mapping filters">
        <div class="grid min-w-0 gap-2 sm:grid-cols-[minmax(12rem,1fr)_minmax(10rem,15rem)_auto] sm:items-center">
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
          <Select
            ariaLabel="Filter by mapping source"
            items={sourceItems}
            onValueChange={(value) => (sourceFilter = value)}
            triggerClass="w-full"
            value={sourceFilter}
          />
          <Tabs.Root
            variant="segmented"
            bind:value={occurrenceScope}
          >
            <Tabs.List aria-label="Mapping occurrence scope">
              <Tabs.Trigger value="active">Active</Tabs.Trigger>
              <Tabs.Trigger value="all">All occurrences</Tabs.Trigger>
            </Tabs.List>
          </Tabs.Root>
        </div>
      </PageToolbar>

      {#if queryError}
        <div
          class="border-b border-danger/20 bg-danger/6 px-4 py-2 text-label text-danger"
          role="alert"
        >
          {queryError}
        </div>
      {/if}

      {#if queryLoading && !currentPage}
        <div
          class="flex min-h-40 flex-1 items-center justify-center gap-2 text-label text-text-muted"
          role="status"
        >
          <Spinner size={15} />
          Loading mappings...
        </div>
      {:else if !currentPage?.items.length}
        <EmptyState
          class="m-4 min-h-48 flex-1"
          description="Adjust the source filter or search terms."
          icon={Braces}
          title="No mappings found"
        />
      {:else}
        <div class="min-h-0 flex-1 overflow-y-auto">
          <ul class="divide-y divide-border-subtle">
            {#each currentPage.items as result (resultKey(result))}
              {@const parts = relationParts(result)}
              {@const expanded = expandedResultKey === resultKey(result)}
              <li class="px-4 py-3 sm:px-6">
                <div class="flex min-w-0 items-start gap-3">
                  <div class="min-w-0 flex-1">
                    {#if parts}
                      <div
                        class="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 font-mono text-label text-text-primary"
                      >
                        <span
                          class="min-w-0 truncate"
                          title={parts.left}
                          >{parts.left}</span
                        >
                        <span
                          class="shrink-0 rounded-control border border-border-strong bg-white/3 px-1.5 py-0.5 text-meta text-text-muted"
                        >
                          {result.operator}
                        </span>
                        <span
                          class="min-w-0 truncate"
                          title={parts.right}
                          >{parts.right}</span
                        >
                      </div>
                    {:else}
                      <p
                        class="truncate font-mono text-label text-text-primary"
                        title={result.canonicalStatement}
                      >
                        {result.canonicalStatement}
                      </p>
                    {/if}
                    {#if occurrenceScope === "all" || result.occurrenceCount > 1}
                      <div class="mt-1 flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 text-meta text-text-muted">
                        {#if occurrenceScope === "all"}
                          <span>{result.active ? "Active" : "Shadowed"}</span>
                        {/if}
                        {#if result.occurrenceCount > 1}
                          <span>{result.occurrenceCount} occurrences</span>
                        {/if}
                      </div>
                    {/if}
                  </div>
                  <div class="flex min-h-8 shrink-0 items-center gap-0.5">
                    <span
                      class={[
                        "mr-1 max-w-36 truncate rounded-control border px-1.5 py-0.5 text-meta",
                        result.sourceKey === "local"
                          ? "border-accent/20 bg-accent/7 text-accent"
                          : "border-border-subtle bg-white/3 text-text-muted",
                      ]}
                      title={sourceName(result.sourceKey)}
                    >
                      {sourceName(result.sourceKey)}
                    </span>
                    {#if result.metadata.length}
                      <IconButton
                        aria-controls={annotationRegionId(result)}
                        aria-expanded={expanded}
                        label={`${expanded ? "Hide" : "Show"} annotations for ${result.canonicalStatement}`}
                        onclick={() => toggleExpanded(result)}
                        size="sm"
                        title={expanded ? "Hide annotations" : "Show annotations"}
                        variant="ghost"
                      >
                        {#if expanded}
                          <ChevronDown
                            aria-hidden="true"
                            size={13}
                          />
                        {:else}
                          <ChevronRight
                            aria-hidden="true"
                            size={13}
                          />
                        {/if}
                      </IconButton>
                    {/if}
                    {#if result.sourceKey === "local"}
                      <IconButton
                        disabled={reloading || changesPending}
                        label={`Edit ${result.canonicalStatement}`}
                        onclick={() => openMappingEditor(result)}
                        size="sm"
                        title="Edit local mapping"
                        variant="ghost"
                      >
                        <Pencil
                          aria-hidden="true"
                          size={13}
                        />
                      </IconButton>
                      <IconButton
                        disabled={reloading || changesPending}
                        label={`Delete ${result.canonicalStatement}`}
                        onclick={() => requestRemoval(result)}
                        size="sm"
                        title="Delete local mapping"
                        variant="danger"
                      >
                        <Trash2
                          aria-hidden="true"
                          size={13}
                        />
                      </IconButton>
                    {:else}
                      <IconButton
                        disabled={reloading || changesPending}
                        label={`Override ${result.canonicalStatement} locally`}
                        onclick={() => openLocalOverride(result)}
                        size="sm"
                        title="Override locally"
                        variant="ghost"
                      >
                        <CopyPlus
                          aria-hidden="true"
                          size={13}
                        />
                      </IconButton>
                    {/if}
                  </div>
                </div>
                {#if expanded}
                  <div
                    class="mt-3 border-l-2 border-border-strong pl-3"
                    id={annotationRegionId(result)}
                  >
                    <p class="font-mono text-meta wrap-anywhere text-text-muted">{result.canonicalRecord}</p>
                    <ul class="mt-2 grid gap-1 font-mono text-meta text-text-secondary">
                      {#each result.metadata as annotation}
                        <li class="wrap-anywhere">{annotation}</li>
                      {/each}
                    </ul>
                  </div>
                {/if}
              </li>
            {/each}
          </ul>
        </div>
        <footer
          class="flex shrink-0 items-center justify-between border-t border-border-strong bg-surface-raised/30 px-4 py-2.5 sm:px-6"
        >
          <span class="text-meta text-text-muted">Page {pageIndex + 1}</span>
          <div class="flex items-center gap-1">
            <IconButton
              disabled={queryLoading || pageIndex === 0}
              label="Previous mapping page"
              onclick={showPreviousPage}
              size="sm"
              title="Previous page"
            >
              <ArrowLeft
                aria-hidden="true"
                size={14}
              />
            </IconButton>
            <IconButton
              disabled={queryLoading || !currentPage.nextCursor}
              label="Next mapping page"
              onclick={() => void showNextPage()}
              size="sm"
              title="Next page"
            >
              <ArrowRight
                aria-hidden="true"
                size={14}
              />
            </IconButton>
          </div>
        </footer>
      {/if}
    </section>
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
      class="sm:max-w-lg"
      style={editorOpen ? "translate: none" : undefined}
    >
      <form
        class="flex min-h-0 flex-1 flex-col"
        onsubmit={(event) => { event.preventDefault(); void saveMapping(); }}
      >
        <header class="flex shrink-0 items-start justify-between gap-4 border-b border-border-strong px-4 py-4 sm:px-5">
          <div class="min-w-0">
            <Dialog.Title class="text-title font-semibold text-text-primary">{editorTitle()}</Dialog.Title>
            <Dialog.Description class="mt-1 text-label text-text-muted"
              >The record is compiled into the local mapping source.</Dialog.Description
            >
          </div>
          <IconButton
            disabled={editorSaving}
            label="Close mapping editor"
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
          <label
            class="grid gap-1.5 text-label text-text-muted"
            for="local-mapping-record"
          >
            Mapping record
            <Textarea
              class="min-h-40 font-mono text-[0.78rem] leading-5"
              id="local-mapping-record"
              placeholder="co.anilist://anime/1 <=> org.themoviedb://tv/2"
              required
              spellcheck="false"
              bind:value={mappingRecord}
            />
          </label>
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
            {editorMode === "edit" ? "Save mapping" : "Add mapping"}
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
      <AlertDialog.Title class="text-title font-semibold text-text-primary">Delete local mapping?</AlertDialog.Title>
      <AlertDialog.Description class="mt-1 font-mono text-label leading-5 wrap-anywhere text-text-muted">
        {removalMapping?.canonicalStatement ?? "This mapping"}
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
          disabled={removalPending}
          >Cancel</AlertDialog.Cancel
        >
        <AlertDialog.Action
          class="inline-flex h-8 items-center gap-1.5 rounded-md border border-danger/30 bg-danger/8 px-3 text-label text-danger hover:bg-danger/12 disabled:pointer-events-none disabled:opacity-35"
          disabled={removalPending}
          onclick={(event) => { event.preventDefault(); void confirmRemoval(); }}
        >
          {#if removalPending}
            <Spinner size={13} />
          {/if}
          Delete
        </AlertDialog.Action>
      </div>
    </AlertDialog.Content>
  </AlertDialog.Root>
</div>
