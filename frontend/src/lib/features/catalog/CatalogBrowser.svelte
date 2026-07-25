<script lang="ts">
import { ArrowLeft, ChevronRight, Database, Layers3, RefreshCw, RotateCcw, Search, X } from "@lucide/svelte";
import { onDestroy } from "svelte";
import { AlertDialog, Button, Chip, IconButton, MediaArtwork, Panel, Select, Spinner, Tabs, TextInput } from "$lib";
import {
  CATALOG_PAGE_LIMIT,
  type CatalogAsset,
  type CatalogEntry,
  formatPortableReference,
  formatProviderKey,
  formatTerm,
  getCatalog,
  type ProviderKey,
} from "$lib/api/catalog";
import { pollCatalog, resetAndRefetch, waitForTask } from "$lib/api/operations";
import type { WorkspaceCatalog, WorkspaceCatalogSource } from "./workspaceCatalog";

type BrowsePart = { key: ProviderKey; label: string };

type Props = {
  data: WorkspaceCatalog;
  onreload: () => Promise<void>;
};

let { data, onreload }: Props = $props();

let sourceFilter = $state("");
let query = $state("");
let catalogSearch = $state("");
let kindFilter = $state("all");
let browsePath = $state<BrowsePart[]>([]);
let entries = $state<CatalogEntry[]>([]);
let publicationId = $state("");
let nextCursor = $state<string | null>(null);
let loading = $state(false);
let loadingMore = $state(false);
let operationBusy = $state(false);
let errorMessage = $state("");
let focusedRelationId = $state("");
let resetOpen = $state(false);
let reloadSequence = $state(0);
let requestSequence = 0;
let operationSequence = 0;
let operationWorkspaceId = "";
let operationController: AbortController | null = null;

let selectedSource = $derived(data.sources.find((candidate) => candidate.source.id === sourceFilter) ?? null);
let currentParent = $derived(browsePath.at(-1)?.key);
let focusedEntry = $derived(entries.find((entry) => relationId(entry) === focusedRelationId) ?? null);
let kindItems = $derived.by(() => {
  const kinds = new Map<string, string>();
  for (const entry of entries) {
    const kind = entry.item?.kind ?? entry.relation.kind;
    kinds.set(termKey(kind), formatTerm(kind));
  }
  return [
    { value: "all", label: "All kinds" },
    ...[...kinds].sort((left, right) => left[1].localeCompare(right[1])).map(([value, label]) => ({ value, label })),
  ];
});
let visibleEntries = $derived(
  kindFilter === "all"
    ? entries
    : entries.filter((entry) => termKey(entry.item?.kind ?? entry.relation.kind) === kindFilter),
);

$effect(() => {
  if (data.workspace.id === operationWorkspaceId) return;
  operationWorkspaceId = data.workspace.id;
  invalidateOperation();
});

$effect(() => {
  if (data.sources.some((source) => source.source.id === sourceFilter)) return;
  invalidateOperation();
  sourceFilter = data.sources[0]?.source.id ?? "";
  browsePath = [];
  focusedRelationId = "";
});

onDestroy(invalidateOperation);

$effect(() => {
  const normalized = query.trim().split(/\s+/).join(" ").toLowerCase();
  const timeout = setTimeout(() => {
    if (catalogSearch !== normalized) catalogSearch = normalized;
  }, 250);
  return () => clearTimeout(timeout);
});

$effect(() => {
  const source = selectedSource;
  const search = catalogSearch;
  const parent = currentParent;
  reloadSequence;
  if (!source || source.error) {
    requestSequence += 1;
    entries = [];
    publicationId = "";
    nextCursor = null;
    loading = false;
    loadingMore = false;
    return;
  }
  const controller = new AbortController();
  void loadFirstPage(source, parent, search, controller.signal);
  return () => controller.abort();
});

async function loadFirstPage(
  source: WorkspaceCatalogSource,
  parent: ProviderKey | undefined,
  search: string,
  signal?: AbortSignal,
) {
  const requestId = ++requestSequence;
  loading = true;
  loadingMore = false;
  errorMessage = "";
  entries = [];
  focusedRelationId = "";
  kindFilter = "all";
  try {
    const page = await getCatalog(
      data.workspace.id,
      source.source.id,
      {
        limit: CATALOG_PAGE_LIMIT,
        ...(parent ? { parent } : {}),
        ...(search ? { search } : {}),
      },
      signal,
    );
    if (requestId !== requestSequence) return;
    entries = page.entries;
    publicationId = page.publicationId;
    nextCursor = page.nextCursor ?? null;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (requestId !== requestSequence) return;
    errorMessage = error instanceof Error ? error.message : "Catalog could not be loaded";
  } finally {
    if (requestId === requestSequence) loading = false;
  }
}

async function loadMore() {
  const source = selectedSource;
  const cursor = nextCursor;
  if (!source || !cursor || loadingMore) return;
  const requestId = requestSequence;
  const workspaceId = data.workspace.id;
  const sourceId = source.source.id;
  const parent = currentParent;
  const search = catalogSearch;
  const currentPublicationId = publicationId;
  loadingMore = true;
  errorMessage = "";
  try {
    const page = await getCatalog(workspaceId, sourceId, {
      cursor,
      limit: CATALOG_PAGE_LIMIT,
      ...(parent ? { parent } : {}),
      ...(search ? { search } : {}),
    });
    if (requestId !== requestSequence) return;
    if (page.publicationId !== currentPublicationId) {
      reloadSequence += 1;
      return;
    }
    entries = [...entries, ...page.entries];
    nextCursor = page.nextCursor ?? null;
  } catch (error) {
    if (requestId === requestSequence) {
      errorMessage = error instanceof Error ? error.message : "More catalog entries could not be loaded";
    }
  } finally {
    if (requestId === requestSequence) loadingMore = false;
  }
}

function changeSource(value: string) {
  invalidateOperation();
  errorMessage = "";
  sourceFilter = value;
  browsePath = [];
  focusedRelationId = "";
}

function drillInto(entry: CatalogEntry) {
  if (!entry.hasChildren) return;
  browsePath = [...browsePath, { key: entry.relation.key, label: entryLabel(entry) }];
  focusedRelationId = "";
}

function browseBack() {
  browsePath = browsePath.slice(0, -1);
  focusedRelationId = "";
}

function setBrowseDepth(index: number) {
  browsePath = browsePath.slice(0, index + 1);
  focusedRelationId = "";
}

async function refreshCatalog() {
  const source = selectedSource;
  if (!source || operationBusy) return;
  const sequence = ++operationSequence;
  const controller = new AbortController();
  operationController?.abort();
  operationController = controller;
  const activeWorkspaceId = data.workspace.id;
  const sourceId = source.source.id;
  operationBusy = true;
  errorMessage = "";
  try {
    const result = await waitForTask(
      activeWorkspaceId,
      await pollCatalog(activeWorkspaceId, sourceId, controller.signal),
      controller.signal,
    );
    if (!ownsOperation(sequence, activeWorkspaceId, sourceId)) return;
    if (result.status !== "succeeded") throw new Error(result.failure?.code ?? "Catalog refresh failed");
    await onreload();
    if (!ownsOperation(sequence, activeWorkspaceId, sourceId)) return;
    reloadSequence += 1;
  } catch (error) {
    if (ownsOperation(sequence, activeWorkspaceId, sourceId)) {
      errorMessage = error instanceof Error ? error.message : "Catalog refresh failed";
    }
  } finally {
    if (operationController === controller) operationController = null;
    if (ownsOperation(sequence, activeWorkspaceId, sourceId)) operationBusy = false;
  }
}

async function confirmReset() {
  const source = selectedSource;
  if (!source || operationBusy) return;
  const sequence = ++operationSequence;
  const controller = new AbortController();
  operationController?.abort();
  operationController = controller;
  const activeWorkspaceId = data.workspace.id;
  const sourceId = source.source.id;
  operationBusy = true;
  errorMessage = "";
  try {
    const result = await waitForTask(
      activeWorkspaceId,
      await resetAndRefetch(activeWorkspaceId, sourceId, controller.signal),
      controller.signal,
    );
    if (!ownsOperation(sequence, activeWorkspaceId, sourceId)) return;
    if (result.status !== "succeeded") throw new Error(result.failure?.code ?? "Reset and refetch failed");
    resetOpen = false;
    browsePath = [];
    await onreload();
    if (!ownsOperation(sequence, activeWorkspaceId, sourceId)) return;
    reloadSequence += 1;
  } catch (error) {
    if (ownsOperation(sequence, activeWorkspaceId, sourceId)) {
      errorMessage = error instanceof Error ? error.message : "Reset and refetch failed";
    }
  } finally {
    if (operationController === controller) operationController = null;
    if (ownsOperation(sequence, activeWorkspaceId, sourceId)) operationBusy = false;
  }
}

function invalidateOperation(): void {
  operationSequence += 1;
  operationBusy = false;
  operationController?.abort();
  operationController = null;
}

function ownsOperation(sequence: number, activeWorkspaceId: string, sourceId: string): boolean {
  return (
    sequence === operationSequence && data.workspace.id === activeWorkspaceId && selectedSource?.source.id === sourceId
  );
}

function relationId(entry: CatalogEntry): string {
  return `${entry.relation.key.namespace}:${entry.relation.key.value.join(",")}`;
}

function entryLabel(entry: CatalogEntry): string {
  return entry.item?.displayName.trim() || formatProviderKey(entry.relation.providerItemKey ?? entry.relation.key);
}

function entryKind(entry: CatalogEntry): string {
  return formatTerm(entry.item?.kind ?? entry.relation.kind);
}

function preferredArtwork(entry: CatalogEntry | null): CatalogAsset | undefined {
  if (!entry?.item) return;
  const priority = ["poster", "primary", "thumbnail", "thumb", "background", "backdrop"];
  return [...entry.item.binaryAssets].sort((left, right) => {
    const leftIndex = priority.indexOf(left.kind.name);
    const rightIndex = priority.indexOf(right.kind.name);
    return (leftIndex < 0 ? priority.length : leftIndex) - (rightIndex < 0 ? priority.length : rightIndex);
  })[0];
}

function termKey(term: { namespace: string; name: string }): string {
  return `${term.namespace}:${term.name}`;
}

function rawEntry(entry: CatalogEntry): string {
  return JSON.stringify(entry, null, 2);
}
</script>

<div class="flex min-h-0 flex-1 flex-col overflow-hidden">
  <div class="shrink-0 border-b border-border-subtle bg-surface-panel px-4 py-3 sm:px-6">
    {#if data.sources.length > 0}
      <div class="mb-2.5 flex min-w-0 items-center gap-2">
        <div
          class="min-w-0 flex-1 overflow-x-auto overscroll-x-contain scrollbar-thin [scrollbar-color:var(--color-border-strong)_transparent]"
        >
          <Tabs.Root
            onValueChange={changeSource}
            value={sourceFilter}
          >
            <Tabs.List
              aria-label="Catalog sources"
              class="min-w-max rounded-md border border-border-subtle bg-surface-raised p-0.5"
            >
              {#each data.sources as source (source.source.id)}
                <Tabs.Trigger value={source.source.id}>{source.source.displayName}</Tabs.Trigger>
              {/each}
            </Tabs.List>
          </Tabs.Root>
        </div>
        {#if selectedSource}
          <Button
            aria-label={`Scan ${selectedSource.source.displayName}`}
            class="size-7 p-0 sm:w-auto sm:px-2.5"
            disabled={operationBusy || selectedSource.source.availability !== "available"}
            onclick={refreshCatalog}
            size="sm"
            title={`Scan ${selectedSource.source.displayName}`}
            variant="ghost"
            ><RefreshCw
              aria-hidden="true"
              class={operationBusy ? "animate-spin" : ""}
              size={13}
            /><span class="hidden sm:inline">Scan</span></Button
          >
          <Button
            aria-label={`Reset ${selectedSource.source.displayName}`}
            class="size-7 p-0 sm:w-auto sm:px-2.5"
            disabled={operationBusy}
            onclick={() => (resetOpen = true)}
            size="sm"
            title={`Reset and refetch ${selectedSource.source.displayName}`}
            variant="danger"
            ><RotateCcw
              aria-hidden="true"
              size={13}
            /><span class="hidden sm:inline">Reset</span></Button
          >
        {/if}
      </div>
    {/if}
    <div class="grid min-w-0 gap-2 sm:grid-cols-[minmax(0,1fr)_12rem]">
      <label
        class="relative block min-w-0"
        for="catalog-search"
        ><span class="sr-only">Search catalog</span>
        <Search
          aria-hidden="true"
          class="pointer-events-none absolute top-1/2 left-2.5 z-10 -translate-y-1/2 text-text-muted"
          size={14}
        />
        <TextInput
          class="w-full border-border-strong bg-surface-raised pl-8"
          id="catalog-search"
          maxlength={200}
          oninput={(event) => (query = event.currentTarget.value)}
          placeholder="Search this source"
          type="search"
          value={query}
        /></label
      >
      <Select
        ariaLabel="Catalog kind"
        items={kindItems}
        onValueChange={(value) => (kindFilter = value)}
        triggerClass="w-full"
        value={kindFilter}
      />
    </div>
  </div>

  {#if errorMessage}
    <div class="shrink-0 border-b border-danger/20 bg-danger/6 px-4 py-2.5 sm:px-6">
      <div class="flex items-center justify-between gap-3 text-label text-danger">
        <span>{errorMessage}</span
        ><IconButton
          label="Dismiss catalog error"
          onclick={() => (errorMessage = "")}
          size="sm"
          variant="danger"
          ><X
            aria-hidden="true"
            size={14}
          /></IconButton
        >
      </div>
    </div>
  {/if}

  {#if data.sources.length === 0}
    <div class="grid min-h-0 flex-1 place-items-center p-6">
      <Panel
        class="grid min-h-44 w-full max-w-xl place-items-center p-6 text-center"
        tone="dashed"
        ><div>
          <Database
            aria-hidden="true"
            class="mx-auto text-text-disabled"
            size={25}
          />
          <h2 class="mt-3 text-heading font-semibold text-text-primary">No attached sources</h2>
          <p class="mt-1 text-label text-text-muted">Attach a discovered source from Connections.</p>
        </div></Panel
      >
    </div>
  {:else if selectedSource?.error}
    <div class="grid min-h-0 flex-1 place-items-center p-6">
      <Panel
        class="grid min-h-44 w-full max-w-xl place-items-center p-6 text-center"
        tone="dashed"
        ><div>
          <Layers3
            aria-hidden="true"
            class="mx-auto text-text-disabled"
            size={25}
          />
          <h2 class="mt-3 text-heading font-semibold text-text-primary">Catalog unavailable</h2>
          <p class="mt-1 text-label text-text-muted">{selectedSource.error}</p>
        </div></Panel
      >
    </div>
  {:else}
    <div class="min-h-0 flex-1 overflow-auto">
      <div class="mx-auto grid w-full max-w-6xl gap-4 p-4 lg:grid-cols-[minmax(0,1fr)_22rem] lg:p-6">
        <section class="min-w-0">
          {#if browsePath.length > 0}
            <nav
              aria-label="Catalog hierarchy"
              class="mb-3 flex min-w-0 items-center gap-1 overflow-x-auto text-meta text-text-muted"
            >
              <IconButton
                label="Back one catalog level"
                onclick={browseBack}
                size="sm"
                ><ArrowLeft
                  aria-hidden="true"
                  size={13}
                /></IconButton
              ><button
                class="shrink-0 rounded px-1.5 py-1 hover:text-text-primary"
                onclick={() => { browsePath = []; focusedRelationId = ""; }}
                type="button"
              >
                Root
              </button>
              {#each browsePath as part, index (relationId({ relation: { key: part.key } } as CatalogEntry))}
                <ChevronRight
                  aria-hidden="true"
                  class="shrink-0"
                  size={12}
                /><button
                  class="max-w-44 truncate rounded px-1.5 py-1 hover:text-text-primary"
                  onclick={() => setBrowseDepth(index)}
                  type="button"
                >
                  {part.label}
                </button>
              {/each}
            </nav>
          {/if}

          {#if loading}
            <div
              class="flex min-h-48 items-center justify-center gap-2 text-label text-text-muted"
              role="status"
            >
              <Spinner size={16} />Loading published catalog...
            </div>
          {:else if visibleEntries.length === 0}
            <Panel
              class="grid min-h-48 place-items-center p-6 text-center"
              tone="dashed"
              ><div>
                <Layers3
                  aria-hidden="true"
                  class="mx-auto text-text-disabled"
                  size={24}
                />
                <h2 class="mt-3 text-heading font-semibold text-text-primary">No catalog entries</h2>
                <p class="mt-1 text-label text-text-muted">
                  {catalogSearch ? "No entries match this search." : "Run a catalog scan to publish this source."}
                </p>
              </div></Panel
            >
          {:else}
            <div class="overflow-hidden rounded-md border border-border-subtle bg-surface-panel">
              <ul class="divide-y divide-border-subtle">
                {#each visibleEntries as entry (relationId(entry))}
                  {@const artwork = preferredArtwork(entry)}
                  <li
                    class="group flex min-h-20 items-center gap-3 px-3 py-2.5 transition-colors hover:bg-white/3 sm:px-4"
                  >
                    <button
                      aria-label={`Inspect ${entryLabel(entry)}`}
                      class="flex min-w-0 flex-1 items-center gap-3 text-left outline-none focus-visible:ring-2 focus-visible:ring-accent/25"
                      onclick={() => (focusedRelationId = relationId(entry))}
                      type="button"
                    >
                      <MediaArtwork
                        alt=""
                        class="w-11 shrink-0"
                        shape="square"
                        src={artwork?.href}
                      />
                      <span class="min-w-0 flex-1"
                        ><span class="block truncate text-label font-[540] text-text-primary">{entryLabel(entry)}</span
                        ><span class="mt-1 flex flex-wrap items-center gap-2 text-meta text-text-muted"
                          ><span class="capitalize">{entryKind(entry)}</span><span aria-hidden="true">/</span
                          ><span class="font-mono"
                            >{formatProviderKey(entry.item?.key ?? entry.relation.key)}</span
                          ></span
                        ></span
                      >
                    </button>
                    {#if entry.hasChildren}
                      <IconButton
                        label={`Browse ${entryLabel(entry)}`}
                        onclick={() => drillInto(entry)}
                        size="sm"
                        title="Browse children"
                        ><ChevronRight
                          aria-hidden="true"
                          size={15}
                        /></IconButton
                      >
                    {/if}
                  </li>
                {/each}
              </ul>
            </div>
            {#if nextCursor}
              <div class="mt-4 flex justify-center">
                <Button
                  disabled={loadingMore}
                  onclick={loadMore}
                  size="sm"
                  variant="ghost"
                  >{#if loadingMore}
                    <Spinner size={14} />
                  {/if}
                  Load more</Button
                >
              </div>
            {/if}
          {/if}
        </section>

        <aside class="min-w-0">
          {#if focusedEntry}
            <Panel
              class="sticky top-0 overflow-hidden"
              element="section"
            >
              <div class="relative aspect-video bg-surface-raised">
                <MediaArtwork
                  alt=""
                  class="absolute inset-0 size-full rounded-none border-0"
                  shape="landscape"
                  src={preferredArtwork(focusedEntry)?.href}
                /><IconButton
                  class="absolute top-2 right-2 z-10 border-border-strong bg-surface-overlay/90"
                  label="Close catalog details"
                  onclick={() => (focusedRelationId = "")}
                  size="sm"
                  ><X
                    aria-hidden="true"
                    size={14}
                  /></IconButton
                >
              </div>
              <div class="grid gap-4 p-4">
                <div>
                  <div class="flex flex-wrap gap-2">
                    <Chip tone="info">{entryKind(focusedEntry)}</Chip>
                    {#if focusedEntry.hasChildren}
                      <Chip tone="accent">Container</Chip>
                    {/if}
                  </div>
                  <h2 class="mt-2 text-title font-semibold text-text-primary">{entryLabel(focusedEntry)}</h2>
                  <p class="mt-1 break-all font-mono text-meta text-text-disabled">
                    {formatProviderKey(focusedEntry.item?.key ?? focusedEntry.relation.key)}
                  </p>
                </div>
                {#if focusedEntry.item?.portableReferences.length}
                  <section>
                    <h3 class="text-meta font-semibold uppercase text-text-disabled">Portable references</h3>
                    <div class="mt-2 grid gap-1.5">
                      {#each focusedEntry.item.portableReferences as reference}
                        <p
                          class="break-all rounded-md border border-border-subtle bg-black/10 px-2.5 py-2 font-mono text-meta text-text-muted"
                        >
                          {formatPortableReference(reference)}
                        </p>
                      {/each}
                    </div>
                  </section>
                {/if}
                <details>
                  <summary class="cursor-pointer text-label text-text-muted">Raw published entry</summary>
                  <pre
                    class="mt-2 max-h-72 overflow-auto rounded-md border border-border-subtle bg-black/15 p-3 font-mono text-caption text-text-muted"
                  >{rawEntry(focusedEntry)}</pre>
                </details>
              </div>
            </Panel>
          {:else}
            <Panel
              class="hidden min-h-40 place-items-center p-5 text-center text-label text-text-muted lg:grid"
              tone="dashed"
              >Select an entry to inspect its published data.</Panel
            >
          {/if}
        </aside>
      </div>
    </div>
  {/if}
</div>

<AlertDialog.Root bind:open={resetOpen}>
  <AlertDialog.Content>
    <AlertDialog.Title class="text-title font-semibold text-text-primary"
      >Reset and refetch this catalog?</AlertDialog.Title
    >
    <AlertDialog.Description class="mt-1 text-label leading-5 text-text-muted"
      >The current published catalog for {selectedSource?.source.displayName ?? "this source"} will be cleared, then
      rebuilt with a full provider read.</AlertDialog.Description
    >
    <div class="mt-5 flex justify-end gap-2">
      <AlertDialog.Cancel
        class="inline-flex h-8 items-center rounded-md border border-border-subtle px-3 text-label text-text-secondary hover:bg-white/4"
        >Cancel</AlertDialog.Cancel
      ><AlertDialog.Action
        class="inline-flex h-8 items-center gap-1.5 rounded-md border border-danger/30 bg-danger/8 px-3 text-label text-danger hover:bg-danger/12 disabled:pointer-events-none disabled:opacity-35"
        disabled={operationBusy}
        onclick={confirmReset}
        >{#if operationBusy}
          <Spinner size={13} />
        {/if}
        Reset and refetch</AlertDialog.Action
      >
    </div>
  </AlertDialog.Content>
</AlertDialog.Root>
