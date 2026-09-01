<script lang="ts">
import { ChevronDown, LibraryBig, Plus, Search, X } from "@lucide/svelte";
import { Button, IconButton, Select, Spinner, TextInput } from "$lib";
import {
  CATALOG_PAGE_LIMIT,
  type CatalogEntry,
  formatPortableReference,
  formatTerm,
  getCatalog,
} from "$lib/api/catalog";
import {
  loadWorkspaceSourceDirectory,
  type WorkspaceSourceDirectory,
} from "$lib/features/workspaces/workspaceSourceDirectory";

type Props = {
  workspaceId: string;
  targetRow: number;
  onselect: (reference: string) => void;
};

let { workspaceId, targetRow, onselect }: Props = $props();

let pickerOpen = $state(false);
let directory = $state<WorkspaceSourceDirectory | null>(null);
let sourceId = $state("");
let query = $state("");
let search = $state("");
let entries = $state<CatalogEntry[]>([]);
let publicationId = $state("");
let nextCursor = $state<string | null>(null);
let loading = $state(false);
let loadingMore = $state(false);
let errorMessage = $state("");
let directorySequence = 0;
let loadSequence = 0;
let reloadSequence = $state(0);

let selectedSource = $derived(directory?.sources.find((entry) => entry.source.id === sourceId) ?? null);
let selectedSourceError = $derived(
  selectedSource && selectedSource.source.availability !== "available" ? "Source unavailable" : "",
);
let sourceItems = $derived(
  directory?.sources.map((entry) => ({ value: entry.source.id, label: entry.source.displayName })) ?? [],
);
let referenceEntries = $derived(entries.filter((entry) => Boolean(entry.item?.portableReferences.length)));

$effect(() => {
  const activeWorkspaceId = workspaceId;
  const sequence = ++directorySequence;
  loadSequence += 1;
  directory = null;
  sourceId = "";
  entries = [];
  publicationId = "";
  nextCursor = null;
  loading = false;
  loadingMore = false;
  if (!pickerOpen || !activeWorkspaceId) return;
  const controller = new AbortController();
  void loadDirectory(activeWorkspaceId, sequence, controller.signal);
  return () => controller.abort();
});

$effect(() => {
  const normalized = query.trim().split(/\s+/).join(" ").toLowerCase();
  const timeout = setTimeout(() => {
    if (search !== normalized) search = normalized;
  }, 250);
  return () => clearTimeout(timeout);
});

$effect(() => {
  const source = selectedSource;
  const currentSearch = search;
  reloadSequence;
  if (!pickerOpen || !source || selectedSourceError) {
    entries = [];
    publicationId = "";
    nextCursor = null;
    return;
  }
  const controller = new AbortController();
  void loadFirstPage(source.source.id, currentSearch, controller.signal);
  return () => controller.abort();
});

async function loadDirectory(activeWorkspaceId: string, sequence: number, signal?: AbortSignal): Promise<void> {
  loading = true;
  errorMessage = "";
  try {
    const result = await loadWorkspaceSourceDirectory(activeWorkspaceId, signal);
    if (sequence !== directorySequence) return;
    directory = result;
    sourceId = result.sources[0]?.source.id ?? "";
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (sequence === directorySequence) {
      errorMessage = error instanceof Error ? error.message : "Catalog sources could not be loaded";
    }
  } finally {
    if (sequence === directorySequence) loading = false;
  }
}

async function loadFirstPage(source: string, currentSearch: string, signal?: AbortSignal): Promise<void> {
  const sequence = ++loadSequence;
  loading = true;
  loadingMore = false;
  errorMessage = "";
  entries = [];
  nextCursor = null;
  try {
    const page = await getCatalog(
      workspaceId,
      source,
      { limit: CATALOG_PAGE_LIMIT, ...(currentSearch ? { search: currentSearch } : {}) },
      signal,
    );
    if (sequence !== loadSequence) return;
    entries = page.entries;
    publicationId = page.publicationId;
    nextCursor = page.nextCursor ?? null;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (sequence !== loadSequence) return;
    errorMessage = error instanceof Error ? error.message : "Catalog references could not be loaded";
  } finally {
    if (sequence === loadSequence) loading = false;
  }
}

async function loadMore(): Promise<void> {
  const source = selectedSource;
  const cursor = nextCursor;
  if (!source || !cursor || loadingMore) return;
  const sequence = loadSequence;
  const activeWorkspaceId = workspaceId;
  const activeSourceId = source.source.id;
  const activeSearch = search;
  const activePublicationId = publicationId;
  loadingMore = true;
  errorMessage = "";
  try {
    const page = await getCatalog(activeWorkspaceId, activeSourceId, {
      cursor,
      limit: CATALOG_PAGE_LIMIT,
      ...(activeSearch ? { search: activeSearch } : {}),
    });
    if (sequence !== loadSequence) return;
    if (page.publicationId !== activePublicationId) {
      reloadSequence += 1;
      return;
    }
    entries = [...entries, ...page.entries];
    nextCursor = page.nextCursor ?? null;
  } catch (error) {
    if (sequence === loadSequence) {
      errorMessage = error instanceof Error ? error.message : "More catalog references could not be loaded";
    }
  } finally {
    if (sequence === loadSequence) loadingMore = false;
  }
}
</script>

<section class="border-y border-border-strong bg-surface-raised/20">
  <button
    aria-expanded={pickerOpen}
    class="flex min-h-10 w-full min-w-0 items-center gap-2 px-4 py-2 text-left outline-none hover:bg-white/3 focus-visible:bg-white/4 sm:px-5"
    onclick={() => (pickerOpen = !pickerOpen)}
    type="button"
  >
    <LibraryBig
      aria-hidden="true"
      class="shrink-0 text-text-muted"
      size={13}
    />
    <span class="min-w-0 flex-1 text-meta font-[650] uppercase text-text-muted">Catalog references</span>
    <span class="text-meta text-text-disabled">Row {targetRow}</span>
    <ChevronDown
      aria-hidden="true"
      class={["shrink-0 text-text-muted transition-transform", pickerOpen && "rotate-180"]}
      size={13}
    />
  </button>

  {#if pickerOpen}
    <div class="border-t border-border-subtle">
      <div class="grid gap-2 border-b border-border-subtle px-4 py-3 sm:grid-cols-[11rem_minmax(0,1fr)] sm:px-5">
        <Select
          ariaLabel="Catalog source"
          disabled={loading || sourceItems.length === 0}
          items={sourceItems}
          onValueChange={(value) => (sourceId = value)}
          placeholder="No attached sources"
          triggerClass="w-full min-w-0"
          value={sourceId}
        />
        <label
          class="relative block min-w-0"
          for="mapping-catalog-search"
        >
          <span class="sr-only">Search catalog references</span>
          <Search
            aria-hidden="true"
            class="pointer-events-none absolute top-1/2 left-2.5 z-10 -translate-y-1/2 text-text-muted"
            size={13}
          />
          <TextInput
            class="w-full pl-8"
            controlSize="sm"
            disabled={loading || !selectedSource}
            id="mapping-catalog-search"
            maxlength={200}
            placeholder="Search catalog"
            type="search"
            bind:value={query}
          />
        </label>
      </div>

      {#if errorMessage}
        <div
          class="flex items-center gap-2 border-b border-danger/25 bg-danger/6 px-4 py-2 text-label text-danger sm:px-5"
          role="alert"
        >
          <span class="min-w-0 flex-1">{errorMessage}</span>
          <IconButton
            label="Dismiss catalog reference error"
            onclick={() => (errorMessage = "")}
            size="sm"
            variant="ghost"
          >
            <X
              aria-hidden="true"
              size={12}
            />
          </IconButton>
        </div>
      {/if}

      {#if loading && entries.length === 0}
        <div
          class="flex min-h-24 items-center justify-center gap-2 text-label text-text-muted"
          role="status"
        >
          <Spinner size={13} />
          Loading catalog references...
        </div>
      {:else if directory?.sources.length === 0}
        <div class="grid min-h-24 place-items-center px-4 text-center text-label text-text-muted">
          No attached sources.
        </div>
      {:else if selectedSourceError}
        <div class="grid min-h-24 place-items-center px-4 text-center text-label text-text-muted">
          {selectedSourceError}
        </div>
      {:else if referenceEntries.length === 0}
        <div class="grid min-h-24 place-items-center px-4 text-center text-label text-text-muted">
          No portable references in this page.
        </div>
      {:else}
        <ol class="max-h-72 overflow-y-auto">
          {#each referenceEntries as entry (`${entry.relation.key.namespace}:${entry.relation.key.value.join(",")}`)}
            <li class="border-b border-border-subtle px-4 py-2.5 last:border-b-0 sm:px-5">
              <div class="flex min-w-0 items-center gap-2">
                <div class="min-w-0 flex-1">
                  <p class="truncate text-label font-[620] text-text-secondary">{entry.item?.displayName}</p>
                  <p class="mt-0.5 text-meta text-text-disabled">{formatTerm(entry.item?.kind ?? null)}</p>
                </div>
              </div>
              <div class="mt-2 grid gap-1.5">
                {#each entry.item?.portableReferences ?? [] as reference}
                  {@const endpoint = formatPortableReference(reference)}
                  <div
                    class="flex min-w-0 items-center gap-2 rounded-control border border-border-subtle bg-black/10 px-2 py-1.5"
                  >
                    <code class="min-w-0 flex-1 break-all font-mono text-meta text-text-muted">{endpoint}</code>
                    <IconButton
                      label={`Add ${endpoint} to mapping row ${targetRow}`}
                      onclick={() => onselect(endpoint)}
                      size="sm"
                      title={`Add to row ${targetRow}`}
                      variant="ghost"
                    >
                      <Plus
                        aria-hidden="true"
                        size={12}
                      />
                    </IconButton>
                  </div>
                {/each}
              </div>
            </li>
          {/each}
        </ol>
        {#if nextCursor}
          <div class="border-t border-border-subtle px-4 py-2.5 sm:px-5">
            <Button
              disabled={loadingMore}
              onclick={() => void loadMore()}
              size="sm"
              variant="ghost"
            >
              {#if loadingMore}
                <Spinner size={12} />
              {/if}
              Load more
            </Button>
          </div>
        {/if}
      {/if}
    </div>
  {/if}
</section>
