<script lang="ts">
import {
  ArrowLeft,
  ArrowRight,
  ChevronRight,
  CircleCheck,
  Copy,
  Database,
  FolderOpen,
  Layers,
  LayoutGrid,
  List,
  RefreshCw,
  RotateCcw,
  Search,
  X,
} from "@lucide/svelte";
import { onDestroy, onMount } from "svelte";
import { goto } from "$app/navigation";
import {
  AlertDialog,
  Button,
  EmptyState,
  IconButton,
  MediaArtwork,
  PageToolbar,
  Panel,
  Select,
  Spinner,
  Tabs,
  TextInput,
} from "$lib";
import {
  CATALOG_PAGE_LIMIT,
  type CatalogAsset,
  type CatalogEntry,
  type CatalogPage,
  formatPortableReference,
  formatProviderKey,
  formatTerm,
  getCatalog,
  type ProviderKey,
} from "$lib/api/catalog";
import { pollCatalog, resetAndRefetch, waitForTask } from "$lib/api/operations";
import { type CatalogViewMode, catalogViewPreference, decodeCatalogViewMode } from "./catalogPreferences";
import { catalogPublicationIssue } from "./catalogPublicationIssue";
import type { WorkspaceCatalog, WorkspaceCatalogSource } from "./workspaceCatalog";

type BrowsePart = { key: ProviderKey; label: string };
type AttributeRow = { key: string; label: string; value: string };
type EncodedAttribute = {
  term?: { name?: unknown; namespace?: unknown };
  value?: { kind?: unknown; value?: unknown };
};

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
let catalogPages = $state<CatalogPage[]>([]);
let pageIndex = $state(0);
let loading = $state(false);
let loadingPage = $state(false);
let operationBusy = $state(false);
let errorMessage = $state("");
let focusedRelationId = $state("");
let resetOpen = $state(false);
let copiedReferenceId = $state("");
let viewMode = $state<CatalogViewMode>("grid");
let reloadSequence = $state(0);
let requestSequence = 0;
let operationSequence = 0;
let operationWorkspaceId = "";
let operationController: AbortController | null = null;
let copyTimer: ReturnType<typeof setTimeout> | undefined;

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

onMount(() => {
  viewMode = catalogViewPreference.read() ?? "grid";
  return catalogViewPreference.subscribe((value) => (viewMode = value ?? "grid"));
});

onDestroy(() => {
  invalidateOperation();
  if (copyTimer) clearTimeout(copyTimer);
});

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
    catalogPages = [];
    pageIndex = 0;
    loading = false;
    loadingPage = false;
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
  loadingPage = false;
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
    catalogPages = [page];
    pageIndex = 0;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (requestId !== requestSequence) return;
    const message = await catalogLoadErrorMessage(error, source.source.id, signal);
    if (requestId === requestSequence) errorMessage = message;
  } finally {
    if (requestId === requestSequence) loading = false;
  }
}

async function catalogLoadErrorMessage(error: unknown, sourceId: string, signal?: AbortSignal): Promise<string> {
  const issue = await catalogPublicationIssue(error, data.workspace.id, sourceId, signal);
  if (issue === "scan-active") return "A catalog scan is in progress. Entries will appear when it finishes.";
  if (issue === "unpublished") return "This source has not published a catalog yet. Start a scan to load its entries.";
  return error instanceof Error ? error.message : "Catalog could not be loaded";
}

async function showNextPage() {
  const source = selectedSource;
  const cursor = nextCursor;
  if (!source || loadingPage) return;
  const cachedPage = catalogPages[pageIndex + 1];
  if (cachedPage) {
    showCatalogPage(cachedPage, pageIndex + 1);
    return;
  }
  if (!cursor) return;
  const requestId = requestSequence;
  const workspaceId = data.workspace.id;
  const sourceId = source.source.id;
  const parent = currentParent;
  const search = catalogSearch;
  const currentPublicationId = publicationId;
  loadingPage = true;
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
    catalogPages = [...catalogPages.slice(0, pageIndex + 1), page];
    showCatalogPage(page, pageIndex + 1);
  } catch (error) {
    if (requestId === requestSequence) {
      errorMessage = error instanceof Error ? error.message : "Catalog page could not be loaded";
    }
  } finally {
    if (requestId === requestSequence) loadingPage = false;
  }
}

function showPreviousPage() {
  if (loadingPage || pageIndex === 0) return;
  const previousIndex = pageIndex - 1;
  showCatalogPage(catalogPages[previousIndex], previousIndex);
}

function showCatalogPage(page: CatalogPage, nextPageIndex: number) {
  entries = page.entries;
  publicationId = page.publicationId;
  nextCursor = page.nextCursor ?? null;
  pageIndex = nextPageIndex;
  focusedRelationId = "";
  kindFilter = "all";
}

function changeSource(value: string) {
  invalidateOperation();
  errorMessage = "";
  sourceFilter = value;
  browsePath = [];
  focusedRelationId = "";
}

function changeViewMode(value: string): void {
  const nextViewMode = decodeCatalogViewMode(value);
  if (!nextViewMode) return;
  viewMode = nextViewMode;
  catalogViewPreference.write(nextViewMode);
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
    if (result.status !== "succeeded") {
      throw new Error(
        result.status === "cancelled"
          ? "The catalog scan was cancelled."
          : "The catalog scan did not finish. Check the source connection and try again.",
      );
    }
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
    if (result.status !== "succeeded") {
      throw new Error(
        result.status === "cancelled"
          ? "The catalog reset was cancelled."
          : "The catalog reset or refetch did not finish. Check the source connection and try again.",
      );
    }
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
  return artworkByPriority(entry, ["poster", "cover", "primary", "thumbnail", "thumb", "background", "backdrop"]);
}

function preferredBannerArtwork(entry: CatalogEntry | null): CatalogAsset | undefined {
  return artworkByPriority(entry, ["banner", "background", "backdrop", "landscape", "hero", "primary", "poster"]);
}

function artworkByPriority(entry: CatalogEntry | null, priority: string[]): CatalogAsset | undefined {
  if (!entry?.item) return;
  return [...entry.item.binaryAssets].sort((left, right) => {
    const leftIndex = priority.indexOf(left.kind.name.toLowerCase());
    const rightIndex = priority.indexOf(right.kind.name.toLowerCase());
    return (leftIndex < 0 ? priority.length : leftIndex) - (rightIndex < 0 ? priority.length : rightIndex);
  })[0];
}

function termKey(term: { namespace: string; name: string }): string {
  return `${term.namespace}:${term.name}`;
}

function attributeRows(attributes: Record<string, unknown>): AttributeRow[] {
  if (Array.isArray(attributes.entries)) return encodedAttributeRows(attributes.entries);
  return Object.entries(attributes)
    .map(([key, value]) => ({
      key,
      label: formatAttributeLabel(key),
      value: formatGenericAttributeValue(value),
    }))
    .sort((left, right) => left.label.localeCompare(right.label));
}

function encodedAttributeRows(entries: unknown[]): AttributeRow[] {
  const grouped = new Map<string, AttributeRow & { values: string[] }>();
  for (const candidate of entries) {
    if (!isRecord(candidate) || !isRecord(candidate.term)) continue;
    const attribute = candidate as EncodedAttribute;
    const name = typeof attribute.term?.name === "string" ? attribute.term.name : "";
    if (!name) continue;
    const namespace = typeof attribute.term?.namespace === "string" ? attribute.term.namespace : "";
    const encodedValue = isRecord(attribute.value) ? attribute.value : {};
    const kind = typeof encodedValue.kind === "string" ? encodedValue.kind : "";
    const value = formatEncodedAttributeValue(kind, encodedValue.value);
    const key = `${namespace}:${name}`;
    const existing = grouped.get(key);
    if (existing) {
      if (!existing.values.includes(value)) existing.values.push(value);
      continue;
    }
    grouped.set(key, {
      key,
      label: formatAttributeLabel(name),
      value: "",
      values: [value],
    });
  }
  return [...grouped.values()]
    .sort((left, right) => left.label.localeCompare(right.label) || left.key.localeCompare(right.key))
    .map(({ key, label, values }) => ({ key, label, value: values.join(", ") }));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function formatAttributeLabel(value: string): string {
  return value
    .replaceAll(/[-_]/g, " ")
    .replaceAll(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/^./, (character) => character.toUpperCase());
}

function formatEncodedAttributeValue(kind: string, value: unknown): string {
  if (kind === "timestamp_milliseconds" && typeof value === "number") {
    return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(new Date(value));
  }
  return formatGenericAttributeValue(value);
}

function formatGenericAttributeValue(value: unknown): string {
  if (value === null || value === undefined || value === "") return "None";
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (typeof value === "number") return new Intl.NumberFormat().format(value);
  if (Array.isArray(value)) {
    return value.map(formatGenericAttributeValue).join(", ");
  }
  if (isRecord(value)) {
    return Object.entries(value)
      .map(([key, nestedValue]) => `${formatAttributeLabel(key)}: ${formatGenericAttributeValue(nestedValue)}`)
      .join("; ");
  }
  return String(value);
}

async function copyPortableReference(referenceId: string, value: string): Promise<void> {
  try {
    await navigator.clipboard.writeText(value);
    copiedReferenceId = referenceId;
    if (copyTimer) clearTimeout(copyTimer);
    copyTimer = setTimeout(() => {
      if (copiedReferenceId === referenceId) copiedReferenceId = "";
      copyTimer = undefined;
    }, 1_500);
  } catch (error) {
    errorMessage = error instanceof Error ? error.message : "Reference could not be copied";
  }
}
</script>

{#snippet catalogDetails(entry: CatalogEntry)}
  {@const artwork = preferredBannerArtwork(entry)}
  {@const itemAttributes = attributeRows(entry.item?.attributes ?? {})}
  {@const relationAttributes = attributeRows(entry.relation.attributes)}
  <Panel
    aria-label={`Catalog details for ${entryLabel(entry)}`}
    class="min-h-full overflow-hidden rounded-none border-0 shadow-none"
    element="section"
  >
    <div class="relative overflow-hidden border-b border-border-subtle bg-surface-raised">
      <MediaArtwork
        alt={`Banner for ${entryLabel(entry)}`}
        class="w-full rounded-none border-0 shadow-none"
        shape="landscape"
        src={artwork?.href}
      />
      <div class="pointer-events-none absolute inset-x-0 bottom-0 z-10 p-4 pr-12">
        <h2
          class="text-title font-semibold text-white drop-shadow-[0_2px_8px_rgb(0_0_0/80%)]"
          title={formatProviderKey(entry.item?.key ?? entry.relation.key)}
        >
          {entryLabel(entry)}
        </h2>
      </div>
      <div class="absolute top-2.5 right-2.5 z-10">
        <IconButton
          class="border-border-strong bg-black/55 backdrop-blur-sm"
          label="Close catalog details"
          onclick={() => (focusedRelationId = "")}
          size="sm"
          ><X
            aria-hidden="true"
            size={14}
          /></IconButton
        >
      </div>
    </div>
    <div class="grid gap-5 p-4">
      {#if entry.hasChildren}
        <div class="flex flex-wrap gap-2">
          <Button
            onclick={() => drillInto(entry)}
            size="sm"
            variant="primary"
          >
            <FolderOpen
              aria-hidden="true"
              size={14}
            />
            Browse collection
          </Button>
        </div>
      {/if}

      <section>
        <h3 class="text-meta font-semibold uppercase text-text-disabled">Published details</h3>
        <dl class="mt-2 overflow-hidden rounded-md border border-border-subtle bg-black/10">
          <div class="grid min-h-10 grid-cols-[minmax(6.5rem,2fr)_minmax(0,3fr)] items-center gap-4 px-3 py-2">
            <dt class="text-label leading-5 text-text-muted">Media type</dt>
            <dd class="text-label leading-5 capitalize text-text-secondary">{entryKind(entry)}</dd>
          </div>
        </dl>
      </section>

      {#if entry.item?.portableReferenceCandidates.length}
        <section>
          <h3 class="text-meta font-semibold uppercase text-text-disabled">Portable references</h3>
          <div class="mt-2 grid gap-1.5">
            {#each entry.item.portableReferenceCandidates as reference, index}
              {@const referenceValue = formatPortableReference(reference)}
              {@const referenceId = `${relationId(entry)}:${index}`}
              {@const recommended = reference.providerRecommended}
              {@const effective = reference.effectiveMappingRoot}

              <div
                class="flex min-h-10 items-center gap-2 rounded-md border border-border-subtle bg-black/10 px-3 py-2"
              >
                <div class="min-w-0 flex-1">
                  <div
                    class="truncate font-mono text-label leading-5 text-text-secondary"
                    title={referenceValue}
                  >
                    {referenceValue}
                  </div>
                  {#if recommended || effective}
                    <div class="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-meta font-[560]">
                      {#if recommended}
                        <span class="text-info">Provider recommended</span>
                      {/if}
                      {#if effective}
                        <span class="text-success">Effective root</span>
                      {/if}
                    </div>
                  {/if}
                </div>

                <IconButton
                  class="shrink-0 border-border-subtle bg-surface-raised/90"
                  label={`Copy portable reference ${referenceValue}`}
                  onclick={() => void copyPortableReference(referenceId, referenceValue)}
                  size="sm"
                  title="Copy portable reference"
                >
                  {#if copiedReferenceId === referenceId}
                    <CircleCheck
                      aria-hidden="true"
                      size={13}
                    />
                  {:else}
                    <Copy
                      aria-hidden="true"
                      size={13}
                    />
                  {/if}
                </IconButton>
              </div>
            {/each}
          </div>
        </section>
      {/if}
      {#if itemAttributes.length || relationAttributes.length}
        <section>
          <h3 class="text-meta font-semibold uppercase text-text-disabled">Attributes</h3>
          {#if itemAttributes.length}
            <h4 class="mt-2 text-meta font-[560] text-text-muted">Published item</h4>
            <dl class="mt-1 divide-y divide-border-subtle border-y border-border-subtle">
              {#each itemAttributes as attribute}
                <div class="grid gap-1 px-1 py-2.5 sm:grid-cols-[minmax(7rem,2fr)_minmax(0,3fr)] sm:gap-4">
                  <dt class="text-meta leading-5 text-text-muted">{attribute.label}</dt>
                  <dd class="wrap-break-word text-label leading-5 text-text-secondary">{attribute.value}</dd>
                </div>
              {/each}
            </dl>
          {/if}
          {#if relationAttributes.length}
            <h4 class="mt-3 text-meta font-[560] text-text-muted">Provider relation</h4>
            <dl class="mt-1 divide-y divide-border-subtle border-y border-border-subtle">
              {#each relationAttributes as attribute}
                <div class="grid gap-1 px-1 py-2.5 sm:grid-cols-[minmax(7rem,2fr)_minmax(0,3fr)] sm:gap-4">
                  <dt class="text-meta leading-5 text-text-muted">{attribute.label}</dt>
                  <dd class="wrap-break-word text-label leading-5 text-text-secondary">{attribute.value}</dd>
                </div>
              {/each}
            </dl>
          {/if}
        </section>
      {/if}
    </div>
  </Panel>
{/snippet}

<div class="flex min-h-0 flex-1 flex-col overflow-hidden">
  <PageToolbar label="Catalog filters">
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
          <div class="flex shrink-0 gap-1.5">
            <Button
              aria-label={`Scan ${selectedSource.source.displayName}`}
              class="w-8 px-0! sm:w-auto sm:px-3!"
              disabled={operationBusy || selectedSource.source.availability !== "available"}
              onclick={refreshCatalog}
              title={`Scan ${selectedSource.source.displayName}`}
              variant="ghost"
              ><RefreshCw
                aria-hidden="true"
                class={operationBusy ? "animate-spin" : ""}
                size={14}
              /><span class="hidden sm:inline">Scan</span></Button
            >
            <Button
              aria-label={`Reset ${selectedSource.source.displayName}`}
              class="w-8 px-0! sm:w-auto sm:px-3!"
              disabled={operationBusy}
              onclick={() => (resetOpen = true)}
              title={`Reset and refetch ${selectedSource.source.displayName}`}
              variant="danger"
              ><RotateCcw
                aria-hidden="true"
                size={14}
              /><span class="hidden sm:inline">Reset</span></Button
            >
          </div>
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
          class="w-full pl-8"
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
  </PageToolbar>

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
    <EmptyState
      class="flex-1"
      description="This workspace has no attached sources."
      icon={Database}
      title="No attached sources"
    >
      {#snippet actions()}
        <Button
          onclick={() => goto(`/connections?workspace=${data.workspace.id}`)}
          size="sm"
        >
          Attach sources
          <ArrowRight
            aria-hidden="true"
            size={14}
          />
        </Button>
      {/snippet}
    </EmptyState>
  {:else if selectedSource?.error}
    <EmptyState
      class="flex-1"
      description={selectedSource.error}
      icon={Layers}
      role="alert"
      title="Catalog unavailable"
    />
  {:else}
    <div class="min-h-0 flex-1 overflow-hidden">
      <div
        class={[
          "grid h-full min-h-0 w-full lg:grid-cols-[minmax(0,1fr)_24rem] lg:grid-rows-1 xl:grid-cols-[minmax(0,1fr)_26rem]",
          focusedEntry ? "grid-rows-[minmax(0,1fr)_minmax(14rem,0.8fr)]" : "grid-rows-1",
        ]}
      >
        <section class="min-h-0 min-w-0 overflow-y-auto p-4 sm:p-5 lg:p-6">
          <div
            class="mb-4 flex h-9 min-w-0 items-center rounded-md border border-border-subtle bg-surface-raised px-1.5 shadow-[0_6px_18px_rgb(0_0_0/12%)]"
          >
            <nav
              aria-label="Catalog hierarchy"
              class="flex min-w-0 flex-1 items-center gap-0.5 overflow-x-auto"
            >
              {#if browsePath.length > 0}
                <IconButton
                  class="mr-0.5 shrink-0"
                  label="Back one catalog level"
                  onclick={browseBack}
                  size="sm"
                  ><ArrowLeft
                    aria-hidden="true"
                    size={13}
                  /></IconButton
                ><span
                  aria-hidden="true"
                  class="mr-0.5 h-4 w-px shrink-0 bg-border-subtle"
                ></span>
              {/if}
              <button
                aria-current={browsePath.length === 0 ? "page" : undefined}
                class={[
                  "inline-flex h-7 shrink-0 items-center gap-1.5 rounded-md px-2 text-label font-[510] outline-none transition-colors focus-visible:ring-2 focus-visible:ring-accent/15",
                  browsePath.length === 0
                    ? "bg-white/5 text-text-secondary"
                    : "text-text-muted hover:bg-white/3 hover:text-text-secondary",
                ]}
                onclick={() => { browsePath = []; focusedRelationId = ""; }}
                title="Library home"
                type="button"
              >
                <Layers
                  aria-hidden="true"
                  size={13}
                />
                {browsePath.length === 0 ? "Library home" : "Root"}
              </button>
              {#each browsePath as part, index (relationId({ relation: { key: part.key } } as CatalogEntry))}
                <ChevronRight
                  aria-hidden="true"
                  class="shrink-0 text-text-disabled"
                  size={12}
                /><button
                  aria-current={index === browsePath.length - 1 ? "page" : undefined}
                  class={[
                    "h-7 max-w-44 shrink-0 truncate rounded-md px-2 text-label font-[510] outline-none transition-colors focus-visible:ring-2 focus-visible:ring-accent/15",
                    index === browsePath.length - 1
                      ? "bg-white/5 text-text-secondary"
                      : "text-text-muted hover:bg-white/3 hover:text-text-secondary",
                  ]}
                  onclick={() => setBrowseDepth(index)}
                  title={part.label}
                  type="button"
                >
                  {part.label}
                </button>
              {/each}
            </nav>
            <p class="ml-2 shrink-0 border-l border-border-subtle px-2 text-meta tabular-nums text-text-muted">
              Page {pageIndex + 1}
            </p>
            <Tabs.Root
              class="w-auto! shrink-0"
              onValueChange={changeViewMode}
              value={viewMode}
            >
              <Tabs.List
                aria-label="Catalog view"
                class="min-h-0!"
              >
                <Tabs.Trigger
                  aria-label="Grid view"
                  class="w-7 px-0!"
                  title="Grid view"
                  value="grid"
                >
                  <LayoutGrid
                    aria-hidden="true"
                    size={13}
                  />
                </Tabs.Trigger>
                <Tabs.Trigger
                  aria-label="List view"
                  class="w-7 px-0!"
                  title="List view"
                  value="list"
                >
                  <List
                    aria-hidden="true"
                    size={14}
                  />
                </Tabs.Trigger>
              </Tabs.List>
            </Tabs.Root>
          </div>

          {#if loading}
            <div
              class="flex min-h-48 items-center justify-center gap-2 text-label text-text-muted"
              role="status"
            >
              <Spinner size={16} />Loading published catalog...
            </div>
          {:else if visibleEntries.length === 0}
            <EmptyState
              description={catalogSearch ? "No entries match this search." : "This source has no published entries."}
              icon={Layers}
              title="No catalog entries"
            />
          {:else}
            {#if viewMode === "grid"}
              <ul
                aria-label="Catalog entries"
                class="grid grid-cols-[repeat(auto-fill,minmax(8.5rem,1fr))] gap-4 sm:grid-cols-[repeat(auto-fill,minmax(9.5rem,1fr))]"
              >
                {#each visibleEntries as entry (relationId(entry))}
                  {@const artwork = preferredArtwork(entry)}
                  {@const entryId = relationId(entry)}
                  <li class="group min-w-0">
                    <div class="group/poster relative">
                      <button
                        aria-label={`Inspect ${entryLabel(entry)}`}
                        aria-pressed={focusedRelationId === entryId}
                        class="block w-full rounded-md text-left outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-surface-canvas"
                        onclick={() => (focusedRelationId = entryId)}
                        title={formatProviderKey(entry.item?.key ?? entry.relation.key)}
                        type="button"
                      >
                        <MediaArtwork
                          alt=""
                          class={[
                            "w-full transition duration-200 group-hover:-translate-y-0.5 group-hover:border-border-strong group-hover:shadow-[0_14px_32px_rgb(0_0_0/38%)] motion-reduce:transform-none",
                            focusedRelationId === entryId && "border-accent/70 ring-1 ring-accent/35",
                          ]}
                          shape="poster"
                          src={artwork?.href}
                        >
                          <span class="absolute inset-x-0 bottom-0 truncate p-2.5 text-label font-[650] text-white">
                            {entryLabel(entry)}
                          </span>
                        </MediaArtwork>
                      </button>
                      {#if entry.hasChildren}
                        <div
                          class="absolute top-2 right-2 z-10 opacity-100 transition sm:opacity-0 sm:group-hover/poster:opacity-100 sm:focus-within:opacity-100"
                        >
                          <IconButton
                            class="border-border-strong bg-black/75 text-white shadow-[0_6px_18px_rgb(0_0_0/40%)] backdrop-blur-sm group-hover/poster:border-accent/45 group-hover/poster:bg-accent group-hover/poster:text-accent-contrast hover:bg-accent hover:text-accent-contrast"
                            label={`Browse ${entryLabel(entry)}`}
                            onclick={() => drillInto(entry)}
                            size="md"
                            title={`Browse ${entryLabel(entry)}`}
                            ><FolderOpen
                              aria-hidden="true"
                              size={15}
                            /></IconButton
                          >
                        </div>
                      {/if}
                    </div>
                  </li>
                {/each}
              </ul>
            {:else}
              <div class="overflow-hidden rounded-md border border-border-subtle bg-surface-raised">
                <div
                  aria-hidden="true"
                  class="hidden grid-cols-[minmax(0,1fr)_8rem_minmax(8rem,0.7fr)_2.5rem] items-center gap-4 border-b border-border-subtle bg-black/10 px-3 py-2 text-meta font-semibold uppercase text-text-disabled sm:grid"
                >
                  <span>Name</span>
                  <span>Media type</span>
                  <span>Provider key</span>
                  <span></span>
                </div>
                <ul
                  aria-label="Catalog entries"
                  class="divide-y divide-border-subtle"
                >
                  {#each visibleEntries as entry (relationId(entry))}
                    {@const entryId = relationId(entry)}
                    {@const providerKey = formatProviderKey(entry.item?.key ?? entry.relation.key)}
                    <li class="grid min-w-0 grid-cols-[minmax(0,1fr)_2.5rem] items-stretch">
                      <button
                        aria-label={`Inspect ${entryLabel(entry)}`}
                        aria-pressed={focusedRelationId === entryId}
                        class={[
                          "grid min-w-0 grid-cols-[minmax(0,1fr)_auto] items-center gap-x-3 gap-y-0.5 px-3 py-2.5 text-left outline-none transition-colors hover:bg-white/3 focus-visible:z-10 focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-accent sm:grid-cols-[minmax(0,1fr)_8rem_minmax(8rem,0.7fr)] sm:gap-4",
                          focusedRelationId === entryId && "bg-accent/8",
                        ]}
                        onclick={() => (focusedRelationId = entryId)}
                        title={providerKey}
                        type="button"
                      >
                        <span class="truncate text-label font-semibold text-text-primary">{entryLabel(entry)}</span>
                        <span class="text-meta capitalize text-text-muted sm:text-label">{entryKind(entry)}</span>
                        <span
                          class="col-span-2 truncate font-mono text-meta text-text-disabled sm:col-span-1 sm:text-text-muted"
                        >
                          {providerKey}
                        </span>
                      </button>
                      <div class="flex items-center justify-center">
                        {#if entry.hasChildren}
                          <IconButton
                            label={`Browse ${entryLabel(entry)}`}
                            onclick={() => drillInto(entry)}
                            size="sm"
                            title={`Browse ${entryLabel(entry)}`}
                            ><FolderOpen
                              aria-hidden="true"
                              size={14}
                            /></IconButton
                          >
                        {/if}
                      </div>
                    </li>
                  {/each}
                </ul>
              </div>
            {/if}
            {#if pageIndex > 0 || nextCursor}
              <nav
                aria-label="Catalog pages"
                class="mt-5 flex items-center justify-center gap-2"
              >
                <Button
                  disabled={loadingPage || pageIndex === 0}
                  onclick={showPreviousPage}
                  size="sm"
                  variant="ghost"
                >
                  <ArrowLeft
                    aria-hidden="true"
                    size={14}
                  />Previous
                </Button>
                <span class="min-w-16 text-center text-meta tabular-nums text-text-muted">Page {pageIndex + 1}</span>
                <Button
                  disabled={loadingPage || !nextCursor}
                  onclick={showNextPage}
                  size="sm"
                  variant="ghost"
                >
                  {#if loadingPage}
                    <Spinner size={14} />
                  {:else}
                    <ArrowRight
                      aria-hidden="true"
                      size={14}
                    />
                  {/if}
                  Next
                </Button>
              </nav>
            {/if}
          {/if}
        </section>

        {#if focusedEntry}
          <aside class="min-h-0 min-w-0 overflow-y-auto border-t border-border-subtle lg:border-t-0 lg:border-l">
            {@render catalogDetails(focusedEntry)}
          </aside>
        {:else}
          <aside
            class="hidden min-h-0 min-w-0 place-items-center border-l border-border-subtle p-5 text-center lg:grid"
          >
            <div class="max-w-56 text-label text-text-muted">
              <Layers
                aria-hidden="true"
                class="mx-auto text-text-disabled"
                size={24}
              />
              <p class="mt-3">Select an entry to view its published details.</p>
            </div>
          </aside>
        {/if}
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
