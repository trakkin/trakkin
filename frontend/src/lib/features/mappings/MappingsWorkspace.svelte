<script lang="ts">
import { Plus, RefreshCw, X } from "@lucide/svelte";
import { page } from "$app/state";
import { Button, IconButton, PageHeader, Panel, Spinner } from "$lib";
import {
  activateMappingRevision,
  createMappingDocument,
  createMappingRevision,
  getMappingDetail,
  getMappingRevisionDetail,
  listMappingHistory,
  listMappingRegistry,
  type MappingActivation,
  type MappingDetail,
  type MappingHeadMoved,
  MappingHeadMovedError,
  type MappingRevisionSummary,
  type MappingSummary,
  retireMapping,
} from "$lib/api/mappings";
import MappingEditorDrawer from "./MappingEditorDrawer.svelte";
import MappingRegistry from "./MappingRegistry.svelte";

const PAGE_SIZE = 25;

let mappings = $state<MappingSummary[]>([]);
let nextCursor = $state<string | null>(null);
let cursors = $state<Array<string | undefined>>([undefined]);
let pageIndex = $state(0);
let loading = $state(true);
let registryLoaded = $state(false);
let errorMessage = $state("");
let expandedId = $state<string | null>(null);
let details = $state<Record<string, MappingDetail>>({});
let headDetails = $state<Record<string, MappingDetail>>({});
let histories = $state<Record<string, MappingRevisionSummary[]>>({});
let detailLoading = $state<Record<string, boolean>>({});
let historyLoading = $state<Record<string, boolean>>({});
let busyId = $state<string | null>(null);
let editorOpen = $state(false);
let editorMode = $state<"create" | "update">("create");
let editorMappingId = $state<string | null>(null);
let editorBaseContentHash = $state<string | null>(null);
let editorInitialMappings = $state<string[][]>([]);
let editorBusy = $state(false);
let editorConflict = $state<MappingHeadMoved | null>(null);
let loadSequence = 0;
let expansionSequence = 0;
let revisionSequence = 0;
let saveSequence = 0;
let commandSequence = 0;
let workspaceId = $derived(page.params.workspaceId ?? "");

$effect(() => {
  const activeWorkspaceId = workspaceId;
  if (!activeWorkspaceId) return;
  expansionSequence += 1;
  revisionSequence += 1;
  saveSequence += 1;
  invalidateMappingCommand();
  mappings = [];
  registryLoaded = false;
  cursors = [undefined];
  pageIndex = 0;
  nextCursor = null;
  expandedId = null;
  details = {};
  headDetails = {};
  histories = {};
  detailLoading = {};
  historyLoading = {};
  editorOpen = false;
  editorBusy = false;
  editorConflict = null;
  const controller = new AbortController();
  void loadPage(activeWorkspaceId, undefined, controller.signal);
  return () => controller.abort();
});

async function loadPage(activeWorkspaceId: string, cursor?: string, signal?: AbortSignal): Promise<void> {
  const sequence = ++loadSequence;
  loading = true;
  errorMessage = "";
  try {
    const result = await listMappingRegistry(activeWorkspaceId, { cursor, limit: PAGE_SIZE }, signal);
    if (sequence !== loadSequence) return;
    mappings = result.mappings;
    nextCursor = result.nextCursor ?? null;
    registryLoaded = true;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (sequence === loadSequence) errorMessage = messageFor(error, "Mappings could not be loaded");
  } finally {
    if (sequence === loadSequence) loading = false;
  }
}

function loadCurrentPage(): Promise<void> {
  invalidateMappingCommand();
  return loadPage(workspaceId, cursors[pageIndex]);
}

function messageFor(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}

async function expand(mapping: MappingSummary): Promise<void> {
  invalidateMappingCommand();
  const sequence = ++expansionSequence;
  revisionSequence += 1;
  const activeWorkspaceId = workspaceId;
  const mappingId = mapping.mappingId;
  detailLoading = {};
  historyLoading = {};
  errorMessage = "";
  if (expandedId === mappingId) {
    expandedId = null;
    return;
  }
  expandedId = mappingId;
  if (headDetails[mappingId] && histories[mappingId]) return;
  detailLoading = { ...detailLoading, [mappingId]: !headDetails[mappingId] };
  historyLoading = { ...historyLoading, [mappingId]: !histories[mappingId] };
  try {
    const [detail, history] = await Promise.all([
      headDetails[mappingId] ? Promise.resolve(headDetails[mappingId]) : getMappingDetail(activeWorkspaceId, mappingId),
      histories[mappingId]
        ? Promise.resolve({ revisions: histories[mappingId] })
        : listMappingHistory(activeWorkspaceId, mappingId, { limit: 100 }),
    ]);
    if (!ownsExpansionRequest(sequence, activeWorkspaceId, mappingId)) return;
    headDetails = { ...headDetails, [mappingId]: detail };
    if (!details[mappingId]) details = { ...details, [mappingId]: detail };
    histories = { ...histories, [mappingId]: history.revisions };
  } catch (error) {
    if (ownsExpansionRequest(sequence, activeWorkspaceId, mappingId)) {
      errorMessage = messageFor(error, "Mapping details could not be loaded");
    }
  } finally {
    if (ownsExpansionRequest(sequence, activeWorkspaceId, mappingId)) {
      detailLoading = { ...detailLoading, [mappingId]: false };
      historyLoading = { ...historyLoading, [mappingId]: false };
    }
  }
}

function ownsExpansionRequest(sequence: number, activeWorkspaceId: string, mappingId: string): boolean {
  return sequence === expansionSequence && workspaceId === activeWorkspaceId && expandedId === mappingId;
}

function openCreateEditor(): void {
  invalidateMappingCommand();
  saveSequence += 1;
  editorBusy = false;
  editorMode = "create";
  editorMappingId = null;
  editorBaseContentHash = null;
  editorInitialMappings = [];
  editorConflict = null;
  editorOpen = true;
}

async function openUpdateEditor(mapping: MappingSummary): Promise<void> {
  const sequence = ++commandSequence;
  const activeWorkspaceId = workspaceId;
  saveSequence += 1;
  editorBusy = false;
  const mappingId = mapping.mappingId;
  busyId = mappingId;
  errorMessage = "";
  try {
    const head = headDetails[mappingId] ?? (await getMappingDetail(activeWorkspaceId, mappingId));
    if (!ownsMappingCommand(sequence, activeWorkspaceId, mappingId)) return;
    headDetails = { ...headDetails, [mappingId]: head };
    editorMode = "update";
    editorMappingId = mappingId;
    editorBaseContentHash = head.contentHash;
    editorInitialMappings = head.document.mappings.map((row) => [...row]);
    editorConflict = null;
    editorOpen = true;
  } catch (error) {
    if (ownsMappingCommand(sequence, activeWorkspaceId, mappingId)) {
      errorMessage = messageFor(error, "Mapping editor could not be opened");
    }
  } finally {
    if (ownsMappingCommand(sequence, activeWorkspaceId, mappingId)) busyId = null;
  }
}

function closeEditor(): void {
  saveSequence += 1;
  editorOpen = false;
  editorBusy = false;
  editorConflict = null;
}

async function submitEditor(mappings: string[][]): Promise<void> {
  if (editorBusy) return;
  const sequence = ++saveSequence;
  const activeWorkspaceId = workspaceId;
  const activeMode = editorMode;
  const activeMappingId = editorMappingId;
  const activeBaseContentHash = editorBaseContentHash;
  editorBusy = true;
  errorMessage = "";
  try {
    const detail =
      activeMode === "create"
        ? await createMappingDocument(activeWorkspaceId, mappings)
        : await createMappingRevision(activeWorkspaceId, activeMappingId ?? "", activeBaseContentHash ?? "", mappings);
    if (!ownsSaveRequest(sequence, activeWorkspaceId, activeMode, activeMappingId, activeBaseContentHash)) return;
    editorOpen = false;
    editorConflict = null;
    await refreshAfterWrite(detail, sequence, activeWorkspaceId, activeMode, activeMappingId, activeBaseContentHash);
  } catch (error) {
    if (!ownsSaveRequest(sequence, activeWorkspaceId, activeMode, activeMappingId, activeBaseContentHash)) return;
    if (error instanceof MappingHeadMovedError) {
      editorConflict = error.conflict;
    } else {
      errorMessage = messageFor(error, "Mapping could not be saved");
    }
  } finally {
    if (ownsSaveRequest(sequence, activeWorkspaceId, activeMode, activeMappingId, activeBaseContentHash)) {
      editorBusy = false;
    }
  }
}

function ownsSaveRequest(
  sequence: number,
  activeWorkspaceId: string,
  activeMode: "create" | "update",
  activeMappingId: string | null,
  activeBaseContentHash: string | null,
): boolean {
  return (
    sequence === saveSequence &&
    workspaceId === activeWorkspaceId &&
    editorMode === activeMode &&
    editorMappingId === activeMappingId &&
    editorBaseContentHash === activeBaseContentHash
  );
}

function rebaseEditor(): void {
  if (!editorConflict) return;
  editorBaseContentHash = editorConflict.currentHead;
  editorConflict = null;
}

async function refreshAfterWrite(
  detail: MappingDetail,
  sequence: number,
  activeWorkspaceId: string,
  activeMode: "create" | "update",
  activeMappingId: string | null,
  activeBaseContentHash: string | null,
): Promise<void> {
  cursors = [undefined];
  pageIndex = 0;
  nextCursor = null;
  await loadPage(activeWorkspaceId);
  if (!ownsSaveRequest(sequence, activeWorkspaceId, activeMode, activeMappingId, activeBaseContentHash)) return;
  expandedId = detail.mappingId;
  details = { ...details, [detail.mappingId]: detail };
  headDetails = { ...headDetails, [detail.mappingId]: detail };
  historyLoading = { ...historyLoading, [detail.mappingId]: true };
  try {
    const history = await listMappingHistory(activeWorkspaceId, detail.mappingId, { limit: 100 });
    if (!ownsSaveRequest(sequence, activeWorkspaceId, activeMode, activeMappingId, activeBaseContentHash)) return;
    histories = { ...histories, [detail.mappingId]: history.revisions };
  } finally {
    if (ownsSaveRequest(sequence, activeWorkspaceId, activeMode, activeMappingId, activeBaseContentHash)) {
      historyLoading = { ...historyLoading, [detail.mappingId]: false };
    }
  }
}

async function activate(mapping: MappingSummary, detail: MappingDetail): Promise<void> {
  const contentHash = detail.contentHash;
  await runActivationMutation(mapping, (activeWorkspaceId) =>
    activateMappingRevision(activeWorkspaceId, mapping.mappingId, contentHash),
  );
}

async function retire(mapping: MappingSummary): Promise<void> {
  await runActivationMutation(mapping, (activeWorkspaceId) => retireMapping(activeWorkspaceId, mapping.mappingId));
}

async function runActivationMutation(
  mapping: MappingSummary,
  operation: (activeWorkspaceId: string) => Promise<MappingActivation>,
): Promise<void> {
  if (busyId) return;
  const sequence = ++commandSequence;
  const activeWorkspaceId = workspaceId;
  const mappingId = mapping.mappingId;
  busyId = mappingId;
  errorMessage = "";
  try {
    const activation = await operation(activeWorkspaceId);
    if (!ownsMappingCommand(sequence, activeWorkspaceId, mappingId)) return;
    applyActivation(mappingId, activation);
  } catch (error) {
    if (ownsMappingCommand(sequence, activeWorkspaceId, mappingId)) {
      errorMessage = messageFor(error, "Workspace mapping could not be changed");
    }
  } finally {
    if (ownsMappingCommand(sequence, activeWorkspaceId, mappingId)) busyId = null;
  }
}

function invalidateMappingCommand(): void {
  commandSequence += 1;
  busyId = null;
}

function ownsMappingCommand(sequence: number, activeWorkspaceId: string, mappingId: string): boolean {
  return sequence === commandSequence && workspaceId === activeWorkspaceId && busyId === mappingId;
}

function applyActivation(mappingId: string, activation: MappingActivation): void {
  mappings = mappings.map((mapping) => (mapping.mappingId === mappingId ? { ...mapping, activation } : mapping));
  const selected = details[mappingId];
  if (selected) details = { ...details, [mappingId]: { ...selected, activation } };
  const head = headDetails[mappingId];
  if (head) headDetails = { ...headDetails, [mappingId]: { ...head, activation } };
}

async function inspectRevision(mapping: MappingSummary, revision: MappingRevisionSummary): Promise<void> {
  const sequence = ++revisionSequence;
  const activeWorkspaceId = workspaceId;
  const mappingId = mapping.mappingId;
  const contentHash = revision.contentHash;
  detailLoading = { ...detailLoading, [mappingId]: true };
  errorMessage = "";
  try {
    const detail = await getMappingRevisionDetail(activeWorkspaceId, mappingId, contentHash);
    if (!ownsRevisionRequest(sequence, activeWorkspaceId, mappingId)) return;
    details = { ...details, [mappingId]: detail };
  } catch (error) {
    if (ownsRevisionRequest(sequence, activeWorkspaceId, mappingId)) {
      errorMessage = messageFor(error, "Mapping revision could not be loaded");
    }
  } finally {
    if (ownsRevisionRequest(sequence, activeWorkspaceId, mappingId)) {
      detailLoading = { ...detailLoading, [mappingId]: false };
    }
  }
}

function ownsRevisionRequest(sequence: number, activeWorkspaceId: string, mappingId: string): boolean {
  return sequence === revisionSequence && workspaceId === activeWorkspaceId && expandedId === mappingId;
}

function nextPage(): void {
  if (!nextCursor) return;
  cursors = [...cursors.slice(0, pageIndex + 1), nextCursor];
  pageIndex += 1;
  expandedId = null;
  void loadCurrentPage();
}

function previousPage(): void {
  if (pageIndex === 0) return;
  pageIndex -= 1;
  cursors = cursors.slice(0, pageIndex + 1);
  expandedId = null;
  void loadCurrentPage();
}
</script>

<svelte:head>
  <title>Trakkin &middot; Mappings</title>
  <meta
    content="Manage correspondence between external catalog references."
    name="description"
  >
</svelte:head>

<div class="flex h-full min-h-0 flex-col text-text-primary">
  <PageHeader title="Mappings">
    {#snippet actions()}
      <Button
        disabled={loading || !registryLoaded}
        onclick={openCreateEditor}
        size="sm"
        variant="primary"
      >
        <Plus
          aria-hidden="true"
          size={13}
        />
        New mapping
      </Button>
      <IconButton
        disabled={loading || !registryLoaded}
        label="Refresh mappings"
        onclick={() => void loadCurrentPage()}
        size="sm"
        title="Refresh mappings"
      >
        <RefreshCw
          aria-hidden="true"
          size={14}
        />
      </IconButton>
    {/snippet}
  </PageHeader>

  <div class="flex min-h-0 flex-1 px-4 py-4 sm:px-6 sm:py-5">
    {#if loading && !registryLoaded}
      <div
        class="flex min-h-48 flex-1 items-center justify-center gap-2 text-label text-text-muted"
        role="status"
      >
        <Spinner size={16} />
        Loading mappings...
      </div>
    {:else if !registryLoaded}
      <Panel
        class="grid min-h-48 flex-1 place-items-center px-4 py-6 text-center text-label text-danger"
        role="alert"
        tone="danger"
      >
        <div>
          <p>{errorMessage || "Mappings could not be loaded"}</p>
          <Button
            class="mt-4"
            onclick={() => void loadCurrentPage()}
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
    {:else}
      {#key pageIndex}
        <MappingRegistry
          {busyId}
          canGoNext={Boolean(nextCursor)}
          canGoPrevious={pageIndex > 0}
          {detailLoading}
          {details}
          {histories}
          {historyLoading}
          {mappings}
          onactivate={(mapping, detail) => void activate(mapping, detail)}
          onedit={(mapping) => void openUpdateEditor(mapping)}
          onexpand={(mapping) => void expand(mapping)}
          oninspectrevision={(mapping, revision) => void inspectRevision(mapping, revision)}
          onnextpage={nextPage}
          onpreviouspage={previousPage}
          onretire={(mapping) => void retire(mapping)}
          pageNumber={pageIndex + 1}
          {workspaceId}
          bind:expandedId
        />
      {/key}
    {/if}
  </div>
</div>

<MappingEditorDrawer
  baseContentHash={editorBaseContentHash}
  busy={editorBusy}
  conflict={editorConflict}
  initialMappings={editorInitialMappings}
  mappingId={editorMappingId}
  mode={editorMode}
  onclose={closeEditor}
  onrebase={rebaseEditor}
  onsubmit={(mappings) => void submitEditor(mappings)}
  {workspaceId}
  bind:open={editorOpen}
/>

{#if errorMessage && registryLoaded}
  <div class="pointer-events-none fixed right-4 bottom-4 left-4 z-50 flex justify-center">
    <Panel
      class="pointer-events-auto flex max-w-xl items-center gap-3 px-4 py-3 shadow-[0_18px_44px_rgb(0_0_0/45%)]"
      role="alert"
      tone="danger"
    >
      <p class="min-w-0 flex-1 text-label text-danger">{errorMessage}</p>
      <IconButton
        label="Dismiss mapping error"
        onclick={() => (errorMessage = "")}
        size="sm"
        ><X
          aria-hidden="true"
          size={13}
        /></IconButton
      >
    </Panel>
  </div>
{/if}
