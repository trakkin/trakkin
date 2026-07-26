<script lang="ts">
import {
  Check,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  Clock3,
  FileJson2,
  History,
  Link2,
  Pause,
  Pencil,
  Play,
  RotateCcw,
} from "@lucide/svelte";
import { Button, IconButton, Panel, Spinner } from "$lib";
import type { MappingDetail, MappingRevisionSummary, MappingSummary } from "$lib/api/mappings";
import MappingResolutionPreview from "./MappingResolutionPreview.svelte";

type Props = {
  mappings: MappingSummary[];
  workspaceId: string;
  details: Record<string, MappingDetail>;
  histories: Record<string, MappingRevisionSummary[]>;
  detailLoading: Record<string, boolean>;
  historyLoading: Record<string, boolean>;
  expandedId?: string | null;
  pageNumber: number;
  canGoPrevious: boolean;
  canGoNext: boolean;
  busyId?: string | null;
  onexpand: (mapping: MappingSummary) => void;
  onedit: (mapping: MappingSummary) => void;
  onactivate: (mapping: MappingSummary, detail: MappingDetail) => void;
  onretire: (mapping: MappingSummary) => void;
  oninspectrevision: (mapping: MappingSummary, revision: MappingRevisionSummary) => void;
  onnextpage: () => void;
  onpreviouspage: () => void;
};

let {
  mappings,
  workspaceId,
  details,
  histories,
  detailLoading,
  historyLoading,
  expandedId = $bindable(null),
  pageNumber,
  canGoPrevious,
  canGoNext,
  busyId = null,
  onexpand,
  onedit,
  onactivate,
  onretire,
  oninspectrevision,
  onnextpage,
  onpreviouspage,
}: Props = $props();

function shortHash(value: string): string {
  const separator = value.indexOf(":");
  const digest = separator === -1 ? value : value.slice(separator + 1);
  return `${digest.slice(0, 12)}${digest.length > 12 ? "..." : ""}`;
}

function changedAt(value: string): string {
  const date = new Date(value);
  if (!Number.isFinite(date.valueOf())) return "Unknown";
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium" }).format(date);
}

function activationLabel(mapping: MappingSummary): string {
  const activeHash = mapping.activation?.activeContentHash;
  if (mapping.activation === undefined || mapping.activation === null) return "Not configured";
  if (!activeHash) return "Retired";
  return activeHash === mapping.headContentHash ? "Active head" : `Active ${shortHash(activeHash)}`;
}

function activationTone(mapping: MappingSummary): string {
  const activeHash = mapping.activation?.activeContentHash;
  if (!activeHash) return "border-border-subtle bg-white/3 text-text-muted";
  return activeHash === mapping.headContentHash
    ? "border-success/25 bg-success/8 text-success"
    : "border-warning/25 bg-warning/8 text-warning";
}
</script>

{#if mappings.length === 0}
  <Panel
    aria-label="Mappings"
    class="grid min-h-56 w-full flex-1 place-items-center p-6 text-center"
    element="section"
    tone="dashed"
  >
    <div>
      <Link2
        aria-hidden="true"
        class="mx-auto text-text-disabled"
        size={24}
      />
      <h2 class="mt-3 text-heading font-semibold text-text-primary">No mappings</h2>
      <p class="mt-1 text-label text-text-muted">No portable mapping records are registered.</p>
    </div>
  </Panel>
{:else}
  <section
    aria-label="Mappings"
    class="flex min-h-0 flex-1 flex-col overflow-hidden rounded-panel border border-border-strong bg-surface-panel shadow-[inset_0_1px_rgb(255_255_255/3%)]"
  >
    <div
      class="hidden h-10 shrink-0 grid-cols-[minmax(16rem,1.4fr)_7rem_6rem_minmax(10rem,1fr)_8rem_1.5rem] items-center gap-3 border-b border-border-strong bg-surface-raised px-3 text-meta font-[620] text-text-secondary shadow-[inset_0_-1px_rgb(0_0_0/20%)] lg:grid"
    >
      <span>Mapping</span>
      <span>Revision</span>
      <span>Rows</span>
      <span>Workspace</span>
      <span>Changed</span>
      <span class="sr-only">Open</span>
    </div>

    <div class="min-h-0 flex-1 overflow-y-auto">
      {#each mappings as mapping (mapping.mappingId)}
        {@const expanded = expandedId === mapping.mappingId}
        {@const detail = details[mapping.mappingId]}
        {@const revisions = histories[mapping.mappingId] ?? []}
        <article class={['border-b border-border-subtle', expanded && 'bg-surface-selected/35']}>
          <button
            aria-expanded={expanded}
            aria-label={`${expanded ? "Collapse" : "Inspect"} mapping ${mapping.mappingId}`}
            class={[
              "grid min-h-16 w-full min-w-0 grid-cols-[minmax(0,1fr)_auto_1.25rem] items-center gap-3 px-3 py-2.5 text-left outline-none transition-colors hover:bg-white/3 focus-visible:bg-white/4 lg:min-h-14 lg:py-2",
              "lg:grid-cols-[minmax(16rem,1.4fr)_7rem_6rem_minmax(10rem,1fr)_8rem_1.5rem]",
              expanded && "bg-surface-selected/55 shadow-[inset_2px_0_0_var(--accent)]",
            ]}
            onclick={() => onexpand(mapping)}
            type="button"
          >
            <span class="flex min-w-0 items-center gap-2.5">
              <span
                class="grid size-8 shrink-0 place-items-center rounded-control border border-border-subtle bg-white/3 text-text-muted"
              >
                <Link2
                  aria-hidden="true"
                  size={13}
                />
              </span>
              <span class="min-w-0">
                <span class="block truncate font-mono text-label font-[560] text-text-primary"
                  >{mapping.mappingId}</span
                >
                <span class="mt-0.5 block truncate font-mono text-meta text-text-disabled">
                  sha256:{shortHash(mapping.headContentHash)}
                </span>
                <span class="mt-1 block text-meta text-text-muted lg:hidden">
                  Revision {mapping.portableRevision} &middot; {mapping.rowCount.toLocaleString()} rows
                </span>
              </span>
            </span>

            <span class="hidden font-mono text-label tabular-nums text-text-secondary lg:block"
              >r{mapping.portableRevision}</span
            >
            <span class="hidden text-label tabular-nums text-text-secondary lg:block"
              >{mapping.rowCount.toLocaleString()}</span
            >
            <span class="justify-self-end lg:justify-self-start">
              <span
                class={["inline-flex h-6 items-center gap-1.5 rounded-md border px-2 text-meta", activationTone(mapping)]}
              >
                {#if mapping.activation?.activeContentHash}
                  <Check
                    aria-hidden="true"
                    size={10}
                  />
                {:else}
                  <Pause
                    aria-hidden="true"
                    size={10}
                  />
                {/if}
                {activationLabel(mapping)}
              </span>
            </span>
            <span class="hidden text-label text-text-muted lg:block">{changedAt(mapping.updatedAt)}</span>
            <ChevronDown
              aria-hidden="true"
              class={['shrink-0 text-text-muted transition-transform', expanded && 'rotate-180']}
              size={14}
            />
          </button>

          {#if expanded}
            <div class="border-t border-border-strong bg-surface-panel">
              {#if detailLoading[mapping.mappingId] && !detail}
                <div
                  class="flex min-h-32 items-center justify-center gap-2 text-label text-text-muted"
                  role="status"
                >
                  <Spinner size={14} />
                  Loading mapping document...
                </div>
              {:else if detail}
                <div class="grid min-w-0 xl:grid-cols-[minmax(0,1.45fr)_minmax(18rem,0.55fr)]">
                  <section
                    aria-label="Portable mapping document"
                    class="min-w-0 border-border-strong xl:border-r"
                  >
                    <header
                      class="flex min-h-10 flex-wrap items-center gap-2 border-b border-border-subtle bg-surface-raised/55 px-3 py-1.5 sm:px-4"
                    >
                      <FileJson2
                        aria-hidden="true"
                        class="text-text-muted"
                        size={12}
                      />
                      <h2 class="text-meta font-[650] uppercase text-text-muted">Portable document</h2>
                      <span class="ml-auto font-mono text-meta text-text-disabled"
                        >{shortHash(detail.contentHash)}</span
                      >
                      <Button
                        disabled={busyId === mapping.mappingId}
                        onclick={() => onedit(mapping)}
                        size="sm"
                        variant="ghost"
                      >
                        <Pencil
                          aria-hidden="true"
                          size={12}
                        />
                        New revision
                      </Button>
                    </header>
                    <pre
                      class="max-h-96 min-w-0 overflow-auto p-3 font-mono text-meta leading-5 text-text-secondary sm:p-4"
                    >{JSON.stringify(detail.document, null, 2)}</pre>
                    <MappingResolutionPreview
                      {detail}
                      {workspaceId}
                    />
                  </section>

                  <div class="min-w-0 bg-surface-raised/25">
                    <section
                      aria-label="Revision details"
                      class="border-b border-border-strong px-3 py-3 sm:px-4"
                    >
                      <dl class="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-2 text-meta">
                        <dt class="text-text-disabled">Origin</dt>
                        <dd class="truncate text-text-secondary">{detail.origin}</dd>
                        <dt class="text-text-disabled">Schema</dt>
                        <dd class="truncate font-mono text-text-secondary">{detail.document.schema}</dd>
                        <dt class="text-text-disabled">Created</dt>
                        <dd class="text-text-secondary">{changedAt(detail.revisionCreatedAt)}</dd>
                        <dt class="text-text-disabled">Parents</dt>
                        <dd class="text-text-secondary">
                          {detail.parents.length === 0
                            ? "Root revision"
                            : `${detail.parents.length} verified ${detail.parents.length === 1 ? "parent" : "parents"}`}
                        </dd>
                      </dl>
                    </section>

                    <section
                      aria-label="Workspace mapping controls"
                      class="border-b border-border-strong px-3 py-3 sm:px-4"
                    >
                      <h2 class="mb-2 text-meta font-[650] uppercase text-text-muted">Workspace</h2>
                      <div class="flex flex-wrap gap-2">
                        <Button
                          disabled={busyId === mapping.mappingId ||
                            detail.activation?.activeContentHash === detail.contentHash}
                          onclick={() => onactivate(mapping, detail)}
                          size="sm"
                          variant="primary"
                        >
                          {#if detail.contentHash === mapping.headContentHash}
                            <Play
                              aria-hidden="true"
                              size={12}
                            />
                            {detail.activation?.activeContentHash === detail.contentHash ? "Active head" : "Activate head"}
                          {:else}
                            <RotateCcw
                              aria-hidden="true"
                              size={12}
                            />
                            {detail.activation?.activeContentHash === detail.contentHash
                              ? "Active revision"
                              : "Roll back to revision"}
                          {/if}
                        </Button>
                        <Button
                          disabled={busyId === mapping.mappingId ||
                            (detail.activation !== null &&
                              detail.activation !== undefined &&
                              detail.activation.activeContentHash === null)}
                          onclick={() => onretire(mapping)}
                          size="sm"
                          variant="danger"
                        >
                          <Pause
                            aria-hidden="true"
                            size={12}
                          />
                          Retire
                        </Button>
                      </div>
                    </section>

                    <section
                      aria-label="Mapping history"
                      class="px-3 py-3 sm:px-4"
                    >
                      <div class="mb-2 flex h-7 items-center gap-2">
                        <History
                          aria-hidden="true"
                          class="text-text-muted"
                          size={12}
                        />
                        <h2 class="text-meta font-[650] uppercase text-text-muted">History</h2>
                        <span class="ml-auto text-meta tabular-nums text-text-disabled">{revisions.length}</span>
                      </div>
                      {#if historyLoading[mapping.mappingId]}
                        <div
                          class="flex items-center gap-2 py-3 text-label text-text-muted"
                          role="status"
                        >
                          <Spinner size={14} />
                          Loading history...
                        </div>
                      {:else}
                        <ol class="grid max-h-64 gap-1.5 overflow-y-auto">
                          {#each revisions as revision (revision.contentHash)}
                            <li>
                              <button
                                aria-current={detail.contentHash === revision.contentHash ? "true" : undefined}
                                aria-label={`Inspect revision ${revision.portableRevision}`}
                                class={[
                                  "flex min-h-10 w-full min-w-0 items-center gap-2.5 rounded-md border px-2.5 py-1.5 text-left outline-none hover:bg-white/3 focus-visible:border-accent",
                                  detail.contentHash === revision.contentHash
                                    ? "border-accent/40 bg-surface-selected/55"
                                    : "border-border-subtle bg-surface-panel",
                                ]}
                                onclick={() => oninspectrevision(mapping, revision)}
                                type="button"
                              >
                                <span class="font-mono text-label font-[560] text-text-secondary"
                                  >r{revision.portableRevision}</span
                                >
                                <span class="min-w-0 flex-1 truncate font-mono text-meta text-text-disabled"
                                  >{shortHash(revision.contentHash)}</span
                                >
                                <span class="flex shrink-0 items-center gap-1 text-meta text-text-disabled">
                                  <Clock3
                                    aria-hidden="true"
                                    size={9}
                                  />
                                  {changedAt(revision.createdAt)}
                                </span>
                              </button>
                            </li>
                          {/each}
                        </ol>
                      {/if}
                    </section>
                  </div>
                </div>
              {/if}
            </div>
          {/if}
        </article>
      {/each}
    </div>

    <footer
      class="flex h-11 shrink-0 items-center justify-between border-t border-border-strong bg-surface-raised px-3"
    >
      <span class="text-meta tabular-nums text-text-muted">Page {pageNumber}</span>
      <div class="flex items-center gap-1">
        <IconButton
          disabled={!canGoPrevious}
          label="Previous mappings page"
          onclick={onpreviouspage}
          size="sm"
        >
          <ChevronLeft
            aria-hidden="true"
            size={13}
          />
        </IconButton>
        <IconButton
          disabled={!canGoNext}
          label="Next mappings page"
          onclick={onnextpage}
          size="sm"
        >
          <ChevronRight
            aria-hidden="true"
            size={13}
          />
        </IconButton>
      </div>
    </footer>
  </section>
{/if}
