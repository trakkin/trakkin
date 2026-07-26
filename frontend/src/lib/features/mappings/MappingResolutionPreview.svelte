<script lang="ts">
import { AlertTriangle, Check, GitCompareArrows, Play, Route, Search, X } from "@lucide/svelte";
import { Button, IconButton, Spinner, Textarea, TextInput } from "$lib";
import {
  type MappingDetail,
  type MappingResolutionPreview,
  type MappingResolutionPreviewRequest,
  previewMappingResolution,
} from "$lib/api/mappings";

type Props = {
  workspaceId: string;
  detail: MappingDetail;
};

let { workspaceId, detail }: Props = $props();

let mode = $state<MappingResolutionPreviewRequest["mode"]>("direct");
let referenceDraft = $state("");
let windowDraft = $state("");
let busy = $state(false);
let errorMessage = $state("");
let resolution = $state<MappingResolutionPreview | null>(null);
let activeWorkspaceId = "";
let activeMappingId = "";
let activeHash = "";
let requestSequence = 0;
let controller: AbortController | null = null;

$effect(() => {
  const currentWorkspaceId = workspaceId;
  const mappingId = detail.mappingId;
  const contentHash = detail.contentHash;
  if (currentWorkspaceId === activeWorkspaceId && mappingId === activeMappingId && contentHash === activeHash) return;
  activeWorkspaceId = currentWorkspaceId;
  activeMappingId = mappingId;
  activeHash = contentHash;
  requestSequence += 1;
  controller?.abort();
  controller = null;
  mode = "direct";
  referenceDraft = initialReferences(detail).join("\n");
  windowDraft = "";
  busy = false;
  errorMessage = "";
  resolution = null;
});

$effect(() => () => controller?.abort());

function initialReferences(mapping: MappingDetail): string[] {
  const references = new Set<string>();
  for (const row of mapping.document.mappings) {
    for (const reference of row) {
      references.add(reference);
      if (references.size === 256) return [...references];
    }
  }
  return [...references];
}

function requestedReferences(): string[] {
  return [
    ...new Set(
      referenceDraft
        .split(/\r?\n/)
        .map((reference) => reference.trim())
        .filter(Boolean),
    ),
  ];
}

async function runPreview(): Promise<void> {
  const references = requestedReferences();
  if (references.length === 0 || references.length > 256) {
    errorMessage = "Select between 1 and 256 portable endpoints.";
    return;
  }
  const sequence = ++requestSequence;
  const currentWorkspaceId = workspaceId;
  const mappingId = detail.mappingId;
  const contentHash = detail.contentHash;
  controller?.abort();
  const requestController = new AbortController();
  controller = requestController;
  busy = true;
  errorMessage = "";
  resolution = null;
  try {
    const preview = await previewMappingResolution(
      currentWorkspaceId,
      mappingId,
      contentHash,
      { mode, references, window: windowDraft.trim() || undefined },
      requestController.signal,
    );
    if (ownsPreviewRequest(sequence, currentWorkspaceId, mappingId, contentHash)) resolution = preview;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (ownsPreviewRequest(sequence, currentWorkspaceId, mappingId, contentHash)) {
      errorMessage = error instanceof Error ? error.message : "Resolution preview could not be created";
    }
  } finally {
    if (ownsPreviewRequest(sequence, currentWorkspaceId, mappingId, contentHash)) {
      controller = null;
      busy = false;
    }
  }
}

function ownsPreviewRequest(
  sequence: number,
  currentWorkspaceId: string,
  mappingId: string,
  contentHash: string,
): boolean {
  return (
    sequence === requestSequence &&
    workspaceId === currentWorkspaceId &&
    detail.mappingId === mappingId &&
    detail.contentHash === contentHash
  );
}

function shortHash(value: string): string {
  const digest = value.includes(":") ? value.slice(value.indexOf(":") + 1) : value;
  return `${digest.slice(0, 12)}${digest.length > 12 ? "..." : ""}`;
}

function completenessTone(completeness: MappingResolutionPreview["completeness"]): string {
  if (completeness === "complete") return "border-success/25 bg-success/8 text-success";
  if (completeness === "partial" || completeness === "truncated") {
    return "border-warning/25 bg-warning/8 text-warning";
  }
  return "border-danger/25 bg-danger/8 text-danger";
}
</script>

<section
  aria-label="Resolution preview"
  class="border-t border-border-strong"
>
  <header
    class="flex flex-wrap items-center gap-2 border-b border-border-subtle bg-surface-raised/55 px-3 py-2 sm:px-4"
  >
    <Route
      aria-hidden="true"
      class="text-text-muted"
      size={12}
    />
    <h2 class="text-meta font-[650] uppercase text-text-muted">Resolution preview</h2>
    {#if resolution}
      <span
        class={["ml-auto inline-flex h-6 items-center rounded-md border px-2 text-meta", completenessTone(resolution.completeness)]}
      >
        {resolution.completeness}
      </span>
    {/if}
  </header>

  <div class="grid min-w-0 gap-3 px-3 py-3 sm:px-4 lg:grid-cols-[minmax(0,1fr)_11rem]">
    <label
      class="min-w-0 text-meta font-[620] text-text-muted"
      for="mapping-resolution-references"
    >
      Requested endpoints
      <Textarea
        aria-label="Requested portable endpoints"
        class="mt-1.5 min-h-24 font-mono text-meta"
        disabled={busy}
        id="mapping-resolution-references"
        rows={4}
        bind:value={referenceDraft}
      />
    </label>

    <div class="grid content-start gap-3">
      <fieldset>
        <legend class="mb-1.5 text-meta font-[620] text-text-muted">Mode</legend>
        <div class="grid h-8 grid-cols-2 rounded-control border border-border-strong bg-surface-panel p-0.5">
          <button
            aria-pressed={mode === "direct"}
            class={[
              "grid min-w-0 place-items-center rounded-sm px-2 text-meta font-[620] outline-none transition-colors focus-visible:ring-1 focus-visible:ring-accent",
              mode === "direct" ? "bg-accent text-white" : "text-text-muted hover:bg-white/4 hover:text-text-secondary",
            ]}
            disabled={busy}
            onclick={() => (mode = "direct")}
            type="button"
          >
            Direct
          </button>
          <button
            aria-pressed={mode === "transitive"}
            class={[
              "grid min-w-0 place-items-center rounded-sm px-2 text-meta font-[620] outline-none transition-colors focus-visible:ring-1 focus-visible:ring-accent",
              mode === "transitive" ? "bg-accent text-white" : "text-text-muted hover:bg-white/4 hover:text-text-secondary",
            ]}
            disabled={busy}
            onclick={() => (mode = "transitive")}
            type="button"
          >
            Transitive
          </button>
        </div>
      </fieldset>

      <label
        class="text-meta font-[620] text-text-muted"
        for="mapping-resolution-window"
      >
        Finite window
        <TextInput
          aria-label="Finite resolution window"
          class="mt-1.5 font-mono text-meta"
          controlSize="sm"
          disabled={busy}
          id="mapping-resolution-window"
          placeholder="episode:1..24"
          bind:value={windowDraft}
        />
      </label>

      <Button
        disabled={busy}
        onclick={() => void runPreview()}
        size="sm"
        variant="primary"
      >
        {#if busy}
          <Spinner size={13} />
        {:else}
          <Play
            aria-hidden="true"
            size={12}
          />
        {/if}
        Preview
      </Button>
    </div>
  </div>

  {#if errorMessage}
    <div
      class="flex items-start gap-2 border-t border-danger/25 bg-danger/6 px-3 py-2.5 text-label text-danger sm:px-4"
      role="alert"
    >
      <AlertTriangle
        aria-hidden="true"
        class="mt-0.5 shrink-0"
        size={13}
      />
      <span class="min-w-0 flex-1">{errorMessage}</span>
      <IconButton
        label="Dismiss resolution error"
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

  {#if resolution}
    <div class="border-t border-border-strong">
      <dl class="grid grid-cols-2 border-b border-border-subtle bg-surface-raised/25 sm:grid-cols-4">
        <div class="border-r border-border-subtle px-3 py-2.5">
          <dt class="text-meta text-text-disabled">Correspondences</dt>
          <dd class="mt-0.5 text-heading font-[650] tabular-nums text-text-primary">
            {resolution.correspondences.length}
          </dd>
        </div>
        <div class="border-r border-border-subtle px-3 py-2.5">
          <dt class="text-meta text-text-disabled">Findings</dt>
          <dd class="mt-0.5 text-heading font-[650] tabular-nums text-text-primary">{resolution.findings.length}</dd>
        </div>
        <div class="border-r border-border-subtle px-3 py-2.5">
          <dt class="text-meta text-text-disabled">Rows visited</dt>
          <dd class="mt-0.5 text-heading font-[650] tabular-nums text-text-primary">
            {resolution.semanticWork.candidateRows}
          </dd>
        </div>
        <div class="px-3 py-2.5">
          <dt class="text-meta text-text-disabled">Lookup calls</dt>
          <dd class="mt-0.5 text-heading font-[650] tabular-nums text-text-primary">
            {resolution.transportLookupCalls}
          </dd>
        </div>
      </dl>

      {#if resolution.correspondences.length === 0 && resolution.findings.length === 0}
        <div class="flex min-h-20 items-center justify-center gap-2 px-3 text-label text-text-muted">
          <Search
            aria-hidden="true"
            size={13}
          />
          No correspondence was produced.
        </div>
      {:else}
        <div class="grid min-w-0 lg:grid-cols-2">
          <section
            aria-label="Resolved correspondences"
            class="min-w-0 border-border-strong lg:border-r"
          >
            <h3 class="border-b border-border-subtle px-3 py-2 text-meta font-[650] uppercase text-text-muted sm:px-4">
              Correspondences
            </h3>
            <ol class="max-h-72 overflow-y-auto">
              {#each resolution.correspondences.slice(0, 50) as correspondence (correspondence.proof.key)}
                <li class="border-b border-border-subtle px-3 py-2.5 last:border-b-0 sm:px-4">
                  <div class="flex items-start gap-2">
                    <Check
                      aria-hidden="true"
                      class="mt-0.5 shrink-0 text-success"
                      size={12}
                    />
                    <div class="min-w-0 flex-1">
                      {#each correspondence.endpoints as endpoint}
                        <code class="block truncate font-mono text-meta text-text-secondary">{endpoint}</code>
                      {/each}
                      <div class="mt-1.5 flex flex-wrap gap-x-3 gap-y-1 text-meta text-text-disabled">
                        <span>{correspondence.proof.kind}</span>
                        <span
                          >{correspondence.supportCount} {correspondence.supportCount === 1 ? "proof" : "proofs"}</span
                        >
                        <span class="font-mono">{shortHash(correspondence.proof.rowHash)}</span>
                      </div>
                      {#if correspondence.segment}
                        <p class="mt-1 font-mono text-meta text-text-muted">
                          {correspondence.segment.coordinate}:{correspondence.segment.sourceStart}..{correspondence.segment.sourceEnd}
                          &rarr;
                          {correspondence.segment.targetCoordinate}:{correspondence.segment.targetStartNumerator}/{correspondence.segment.targetDenominator}..
                          {correspondence.segment.targetEndExclusiveNumerator}/{correspondence.segment.targetDenominator}
                        </p>
                      {/if}
                    </div>
                  </div>
                </li>
              {/each}
            </ol>
          </section>

          <section
            aria-label="Resolution findings"
            class="min-w-0"
          >
            <h3 class="border-b border-border-subtle px-3 py-2 text-meta font-[650] uppercase text-text-muted sm:px-4">
              Findings
            </h3>
            {#if resolution.findings.length === 0}
              <div class="flex min-h-20 items-center gap-2 px-3 text-label text-text-muted sm:px-4">
                <Check
                  aria-hidden="true"
                  class="text-success"
                  size={12}
                />
                No findings.
              </div>
            {:else}
              <ol class="max-h-72 overflow-y-auto">
                {#each resolution.findings.slice(0, 50) as finding, index (`${finding.endpoint}:${finding.kind}:${index}`)}
                  <li class="border-b border-border-subtle px-3 py-2.5 last:border-b-0 sm:px-4">
                    <div class="flex items-start gap-2">
                      <GitCompareArrows
                        aria-hidden="true"
                        class="mt-0.5 shrink-0 text-warning"
                        size={12}
                      />
                      <div class="min-w-0">
                        <code class="block truncate font-mono text-meta text-text-secondary">{finding.endpoint}</code>
                        <span class="mt-1 block text-meta text-warning">{finding.kind.replaceAll("_", " ")}</span>
                      </div>
                    </div>
                  </li>
                {/each}
              </ol>
            {/if}
          </section>
        </div>
      {/if}

      {#if resolution.nextWorkKey}
        <div class="border-t border-warning/25 bg-warning/6 px-3 py-2 font-mono text-meta text-warning sm:px-4">
          Next: {resolution.nextWorkKey}
        </div>
      {/if}
    </div>
  {/if}
</section>
