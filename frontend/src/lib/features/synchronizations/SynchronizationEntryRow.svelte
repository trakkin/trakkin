<script lang="ts">
import { ArrowRight, CircleAlert, CircleCheck, Equal } from "@lucide/svelte";
import { Chip } from "$lib";
import type { SynchronizationPreviewEntry } from "$lib/api/synchronizations";

type Props = {
  entry: SynchronizationPreviewEntry;
  sourceLabels: Map<string, string>;
};

let { entry, sourceLabels }: Props = $props();

const categoryLabels: Record<SynchronizationPreviewEntry["category"], string> = {
  proposed: "Proposed",
  no_op: "No change",
};

const categoryTones: Record<SynchronizationPreviewEntry["category"], "accent" | "neutral"> = {
  proposed: "accent",
  no_op: "neutral",
};

function sourceLabel(sourceId: string): string {
  return sourceLabels.get(sourceId) ?? `Source ${sourceId.slice(0, 8)}`;
}

function fieldLabel(): string {
  const field = entry.semanticField.split(":").at(-1) ?? entry.semanticField;
  return field
    .split("_")
    .filter(Boolean)
    .map((part) => `${part[0]?.toUpperCase() ?? ""}${part.slice(1)}`)
    .join(" ");
}

function formatValue(value: number | null | undefined): string {
  return value === null || value === undefined ? "Not set" : String(value);
}

function formatSubject(): string {
  return `${entry.receiverSubject.key.namespace}:${formatBytes(entry.receiverSubject.key.value)}`;
}

function formatBytes(bytes: number[]): string {
  const printable = bytes.length > 0 && bytes.every((byte) => byte >= 32 && byte <= 126);
  if (printable) return new TextDecoder().decode(Uint8Array.from(bytes));
  const hex = bytes.map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return hex.length > 24 ? `${hex.slice(0, 24)}...` : hex || "empty";
}
</script>

<article class="border-b border-border-subtle px-4 py-4 last:border-b-0 sm:px-5">
  <div
    class="grid min-w-0 gap-3 lg:grid-cols-[minmax(9rem,1.15fr)_minmax(7rem,0.9fr)_minmax(7rem,0.9fr)_minmax(8rem,1fr)] lg:items-center"
  >
    <div class="flex min-w-0 items-start gap-3">
      <span class="flex size-4 shrink-0 items-center justify-center text-text-disabled">
        {#if entry.category === "no_op"}
          <CircleCheck
            aria-hidden="true"
            size={14}
          />
        {:else}
          <CircleAlert
            aria-hidden="true"
            size={14}
          />
        {/if}
      </span>
      <div class="min-w-0">
        <div class="flex flex-wrap items-center gap-1.5">
          <h3 class="truncate text-body font-[580] text-text-primary">{fieldLabel()}</h3>
          <Chip tone={categoryTones[entry.category]}>{categoryLabels[entry.category]}</Chip>
          {#if entry.lossy}
            <Chip tone="warning">Lossy</Chip>
          {/if}
        </div>
        <p
          class="mt-1 truncate text-meta text-text-muted"
          title={formatSubject()}
        >
          {formatSubject()}
        </p>
      </div>
    </div>

    <div class="min-w-0">
      <p class="text-meta font-semibold uppercase text-text-disabled lg:hidden">From</p>
      <p class="mt-0.5 truncate text-label text-text-secondary lg:mt-0">
        {entry.contributorSourceIds.length
          ? entry.contributorSourceIds.map(sourceLabel).join(", ")
          : "No contributor"}
      </p>
    </div>

    <div class="grid min-w-0 grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] items-center gap-2 lg:contents">
      <div class="min-w-0">
        <p class="text-meta font-semibold uppercase text-text-disabled lg:hidden">Before</p>
        <p
          class="mt-0.5 truncate text-label text-text-muted lg:mt-0"
          title={formatValue(entry.before)}
        >
          {formatValue(entry.before)}
        </p>
      </div>
      {#if entry.category === "no_op"}
        <Equal
          aria-hidden="true"
          class="shrink-0 text-text-disabled lg:hidden"
          size={14}
        />
      {:else}
        <ArrowRight
          aria-hidden="true"
          class="shrink-0 text-text-disabled lg:hidden"
          size={14}
        />
      {/if}
      <div class="min-w-0 lg:hidden">
        <p class="text-meta font-semibold uppercase text-text-disabled">After</p>
        <p
          class="mt-0.5 truncate text-label font-[540] text-text-primary"
          title={formatValue(entry.intended)}
        >
          {formatValue(entry.intended)}
        </p>
      </div>
    </div>

    <div class="hidden min-w-0 lg:block">
      <p
        class="truncate text-label font-[540] text-text-primary"
        title={formatValue(entry.intended)}
      >
        {formatValue(entry.intended)}
      </p>
    </div>
  </div>

  <details class="group mt-2.5 pl-7 text-label text-text-muted">
    <summary
      class="w-fit cursor-pointer select-none outline-none hover:text-text-secondary focus-visible:text-text-primary"
    >
      Details
    </summary>
    <div class="mt-2 grid gap-1.5 border-l border-border-strong pl-3">
      {#if entry.finding}
        <p>{entry.finding.replaceAll("_", " ")}</p>
      {/if}
      <p>Destination: {sourceLabel(entry.receiverSourceId)}</p>
      <p>{entry.contributorSourceIds.length} contributor{entry.contributorSourceIds.length === 1 ? "" : "s"}</p>
      <p>{entry.segmentCoordinate} {entry.segmentStart}-{entry.segmentEnd}</p>
    </div>
  </details>
</article>
