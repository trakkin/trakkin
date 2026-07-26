<script lang="ts">
import { GitBranch, Plus, Save, Trash2, X } from "@lucide/svelte";
import { Button, Dialog, IconButton, Panel, Spinner, Textarea } from "$lib";
import type { MappingHeadMoved } from "$lib/api/mappings";
import CatalogReferencePicker from "./CatalogReferencePicker.svelte";

type Props = {
  open?: boolean;
  mode: "create" | "update";
  mappingId?: string | null;
  baseContentHash?: string | null;
  workspaceId: string;
  initialMappings?: string[][];
  busy: boolean;
  conflict?: MappingHeadMoved | null;
  onclose: () => void;
  onsubmit: (mappings: string[][]) => void;
  onrebase: () => void;
};

let {
  open = $bindable(false),
  mode,
  mappingId = null,
  baseContentHash = null,
  workspaceId,
  initialMappings = [],
  busy,
  conflict = null,
  onclose,
  onsubmit,
  onrebase,
}: Props = $props();

let rowDrafts = $state<string[]>([""]);
let validationMessage = $state("");
let targetRowIndex = $state(0);
let wasOpen = false;

$effect(() => {
  if (open && !wasOpen) {
    rowDrafts = initialMappings.length > 0 ? initialMappings.map((row) => row.join("\n")) : [""];
    validationMessage = "";
    targetRowIndex = 0;
  }
  wasOpen = open;
});

function changeOpen(nextOpen: boolean): void {
  open = nextOpen;
  if (!nextOpen) onclose();
}

function addRow(): void {
  rowDrafts = [...rowDrafts, ""];
}

function removeRow(index: number): void {
  if (rowDrafts.length === 1) return;
  rowDrafts = rowDrafts.filter((_, rowIndex) => rowIndex !== index);
  targetRowIndex = Math.min(targetRowIndex, rowDrafts.length - 1);
}

function addCatalogReference(reference: string): void {
  const references = rowDrafts[targetRowIndex]
    .split(/\r?\n/)
    .map((value) => value.trim())
    .filter(Boolean);
  if (references.includes(reference)) return;
  rowDrafts = rowDrafts.map((draft, index) =>
    index === targetRowIndex ? [...references, reference].join("\n") : draft,
  );
}

function submit(): void {
  const mappings = rowDrafts.map((draft) =>
    draft
      .split(/\r?\n/)
      .map((reference) => reference.trim())
      .filter(Boolean),
  );
  const invalidIndex = mappings.findIndex((row) => row.length < 2);
  if (invalidIndex !== -1) {
    validationMessage = `Row ${invalidIndex + 1} needs at least two portable references.`;
    return;
  }
  validationMessage = "";
  onsubmit(mappings);
}

function shortHash(value: string | null): string {
  if (!value) return "";
  const digest = value.includes(":") ? value.slice(value.indexOf(":") + 1) : value;
  return `${digest.slice(0, 16)}${digest.length > 16 ? "..." : ""}`;
}
</script>

<Dialog.Root
  onOpenChange={changeOpen}
  bind:open
>
  <Dialog.Drawer style={open ? "translate: none" : undefined}>
    <header class="flex shrink-0 items-start justify-between gap-4 border-b border-border-strong px-4 py-4 sm:px-5">
      <div class="min-w-0">
        <Dialog.Title class="text-title font-semibold text-text-primary">
          {mode === "create" ? "New mapping" : "New revision"}
        </Dialog.Title>
        <Dialog.Description class="mt-1 truncate font-mono text-meta text-text-muted">
          {mode === "create" ? "trakkin.mapping/v1" : `${mappingId} · ${shortHash(baseContentHash)}`}
        </Dialog.Description>
      </div>
      <IconButton
        disabled={busy}
        label="Close mapping editor"
        onclick={() => changeOpen(false)}
        size="sm"
      >
        <X
          aria-hidden="true"
          size={15}
        />
      </IconButton>
    </header>

    <div class="min-h-0 flex-1 overflow-y-auto">
      {#if conflict}
        <div class="border-b border-border-strong p-4 sm:p-5">
          <Panel
            class="px-3 py-3"
            role="alert"
            tone="warning"
          >
            <div class="flex items-start gap-2.5">
              <GitBranch
                aria-hidden="true"
                class="mt-0.5 shrink-0 text-warning"
                size={14}
              />
              <div class="min-w-0 flex-1">
                <p class="text-label font-[620] text-warning">Mapping head moved</p>
                <p class="mt-1 truncate font-mono text-meta text-text-muted">{conflict.currentHead}</p>
                <p class="mt-2 text-meta text-text-secondary">
                  {conflict.added.length}
                  added · {conflict.removed.length} removed
                </p>
                <Button
                  class="mt-3"
                  disabled={busy}
                  onclick={onrebase}
                  size="sm"
                >
                  <GitBranch
                    aria-hidden="true"
                    size={13}
                  />
                  Rebase draft
                </Button>
              </div>
            </div>
          </Panel>
        </div>
      {/if}

      {#if validationMessage}
        <div class="border-b border-border-strong p-4 sm:p-5">
          <Panel
            class="px-3 py-2.5 text-label text-danger"
            role="alert"
            tone="danger"
          >
            {validationMessage}
          </Panel>
        </div>
      {/if}

      <fieldset>
        <legend class="sr-only">Portable mapping rows</legend>
        {#each rowDrafts as _, index (index)}
          <section class="border-b border-border-subtle px-4 py-4 sm:px-5">
            <div class="mb-2 flex h-7 items-center justify-between gap-3">
              <label
                class="text-meta font-[650] uppercase text-text-muted"
                for={`mapping-row-${index}`}
                >Row {index + 1}</label
              >
              <IconButton
                disabled={busy || rowDrafts.length === 1}
                label={`Remove mapping row ${index + 1}`}
                onclick={() => removeRow(index)}
                size="sm"
                variant="ghost"
              >
                <Trash2
                  aria-hidden="true"
                  size={12}
                />
              </IconButton>
            </div>
            <Textarea
              aria-label={`Portable references for row ${index + 1}`}
              disabled={busy}
              id={`mapping-row-${index}`}
              onfocus={() => (targetRowIndex = index)}
              placeholder={'example.media:item/one\nexample.alt:item/one'}
              rows={4}
              bind:value={rowDrafts[index]}
            />
          </section>
        {/each}
      </fieldset>

      <CatalogReferencePicker
        onselect={addCatalogReference}
        targetRow={targetRowIndex + 1}
        {workspaceId}
      />

      <div class="px-4 py-4 sm:px-5">
        <Button
          disabled={busy}
          onclick={addRow}
          size="sm"
        >
          <Plus
            aria-hidden="true"
            size={13}
          />
          Add row
        </Button>
      </div>
    </div>

    <footer class="flex shrink-0 items-center justify-end gap-2 border-t border-border-strong px-4 py-3 sm:px-5">
      <Button
        disabled={busy}
        onclick={() => changeOpen(false)}
        size="sm"
        variant="ghost"
        >Cancel</Button
      >
      <Button
        disabled={busy}
        onclick={submit}
        size="sm"
        variant="primary"
      >
        {#if busy}
          <Spinner size={13} />
        {:else}
          <Save
            aria-hidden="true"
            size={13}
          />
        {/if}
        {mode === "create" ? "Create mapping" : "Save revision"}
      </Button>
    </footer>
  </Dialog.Drawer>
</Dialog.Root>
