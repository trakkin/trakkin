<script lang="ts">
import { GitBranch, LibraryBig, Plus, Save, Trash2, X } from "@lucide/svelte";
import { Button, Dialog, IconButton, Panel, Spinner, TextInput } from "$lib";
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

let rowDrafts = $state<string[][]>([["", ""]]);
let invalidRowIndex = $state<number | null>(null);
let targetRowIndex = $state(0);
let catalogPickerOpen = $state(false);
let wasOpen = false;

$effect(() => {
  if (open && !wasOpen) {
    rowDrafts = initialMappings.length > 0 ? initialMappings.map(editableRow) : [["", ""]];
    invalidRowIndex = null;
    targetRowIndex = 0;
    catalogPickerOpen = false;
  }
  wasOpen = open;
});

function changeOpen(nextOpen: boolean): void {
  open = nextOpen;
  if (!nextOpen) onclose();
}

function addRow(): void {
  rowDrafts = [...rowDrafts, ["", ""]];
  targetRowIndex = rowDrafts.length - 1;
  invalidRowIndex = null;
}

function removeRow(index: number): void {
  if (rowDrafts.length === 1) return;
  rowDrafts = rowDrafts.filter((_, rowIndex) => rowIndex !== index);
  targetRowIndex = Math.min(targetRowIndex, rowDrafts.length - 1);
  invalidRowIndex = null;
}

function addReferenceField(rowIndex: number): void {
  rowDrafts = rowDrafts.map((row, index) => (index === rowIndex ? [...row, ""] : row));
  targetRowIndex = rowIndex;
}

function removeReference(rowIndex: number, referenceIndex: number): void {
  const references = rowDrafts[rowIndex].filter((_, index) => index !== referenceIndex);
  while (references.length < 2) references.push("");
  rowDrafts = rowDrafts.map((row, index) => (index === rowIndex ? references : row));
  clearRowValidation(rowIndex);
}

function selectCatalogRow(rowIndex: number): void {
  targetRowIndex = rowIndex;
  catalogPickerOpen = true;
}

function addCatalogReference(reference: string): void {
  const references = rowDrafts[targetRowIndex].map((value) => value.trim());
  if (references.includes(reference)) return;
  const emptyIndex = references.findIndex((value) => !value);
  const nextReferences = [...rowDrafts[targetRowIndex]];
  if (emptyIndex === -1) nextReferences.push(reference);
  else nextReferences[emptyIndex] = reference;
  rowDrafts = rowDrafts.map((row, index) => (index === targetRowIndex ? nextReferences : row));
  clearRowValidation(targetRowIndex);
}

function submit(): void {
  const mappings = rowDrafts.map((row) => row.map((reference) => reference.trim()).filter(Boolean));
  const invalidIndex = mappings.findIndex((row) => row.length < 2);
  if (invalidIndex !== -1) {
    invalidRowIndex = invalidIndex;
    targetRowIndex = invalidIndex;
    return;
  }
  invalidRowIndex = null;
  onsubmit(mappings);
}

function editableRow(references: string[]): string[] {
  const row = [...references];
  while (row.length < 2) row.push("");
  return row;
}

function clearRowValidation(rowIndex: number): void {
  if (invalidRowIndex === rowIndex) invalidRowIndex = null;
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
        <Dialog.Description class="mt-1 truncate text-label text-text-muted">
          {mode === "create" ? "Define portable reference correspondences." : `${mappingId} · ${shortHash(baseContentHash)}`}
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
        <div class="px-4 pt-4 sm:px-5">
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

      <fieldset>
        <legend class="sr-only">Portable mapping rows</legend>
        {#each rowDrafts as row, index (index)}
          <section
            class="border-b border-border-subtle px-4 py-4 sm:px-5"
            onfocusin={() => (targetRowIndex = index)}
          >
            <div class="mb-4 flex items-start gap-3">
              <div class="min-w-0 flex-1">
                <h2 class="text-label font-semibold text-text-primary">Correspondence {index + 1}</h2>
                <p class="mt-1 text-meta text-text-muted">References that identify the same item.</p>
              </div>
              <IconButton
                disabled={busy || rowDrafts.length === 1}
                label={`Remove correspondence ${index + 1}`}
                onclick={() => removeRow(index)}
                size="sm"
                variant="danger"
              >
                <Trash2
                  aria-hidden="true"
                  size={12}
                />
              </IconButton>
            </div>
            <div class="grid gap-3">
              {#each row as _, referenceIndex (`${index}:${referenceIndex}`)}
                <div class="grid min-w-0 gap-1.5">
                  <div class="flex min-h-7 items-center justify-between gap-2">
                    <label
                      class="text-label text-text-secondary"
                      for={`mapping-reference-${index}-${referenceIndex}`}
                    >
                      Portable reference {referenceIndex + 1}
                    </label>
                    <IconButton
                      disabled={busy}
                      label={`Remove reference ${referenceIndex + 1} from correspondence ${index + 1}`}
                      onclick={() => removeReference(index, referenceIndex)}
                      size="sm"
                      variant="ghost"
                    >
                      <X
                        aria-hidden="true"
                        size={12}
                      />
                    </IconButton>
                  </div>
                  <TextInput
                    aria-invalid={invalidRowIndex === index && !row[referenceIndex].trim()}
                    aria-label={`Portable reference ${referenceIndex + 1} for row ${index + 1}`}
                    class="w-full font-mono"
                    disabled={busy}
                    id={`mapping-reference-${index}-${referenceIndex}`}
                    onfocus={() => (targetRowIndex = index)}
                    oninput={() => clearRowValidation(index)}
                    placeholder="example.media://item/one"
                    bind:value={rowDrafts[index][referenceIndex]}
                  />
                </div>
              {/each}
            </div>
            {#if invalidRowIndex === index}
              <p
                class="mt-2 text-meta text-danger"
                role="alert"
              >
                Add at least two portable references to this correspondence.
              </p>
            {/if}
            <div class="mt-3 flex flex-wrap items-center gap-2">
              <Button
                disabled={busy}
                onclick={() => addReferenceField(index)}
                size="sm"
                variant="ghost"
              >
                <Plus
                  aria-hidden="true"
                  size={12}
                />Add reference
              </Button>
              <Button
                disabled={busy}
                onclick={() => selectCatalogRow(index)}
                size="sm"
                variant="ghost"
              >
                <LibraryBig
                  aria-hidden="true"
                  size={12}
                />Add from catalog
              </Button>
            </div>
          </section>
        {/each}
      </fieldset>

      <CatalogReferencePicker
        onselect={addCatalogReference}
        targetRow={targetRowIndex + 1}
        {workspaceId}
        bind:open={catalogPickerOpen}
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
          Add correspondence
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
