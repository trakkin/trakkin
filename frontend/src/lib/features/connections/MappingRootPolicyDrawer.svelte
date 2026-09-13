<script lang="ts">
import { Save, Settings2, X } from "@lucide/svelte";
import { Button, Checkbox, Dialog, IconButton, Panel, Spinner } from "$lib";
import { getMappingRootPolicy, type MappingRootPolicy, updateMappingRootPolicy } from "$lib/api/workspaces";

type Props = {
  workspaceId: string;
  sourceId: string;
  sourceName: string;
};

type MappingRootPolicyMode = "providerRecommended" | "custom";

let { workspaceId, sourceId, sourceName }: Props = $props();

let open = $state(false);
let loading = $state(false);
let saving = $state(false);
let errorMessage = $state("");
let policy = $state<MappingRootPolicy | null>(null);
let draftNamespaces = $state<string[] | null>(null);
let mode = $derived<MappingRootPolicyMode>(draftNamespaces === null ? "providerRecommended" : "custom");
let commandSequence = 0;

let canSave = $derived(Boolean(policy) && !loading && !saving);

function changeOpen(nextOpen: boolean): void {
  if (nextOpen) {
    open = true;
    void loadPolicy();
    return;
  }
  commandSequence += 1;
  open = false;
  loading = false;
  saving = false;
  errorMessage = "";
}

async function loadPolicy(): Promise<void> {
  const sequence = ++commandSequence;
  const activeWorkspaceId = workspaceId;
  const activeSourceId = sourceId;
  loading = true;
  errorMessage = "";
  try {
    const loaded = await getMappingRootPolicy(activeWorkspaceId, activeSourceId);
    if (!ownsCommand(sequence, activeWorkspaceId, activeSourceId)) return;
    applyPolicy(loaded);
  } catch (error) {
    if (ownsCommand(sequence, activeWorkspaceId, activeSourceId)) {
      errorMessage = messageFor(error, "Mapping root policy could not be loaded");
    }
  } finally {
    if (ownsCommand(sequence, activeWorkspaceId, activeSourceId)) loading = false;
  }
}

function selectMode(nextMode: MappingRootPolicyMode): void {
  draftNamespaces =
    nextMode === "providerRecommended"
      ? null
      : (policy?.availableNamespaces.map((namespace) => namespace.namespace) ?? []);
}

function toggleNamespace(namespace: string, checked: boolean): void {
  const next = new Set(draftNamespaces ?? []);
  if (checked) next.add(namespace);
  else next.delete(namespace);
  draftNamespaces = [...next];
}

async function savePolicy(): Promise<void> {
  if (!canSave) return;
  const sequence = ++commandSequence;
  const activeWorkspaceId = workspaceId;
  const activeSourceId = sourceId;
  saving = true;
  errorMessage = "";
  try {
    const namespaces = draftNamespaces === null ? null : [...draftNamespaces].sort();
    const updated = await updateMappingRootPolicy(activeWorkspaceId, activeSourceId, namespaces);
    if (!ownsCommand(sequence, activeWorkspaceId, activeSourceId)) return;
    applyPolicy(updated);
    changeOpen(false);
  } catch (error) {
    if (ownsCommand(sequence, activeWorkspaceId, activeSourceId)) {
      errorMessage = messageFor(error, "Mapping root policy could not be saved");
    }
  } finally {
    if (ownsCommand(sequence, activeWorkspaceId, activeSourceId)) saving = false;
  }
}

function applyPolicy(nextPolicy: MappingRootPolicy): void {
  policy = nextPolicy;
  draftNamespaces = nextPolicy.namespaces === null ? null : [...nextPolicy.namespaces];
}

function ownsCommand(sequence: number, activeWorkspaceId: string, activeSourceId: string): boolean {
  return sequence === commandSequence && workspaceId === activeWorkspaceId && sourceId === activeSourceId && open;
}

function messageFor(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}
</script>

<IconButton
  label={`Configure mapping roots for ${sourceName}`}
  onclick={() => changeOpen(true)}
  size="sm"
>
  <Settings2
    aria-hidden="true"
    size={13}
  />
</IconButton>

<Dialog.Root
  onOpenChange={changeOpen}
  bind:open
>
  <Dialog.Drawer style={open ? "translate: none" : undefined}>
    <header class="flex shrink-0 items-start justify-between gap-4 border-b border-border-strong px-4 py-4 sm:px-5">
      <div class="min-w-0">
        <Dialog.Title class="text-title font-semibold text-text-primary">Mapping roots</Dialog.Title>
        <Dialog.Description class="mt-1 truncate text-label text-text-muted">{sourceName}</Dialog.Description>
      </div>
      <IconButton
        label="Close mapping root settings"
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
      {#if errorMessage}
        <div class="px-4 pt-4 sm:px-5">
          <Panel
            class="px-4 py-3 text-label text-danger"
            role="alert"
            tone="danger"
          >
            {errorMessage}
          </Panel>
        </div>
      {/if}

      {#if loading}
        <div
          class="flex min-h-40 items-center justify-center gap-2 text-label text-text-muted"
          role="status"
        >
          <Spinner size={15} />Loading mapping roots...
        </div>
      {:else if policy}
        <section class="border-b border-border-subtle px-4 py-4 sm:px-5">
          <fieldset>
            <legend class="mb-2 text-meta font-[620] uppercase text-text-muted">Selection mode</legend>
            <div class="grid h-9 grid-cols-2 rounded-control border border-border-strong bg-surface-panel p-0.5">
              <button
                aria-pressed={mode === "providerRecommended"}
                class={[
                  "grid min-w-0 place-items-center rounded-sm px-2 text-label font-[620] outline-none transition-colors focus-visible:ring-1 focus-visible:ring-accent",
                  mode === "providerRecommended"
                    ? "bg-accent text-white"
                    : "text-text-muted hover:bg-white/4 hover:text-text-secondary",
                ]}
                disabled={saving}
                onclick={() => selectMode("providerRecommended")}
                type="button"
              >
                Provider defaults
              </button>
              <button
                aria-pressed={mode === "custom"}
                class={[
                  "grid min-w-0 place-items-center rounded-sm px-2 text-label font-[620] outline-none transition-colors focus-visible:ring-1 focus-visible:ring-accent",
                  mode === "custom"
                    ? "bg-accent text-white"
                    : "text-text-muted hover:bg-white/4 hover:text-text-secondary",
                ]}
                disabled={saving}
                onclick={() => selectMode("custom")}
                type="button"
              >
                Custom namespaces
              </button>
            </div>
          </fieldset>
        </section>

        <section class="px-4 py-4 sm:px-5">
          <div class="flex items-end justify-between gap-3">
            <h2 class="text-label font-semibold text-text-primary">Observed namespaces</h2>
            {#if mode === "custom" && draftNamespaces?.length === 0}
              <span class="text-meta font-[620] text-warning">Automatic mapping disabled</span>
            {/if}
          </div>

          <div class="mt-3 overflow-hidden rounded-md border border-border-subtle">
            {#each policy.availableNamespaces as namespace, index (namespace.namespace)}
              <label
                class="grid min-h-12 grid-cols-[auto_minmax(0,1fr)_auto] items-center gap-3 border-b border-border-subtle px-3 py-2 last:border-b-0"
                for={`mapping-root-namespace-${index}`}
              >
                <Checkbox
                  aria-label={`Use ${namespace.namespace} as an automatic mapping root`}
                  checked={draftNamespaces?.includes(namespace.namespace) ?? false}
                  disabled={mode !== "custom" || saving}
                  id={`mapping-root-namespace-${index}`}
                  onCheckedChange={(checked) => toggleNamespace(namespace.namespace, Boolean(checked))}
                />
                <span class="min-w-0 truncate font-mono text-label text-text-secondary">{namespace.namespace}</span>
                <span class="text-right text-meta tabular-nums text-text-muted"> {namespace.candidateCount} seen </span>
              </label>
            {:else}
              <p class="px-3 py-4 text-label text-text-muted">No portable reference namespaces observed.</p>
            {/each}
          </div>
        </section>
      {/if}
    </div>

    <footer class="flex shrink-0 items-center justify-end gap-2 border-t border-border-strong px-4 py-3 sm:px-5">
      <Button
        disabled={saving}
        onclick={() => changeOpen(false)}
        size="sm"
        variant="ghost"
        >Cancel</Button
      >
      <Button
        disabled={!canSave}
        onclick={savePolicy}
        size="sm"
        variant="primary"
      >
        {#if saving}
          <Spinner size={13} />Saving...
        {:else}
          <Save
            aria-hidden="true"
            size={13}
          />Save
        {/if}
      </Button>
    </footer>
  </Dialog.Drawer>
</Dialog.Root>
