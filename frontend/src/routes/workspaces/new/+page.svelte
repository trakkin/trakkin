<script lang="ts">
import { ArrowLeft, FolderPlus, Plus, X } from "@lucide/svelte";
import { onDestroy, onMount } from "svelte";
import { goto } from "$app/navigation";
import { Button, IconButton, PageHeader, Panel, Spinner, TextInput } from "$lib";
import { ApiRequestError } from "$lib/api/client";
import { createWorkspace, listWorkspaces, type Workspace } from "$lib/api/workspaces";

let name = $state("");
let submitting = $state(false);
let errorMessage = $state("");
let returnWorkspace = $state<Workspace | null>(null);
let submitSequence = 0;

onMount(async () => {
  try {
    returnWorkspace = (await listWorkspaces())[0] ?? null;
  } catch {}
});

onDestroy(() => {
  submitSequence += 1;
});

async function submit(event: SubmitEvent) {
  event.preventDefault();
  const displayName = name.trim();
  if (!displayName || submitting) return;
  const sequence = ++submitSequence;
  submitting = true;
  errorMessage = "";
  try {
    const workspace = await createWorkspace(displayName);
    if (sequence !== submitSequence) return;
    window.dispatchEvent(new Event("trakkin:workspaces-changed"));
    await goto(`/w/${workspace.id}/catalog`);
  } catch (error) {
    if (sequence === submitSequence) {
      errorMessage = error instanceof ApiRequestError ? error.message : "The workspace could not be created";
    }
  } finally {
    if (sequence === submitSequence) submitting = false;
  }
}
</script>

<svelte:head><title>Trakkin &middot; Create workspace</title></svelte:head>

<div class="flex h-full min-h-0 flex-col">
  <PageHeader title="Create workspace">
    {#snippet actions()}
      {#if returnWorkspace}
        <Button
          onclick={() => goto(`/w/${returnWorkspace?.id}/catalog`)}
          size="sm"
          variant="ghost"
        >
          <ArrowLeft
            aria-hidden="true"
            size={14}
          />
          Back to Catalog
        </Button>
      {/if}
    {/snippet}
  </PageHeader>

  <div class="min-h-0 flex-1 overflow-auto">
    <div class="mx-auto grid w-full max-w-3xl gap-4 p-4 sm:p-6">
      {#if errorMessage}
        <Panel
          class="flex items-start justify-between gap-3 px-4 py-3"
          role="alert"
          tone="danger"
        >
          <p class="text-label text-danger">{errorMessage}</p>
          <IconButton
            label="Dismiss error"
            onclick={() => (errorMessage = "")}
            size="sm"
          >
            <X
              aria-hidden="true"
              size={14}
            />
          </IconButton>
        </Panel>
      {/if}

      <Panel
        class="overflow-hidden"
        element="section"
        tone="accent"
      >
        <header class="flex items-center gap-3 border-b border-border-subtle px-4 py-3.5 sm:px-5">
          <span
            class="grid size-9 shrink-0 place-items-center rounded-md border border-accent/25 bg-accent/8 text-accent"
          >
            <FolderPlus
              aria-hidden="true"
              size={17}
            />
          </span>
          <div>
            <h2 class="text-heading font-semibold text-text-primary">Workspace details</h2>
            <p class="mt-0.5 text-meta text-text-muted">Start with a name. Sources are attached from Connections.</p>
          </div>
        </header>
        <form
          class="grid gap-4 px-4 py-5 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-end sm:px-5"
          onsubmit={submit}
        >
          <label
            class="grid gap-1.5 text-label text-text-secondary"
            for="workspace-name"
          >
            Workspace name
            <TextInput
              aria-label="Workspace name"
              autocomplete="off"
              class="w-full"
              id="workspace-name"
              maxlength={80}
              placeholder="Household media"
              required
              bind:value={name}
            />
          </label>
          <Button
            disabled={!name.trim() || submitting}
            type="submit"
            variant="primary"
          >
            {#if submitting}
              <Spinner size={15} />
            {:else}
              <Plus
                aria-hidden="true"
                size={15}
              />
            {/if}
            {submitting ? "Creating..." : "Create workspace"}
          </Button>
        </form>
      </Panel>
    </div>
  </div>
</div>
