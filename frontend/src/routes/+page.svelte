<script lang="ts">
import { onMount } from "svelte";
import { goto } from "$app/navigation";
import { Panel, Spinner } from "$lib";
import { listWorkspaces } from "$lib/api/workspaces";

let errorMessage = $state("");

onMount(() => {
  let active = true;
  const controller = new AbortController();

  async function loadWorkspace(): Promise<void> {
    try {
      const workspace = (await listWorkspaces(controller.signal)).find((candidate) => candidate.status === "active");
      if (!active) return;
      await goto(workspace ? `/w/${workspace.id}/catalog` : "/workspaces/new", { replaceState: true });
    } catch (error) {
      if (!active) return;
      errorMessage = error instanceof Error ? error.message : "Workspaces could not be loaded";
    }
  }

  void loadWorkspace();
  return () => {
    active = false;
    controller.abort();
  };
});
</script>

<svelte:head>
  <title>Trakkin</title>
  <meta
    content="Trakkin media synchronization workspace."
    name="description"
  >
</svelte:head>

<div class="grid h-full min-h-0 place-items-center p-5">
  {#if errorMessage}
    <Panel
      class="max-w-lg px-4 py-3 text-label text-danger"
      role="alert"
      tone="danger"
      >{errorMessage}</Panel
    >
  {:else}
    <p
      class="flex items-center gap-2 text-label text-text-muted"
      role="status"
    >
      <Spinner size={16} />
      Loading workspaces...
    </p>
  {/if}
</div>
