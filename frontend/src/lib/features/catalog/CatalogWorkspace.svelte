<script lang="ts">
import { RefreshCw } from "@lucide/svelte";
import { page } from "$app/state";
import { Button, IconButton, PageHeader, Panel, Spinner } from "$lib";
import CatalogBrowser from "./CatalogBrowser.svelte";
import { loadWorkspaceCatalog, type WorkspaceCatalog } from "./workspaceCatalog";

let data = $state<WorkspaceCatalog | null>(null);
let loading = $state(true);
let reloading = $state(false);
let errorMessage = $state("");
let snapshotRequest = 0;
let workspaceId = $derived(page.params.workspaceId ?? "");
$effect(() => {
  const activeWorkspaceId = workspaceId;
  const requestId = ++snapshotRequest;
  const controller = new AbortController();
  data = null;
  reloading = false;
  void load(activeWorkspaceId, requestId, controller.signal);
  return () => controller.abort();
});

async function load(activeWorkspaceId: string, requestId: number, signal?: AbortSignal) {
  loading = true;
  errorMessage = "";
  try {
    const snapshot = await loadWorkspaceCatalog(activeWorkspaceId, signal);
    if (requestId === snapshotRequest) data = snapshot;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (requestId === snapshotRequest) {
      errorMessage = error instanceof Error ? error.message : "Catalog could not be loaded";
    }
  } finally {
    if (requestId === snapshotRequest) loading = false;
  }
}

function retryLoad(): void {
  const activeWorkspaceId = workspaceId;
  const requestId = ++snapshotRequest;
  const controller = new AbortController();
  void load(activeWorkspaceId, requestId, controller.signal);
}

async function reload() {
  if (reloading) return;
  const activeWorkspaceId = workspaceId;
  const requestId = ++snapshotRequest;
  reloading = true;
  errorMessage = "";
  try {
    const snapshot = await loadWorkspaceCatalog(activeWorkspaceId);
    if (requestId === snapshotRequest) data = snapshot;
  } catch (error) {
    if (requestId !== snapshotRequest) return;
    errorMessage = error instanceof Error ? error.message : "Catalog could not be reloaded";
    throw error;
  } finally {
    if (requestId === snapshotRequest) reloading = false;
  }
}
</script>

<svelte:head><title>Trakkin &middot; {data?.workspace.name ?? "Catalog"}</title></svelte:head>

<div class="flex h-full min-h-0 flex-col">
  <PageHeader title="Catalog">
    {#snippet actions()}
      <IconButton
        disabled={loading || reloading || !data}
        label="Refresh Catalog"
        onclick={reload}
        size="sm"
        title="Refresh Catalog"
      >
        <span class={["inline-flex", reloading && "animate-spin"]}>
          <RefreshCw
            aria-hidden="true"
            size={14}
          />
        </span>
      </IconButton>
    {/snippet}
  </PageHeader>

  {#if loading && !data}
    <div
      class="flex min-h-48 flex-1 items-center justify-center gap-2 text-label text-text-muted"
      role="status"
    >
      <Spinner size={16} />
      Loading catalog...
    </div>
  {:else if !data}
    <div class="flex min-h-0 flex-1 p-4 sm:p-6">
      <Panel
        class="grid min-h-48 w-full place-items-center px-4 py-6 text-center text-label text-danger"
        role="alert"
        tone="danger"
      >
        <div>
          <p>{errorMessage || "Catalog data is unavailable"}</p>
          <Button
            class="mt-4"
            onclick={retryLoad}
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
    </div>
  {:else}
    <CatalogBrowser
      {data}
      onreload={reload}
    />
  {/if}

  {#if data && errorMessage}
    <div class="pointer-events-none fixed right-4 bottom-4 left-4 z-50 flex justify-center">
      <Panel
        class="pointer-events-auto max-w-xl px-4 py-3 text-label text-danger shadow-[0_18px_44px_rgb(0_0_0/45%)]"
        role="alert"
        tone="danger"
      >
        {errorMessage}
      </Panel>
    </div>
  {/if}
</div>
