<script lang="ts">
import { RefreshCw, Workflow } from "@lucide/svelte";
import { page } from "$app/state";
import { Button, Chip, PageHeader, Panel, Select, Spinner } from "$lib";
import { listSynchronizationPolicies, type SynchronizationPolicy } from "$lib/api/syncPolicies";
import {
  loadWorkspaceSourceDirectory,
  type WorkspaceSourceDirectory,
} from "$lib/features/workspaces/workspaceSourceDirectory";
import SynchronizationPolicyDrawer from "./SynchronizationPolicyDrawer.svelte";
import SynchronizationPreviewDrawer from "./SynchronizationPreviewDrawer.svelte";
import SynchronizationScheduleControls from "./SynchronizationScheduleControls.svelte";

let data = $state<WorkspaceSourceDirectory | null>(null);
let policies = $state<SynchronizationPolicy[]>([]);
let selectedPolicyId = $state("");
let previewOpen = $state(false);
let loading = $state(true);
let errorMessage = $state("");
let requestSequence = 0;
let workspaceId = $derived(page.params.workspaceId ?? "");
let policyItems = $derived(
  policies.map((policy) => ({
    label: policyLabel(policy),
    value: policy.policyId,
  })),
);
let selectedPolicy = $derived(policies.find((policy) => policy.policyId === selectedPolicyId) ?? null);

$effect(() => {
  const activeWorkspaceId = workspaceId;
  const sequence = ++requestSequence;
  const controller = new AbortController();
  data = null;
  policies = [];
  selectedPolicyId = "";
  previewOpen = false;
  void load(activeWorkspaceId, sequence, controller.signal);
  return () => controller.abort();
});

async function load(activeWorkspaceId: string, sequence: number, signal: AbortSignal) {
  loading = true;
  errorMessage = "";
  try {
    const [snapshot, loadedPolicies] = await Promise.all([
      loadWorkspaceSourceDirectory(activeWorkspaceId, signal),
      loadPolicies(activeWorkspaceId, signal),
    ]);
    if (sequence !== requestSequence) return;
    data = snapshot;
    policies = loadedPolicies;
    selectedPolicyId = loadedPolicies[0]?.policyId ?? "";
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (sequence === requestSequence) {
      errorMessage = error instanceof Error ? error.message : "Automation could not be loaded";
    }
  } finally {
    if (sequence === requestSequence) loading = false;
  }
}

function reload(): void {
  const activeWorkspaceId = workspaceId;
  const sequence = ++requestSequence;
  const controller = new AbortController();
  void load(activeWorkspaceId, sequence, controller.signal);
}

async function loadPolicies(activeWorkspaceId: string, signal: AbortSignal): Promise<SynchronizationPolicy[]> {
  const loaded: SynchronizationPolicy[] = [];
  const seenCursors = new Set<string>();
  let cursor: string | undefined;
  do {
    const result = await listSynchronizationPolicies(activeWorkspaceId, 100, cursor, signal);
    loaded.push(...result.policies);
    const nextCursor = result.nextCursor ?? undefined;
    if (nextCursor && seenCursors.has(nextCursor)) throw new Error("Policy registry returned a repeated cursor");
    if (nextCursor) seenCursors.add(nextCursor);
    cursor = nextCursor;
  } while (cursor);
  return loaded;
}

function addPolicy(policy: SynchronizationPolicy) {
  policies = [...policies, policy].sort((left, right) => left.policyId.localeCompare(right.policyId));
  selectPolicy(policy.policyId);
}

function selectPolicy(policyId: string): void {
  selectedPolicyId = policyId;
  previewOpen = false;
}

function policyLabel(policy: SynchronizationPolicy): string {
  const fields = policy.document.rules.length;
  return `Policy ${policy.policyId.slice(0, 8)} · revision ${policy.revision} · ${fields} field${fields === 1 ? "" : "s"}`;
}

function fieldLabel(semanticField: string): string {
  const labels: Record<string, string> = {
    "trakkin.state:completed": "Completed",
    "trakkin.state:progress": "Progress",
    "trakkin.state:rating": "Rating",
    "trakkin.state:rewatch-count": "Rewatch count",
    "trakkin.state:status": "Status",
    "trakkin.state:watched": "Watched",
  };
  return labels[semanticField] ?? semanticField;
}
</script>

<svelte:head><title>Trakkin &middot; {data?.workspace.name ?? "Automation"}</title></svelte:head>

<div class="flex h-full min-h-0 flex-col">
  <PageHeader title="Automation">
    {#snippet actions()}
      {#if data && data.sources.length > 0}
        <SynchronizationPolicyDrawer
          oncreated={addPolicy}
          sources={data.sources}
          {workspaceId}
        />
      {/if}
    {/snippet}
  </PageHeader>

  {#if loading && !data}
    <div
      class="flex min-h-48 flex-1 items-center justify-center gap-2 text-label text-text-muted"
      role="status"
    >
      <Spinner size={16} />
      Loading automation...
    </div>
  {:else if !data}
    <div class="flex min-h-0 flex-1 p-4 sm:p-6">
      <Panel
        class="grid min-h-48 w-full place-items-center px-4 py-6 text-center text-label text-danger"
        role="alert"
        tone="danger"
      >
        <div>
          <p>{errorMessage || "Automation is unavailable"}</p>
          <Button
            class="mt-4"
            onclick={reload}
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
    <div class="min-h-0 flex-1 overflow-y-auto p-4 sm:p-6">
      <section class="mx-auto w-full max-w-5xl">
        <div class="flex items-start justify-between gap-4">
          <div>
            <h2 class="text-title font-semibold text-text-primary">Synchronization policies</h2>
            <p class="mt-1 text-label text-text-muted">{data.workspace.name}</p>
          </div>
          <span class="text-meta text-text-disabled">
            {data.sources.length}
            attached {data.sources.length === 1 ? "source" : "sources"}
          </span>
        </div>

        {#if policies.length}
          <Panel
            aria-label="Synchronization policies"
            class="mt-5 overflow-hidden"
            element="section"
          >
            <div class="border-b border-border-strong bg-surface-raised/35 p-3 lg:hidden">
              <label
                class="block text-meta font-semibold text-text-secondary"
                for="synchronization-policy"
              >
                Selected policy
              </label>
              <Select
                ariaLabel="Stored synchronization policy"
                class="mt-2"
                items={policyItems}
                name="synchronization-policy"
                onValueChange={selectPolicy}
                triggerClass="w-full"
                value={selectedPolicyId}
              />
            </div>

            <div class="lg:grid lg:min-h-112 lg:grid-cols-[17rem_minmax(0,1fr)]">
              <nav
                aria-label="Stored synchronization policies"
                class="hidden border-r border-border-strong bg-surface-raised/20 lg:block"
              >
                <div class="flex h-10 items-center justify-between border-b border-border-strong px-3">
                  <span class="text-meta font-semibold text-text-secondary">Policies</span>
                  <span class="font-mono text-meta text-text-disabled">{policies.length}</span>
                </div>
                <div class="py-1">
                  {#each policies as policy (policy.policyId)}
                    {@const selected = policy.policyId === selectedPolicyId}
                    <button
                      aria-current={selected ? "true" : undefined}
                      aria-label={`Select ${policyLabel(policy)}`}
                      class={[
                        "block w-full border-l-2 px-3 py-2.5 text-left outline-none transition-colors hover:bg-white/3 focus-visible:bg-white/4",
                        selected
                          ? "border-accent bg-surface-selected/55"
                          : "border-transparent text-text-secondary",
                      ]}
                      onclick={() => selectPolicy(policy.policyId)}
                      type="button"
                    >
                      <span class="block truncate font-mono text-label font-semibold text-text-primary">
                        Policy {policy.policyId.slice(0, 8)}
                      </span>
                      <span class="mt-1 block text-meta text-text-muted">
                        Revision {policy.revision} &middot; {policy.document.rules.length}
                        {policy.document.rules.length === 1 ? "field" : "fields"}
                      </span>
                    </button>
                  {/each}
                </div>
              </nav>

              {#if selectedPolicy}
                <div class="min-w-0">
                  <header
                    class="flex flex-wrap items-start justify-between gap-3 border-b border-border-strong bg-surface-raised/35 p-4 sm:p-5"
                  >
                    <div class="flex min-w-0 items-start gap-3">
                      <span
                        class="grid size-9 shrink-0 place-items-center rounded-control border border-border-subtle bg-white/3 text-text-muted"
                      >
                        <Workflow
                          aria-hidden="true"
                          size={16}
                        />
                      </span>
                      <div class="min-w-0">
                        <h3 class="truncate font-mono text-heading font-semibold text-text-primary">
                          Policy {selectedPolicy.policyId.slice(0, 8)}
                        </h3>
                        <div class="mt-1 flex flex-wrap items-center gap-2 text-meta text-text-muted">
                          <span>Revision {selectedPolicy.revision}</span>
                          <span aria-hidden="true">&middot;</span>
                          <span class="capitalize">{selectedPolicy.document.resolutionMode} mappings</span>
                        </div>
                      </div>
                    </div>
                    <Button
                      onclick={() => (previewOpen = true)}
                      size="sm"
                      variant="primary"
                    >
                      Generate preview
                    </Button>
                  </header>

                  <section
                    aria-label="Policy source roles"
                    class="grid border-b border-border-subtle sm:grid-cols-2"
                  >
                    <div class="border-b border-border-subtle p-4 sm:border-r sm:border-b-0 sm:p-5">
                      <div class="flex items-center justify-between gap-3">
                        <h4 class="text-label font-semibold text-text-primary">Contributors</h4>
                        <span class="font-mono text-meta text-text-disabled">
                          {selectedPolicy.document.contributors.length}
                        </span>
                      </div>
                      <div class="mt-3 grid gap-2">
                        {#each selectedPolicy.document.contributors as sourceId (sourceId)}
                          {@const entry = data.sources.find((source) => source.source.id === sourceId)}
                          <div class="min-w-0">
                            <p
                              class="wrap-break-word text-label text-text-primary sm:truncate"
                              title={entry?.source.displayName ?? sourceId}
                            >
                              {entry?.source.displayName ?? sourceId}
                            </p>
                            {#if entry}
                              <p
                                class="truncate text-meta text-text-muted"
                                title={entry.connection.displayName}
                              >
                                {entry.connection.displayName}
                              </p>
                            {/if}
                          </div>
                        {/each}
                      </div>
                    </div>
                    <div class="p-4 sm:p-5">
                      <div class="flex items-center justify-between gap-3">
                        <h4 class="text-label font-semibold text-text-primary">Receivers</h4>
                        <span class="font-mono text-meta text-text-disabled">
                          {selectedPolicy.document.receivers.length}
                        </span>
                      </div>
                      <div class="mt-3 grid gap-2">
                        {#each selectedPolicy.document.receivers as sourceId (sourceId)}
                          {@const entry = data.sources.find((source) => source.source.id === sourceId)}
                          <div class="min-w-0">
                            <p
                              class="wrap-break-word text-label text-text-primary sm:truncate"
                              title={entry?.source.displayName ?? sourceId}
                            >
                              {entry?.source.displayName ?? sourceId}
                            </p>
                            {#if entry}
                              <p
                                class="truncate text-meta text-text-muted"
                                title={entry.connection.displayName}
                              >
                                {entry.connection.displayName}
                              </p>
                            {/if}
                          </div>
                        {/each}
                      </div>
                    </div>
                  </section>

                  <section class="border-b border-border-subtle p-4 sm:p-5">
                    <div class="flex items-center justify-between gap-3">
                      <h4 class="text-label font-semibold text-text-primary">Synchronized fields</h4>
                      <span class="font-mono text-meta text-text-disabled">
                        {selectedPolicy.document.rules.length}
                      </span>
                    </div>
                    <div class="mt-3 flex flex-wrap gap-2">
                      {#each selectedPolicy.document.rules as rule (rule.semanticField)}
                        <Chip>{fieldLabel(rule.semanticField)}</Chip>
                      {/each}
                    </div>
                  </section>

                  <div class="px-4 sm:px-5">
                    <SynchronizationScheduleControls
                      policyId={selectedPolicy.policyId}
                      {workspaceId}
                    />
                  </div>
                </div>
              {/if}
            </div>
          </Panel>
        {:else}
          <Panel
            aria-label="Synchronization policies"
            class="mt-5 grid min-h-56 place-items-center p-6 text-center"
            element="section"
            tone="dashed"
          >
            <div>
              <Workflow
                aria-hidden="true"
                class="mx-auto text-text-disabled"
                size={24}
              />
              {#if data.sources.length === 0}
                <h2 class="mt-3 text-heading font-semibold text-text-primary">No sources attached</h2>
                <p class="mt-1 max-w-sm text-label text-text-muted">
                  Attach a source in Connections before creating a synchronization policy.
                </p>
              {:else}
                <h2 class="mt-3 text-heading font-semibold text-text-primary">No synchronization policies</h2>
                <p class="mt-1 max-w-sm text-label text-text-muted">
                  Create a policy to define source roles and synchronized fields.
                </p>
              {/if}
            </div>
          </Panel>
        {/if}
      </section>
    </div>
  {/if}
</div>

{#if data && selectedPolicyId}
  <SynchronizationPreviewDrawer
    policyId={selectedPolicyId}
    sources={data.sources}
    {workspaceId}
    bind:open={previewOpen}
  />
{/if}
