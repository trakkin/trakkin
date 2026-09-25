<script lang="ts">
import { ArrowRight, RefreshCw, Workflow } from "@lucide/svelte";
import { goto } from "$app/navigation";
import { page } from "$app/state";
import { Button, EmptyState, PageHeader, Panel, Select, Spinner } from "$lib";
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
let pageDescription = $derived(
  data
    ? `${data.workspace.name} · ${data.sources.length} attached ${data.sources.length === 1 ? "source" : "sources"}`
    : undefined,
);
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

function reducerLabel(reducer: string): string {
  const labels: Record<string, string> = {
    any: "Any contributor",
    coverage_contiguous_prefix: "Longest contiguous progress",
    equal: "All contributors agree",
    maximum: "Highest value",
    mean: "Average",
  };
  return labels[reducer] ?? reducer.replaceAll("_", " ");
}
</script>

<svelte:head><title>Trakkin &middot; {data?.workspace.name ?? "Automation"}</title></svelte:head>

<div class="flex h-full min-h-0 flex-col">
  <PageHeader
    description={pageDescription}
    title="Automation"
  >
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
  {:else if policies.length && selectedPolicy}
    <div class="min-h-0 flex-1 overflow-y-auto p-4 sm:p-6">
      <section
        aria-label="Synchronization policies"
        class="mx-auto w-full max-w-6xl"
      >
        <Panel
          class="overflow-hidden"
          element="section"
        >
          <div class="border-b border-border-strong bg-surface-raised/20 p-3 lg:hidden">
            <label
              class="sr-only"
              for="synchronization-policy"
              >Selected policy</label
            >
            <Select
              ariaLabel="Stored synchronization policy"
              items={policyItems}
              name="synchronization-policy"
              onValueChange={selectPolicy}
              triggerClass="w-full"
              value={selectedPolicyId}
            />
          </div>

          <div class="lg:grid lg:grid-cols-[15rem_minmax(0,1fr)]">
            <nav
              aria-label="Stored synchronization policies"
              class="hidden border-r border-border-strong bg-surface-raised/20 lg:block"
            >
              <div class="flex h-10 items-center justify-between border-b border-border-strong px-3">
                <span class="text-meta font-[510] text-text-secondary">Policies</span>
                <span class="font-mono text-meta text-text-disabled">{policies.length}</span>
              </div>
              <div class="py-1">
                {#each policies as policy (policy.policyId)}
                  {@const selected = policy.policyId === selectedPolicyId}
                  <button
                    aria-current={selected ? "true" : undefined}
                    aria-label={`Select ${policyLabel(policy)}`}
                    class={[
                      "block w-full border-l px-3 py-2.5 text-left outline-none transition-colors hover:bg-white/3 focus-visible:bg-white/4 focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-accent",
                      selected
                        ? "border-accent bg-surface-selected/55"
                        : "border-transparent text-text-secondary",
                    ]}
                    onclick={() => selectPolicy(policy.policyId)}
                    type="button"
                  >
                    <span class="block truncate font-mono text-label font-[510] text-text-primary">
                      Policy {policy.policyId.slice(0, 8)}
                    </span>
                    <span class="mt-0.5 block text-meta text-text-muted">
                      Revision {policy.revision} &middot; {policy.document.rules.length}
                      {policy.document.rules.length === 1 ? "field" : "fields"}
                    </span>
                  </button>
                {/each}
              </div>
            </nav>

            <div class="min-w-0">
              <header
                class="flex flex-wrap items-start justify-between gap-3 border-b border-border-strong px-4 py-3.5 sm:px-5"
              >
                <div class="min-w-0">
                  <h2 class="truncate font-mono text-heading font-[560] text-text-primary">
                    Policy {selectedPolicy.policyId.slice(0, 8)}
                  </h2>
                  <p class="mt-0.5 text-meta text-text-muted">
                    Revision {selectedPolicy.revision} &middot;
                    <span class="capitalize">{selectedPolicy.document.resolutionMode} mappings</span>
                  </p>
                </div>
                <Button
                  onclick={() => (previewOpen = true)}
                  size="sm"
                  variant="primary"
                  >Generate preview</Button
                >
              </header>

              <section class="border-b border-border-subtle">
                <header class="border-b border-border-subtle bg-surface-raised/20 px-4 py-3 sm:px-5">
                  <h3 class="text-label font-[560] text-text-primary">Policy flow</h3>
                  <p class="mt-0.5 text-meta text-text-muted">
                    Contributions resolve through {selectedPolicy.document.rules.length}
                    {selectedPolicy.document.rules.length === 1 ? "field" : "fields"}
                    before writing to receivers.
                  </p>
                </header>

                <div
                  class="grid divide-y divide-border-subtle lg:grid-cols-[minmax(0,0.8fr)_2rem_minmax(0,1.25fr)_2rem_minmax(0,0.8fr)] lg:divide-y-0"
                >
                  <section class="min-w-0 px-4 py-4 sm:px-5">
                    <div class="flex items-center justify-between gap-3">
                      <h4 class="text-label font-[510] text-text-secondary">Contributors</h4>
                      <span class="font-mono text-meta tabular-nums text-text-disabled">
                        {selectedPolicy.document.contributors.length}
                      </span>
                    </div>
                    <div class="mt-2 divide-y divide-border-subtle border-y border-border-subtle">
                      {#each selectedPolicy.document.contributors as sourceId (sourceId)}
                        {@const entry = data.sources.find((source) => source.source.id === sourceId)}
                        <div class="min-w-0 py-2.5">
                          <p class="truncate text-label font-[450] text-text-primary">
                            {entry?.source.displayName ?? sourceId}
                          </p>
                          <p class="mt-0.5 truncate text-meta text-text-muted">
                            {entry?.connection.displayName ?? "Unavailable connection"}
                          </p>
                        </div>
                      {/each}
                    </div>
                  </section>

                  <div
                    aria-hidden="true"
                    class="hidden items-center justify-center border-x border-border-subtle bg-surface-raised/15 text-text-disabled lg:flex"
                  >
                    <ArrowRight size={14} />
                  </div>

                  <section class="min-w-0 px-4 py-4 sm:px-5">
                    <div class="flex items-center justify-between gap-3">
                      <h4 class="text-label font-[510] text-text-secondary">Synchronized fields</h4>
                      <span class="font-mono text-meta tabular-nums text-text-disabled">
                        {selectedPolicy.document.rules.length}
                      </span>
                    </div>
                    <div class="mt-2 divide-y divide-border-subtle border-y border-border-subtle">
                      {#each selectedPolicy.document.rules as rule (rule.semanticField)}
                        <article class="grid gap-1 py-2.5">
                          <div class="flex min-w-0 items-baseline justify-between gap-3">
                            <p class="text-label font-[450] text-text-primary">{fieldLabel(rule.semanticField)}</p>
                            <p class="shrink-0 text-meta text-text-muted">{reducerLabel(rule.reducer)}</p>
                          </div>
                          <p class="text-meta text-text-disabled">
                            {rule.writeConstraints.requireStrongPrecondition ? "Strong precondition" : "Standard precondition"}
                            &middot; {rule.writeConstraints.allowClear ? "Clears allowed" : "No clears"}
                          </p>
                        </article>
                      {/each}
                    </div>
                  </section>

                  <div
                    aria-hidden="true"
                    class="hidden items-center justify-center border-x border-border-subtle bg-surface-raised/15 text-text-disabled lg:flex"
                  >
                    <ArrowRight size={14} />
                  </div>

                  <section class="min-w-0 px-4 py-4 sm:px-5">
                    <div class="flex items-center justify-between gap-3">
                      <h4 class="text-label font-[510] text-text-secondary">Receivers</h4>
                      <span class="font-mono text-meta tabular-nums text-text-disabled">
                        {selectedPolicy.document.receivers.length}
                      </span>
                    </div>
                    <div class="mt-2 divide-y divide-border-subtle border-y border-border-subtle">
                      {#each selectedPolicy.document.receivers as sourceId (sourceId)}
                        {@const entry = data.sources.find((source) => source.source.id === sourceId)}
                        <div class="min-w-0 py-2.5">
                          <p class="truncate text-label font-[450] text-text-primary">
                            {entry?.source.displayName ?? sourceId}
                          </p>
                          <p class="mt-0.5 truncate text-meta text-text-muted">
                            {entry?.connection.displayName ?? "Unavailable connection"}
                          </p>
                        </div>
                      {/each}
                    </div>
                  </section>
                </div>
              </section>

              <div class="px-4 sm:px-5">
                <SynchronizationScheduleControls
                  policyId={selectedPolicy.policyId}
                  {workspaceId}
                />
              </div>
            </div>
          </div>
        </Panel>
      </section>
    </div>
  {:else}
    <div class="min-h-0 flex-1 overflow-y-auto p-4 sm:p-6">
      <section class="mx-auto w-full max-w-6xl">
        <EmptyState
          aria-label="Synchronization policies"
          description={data.sources.length === 0 ? "This workspace has no attached sources." : "No synchronization policies are configured."}
          icon={Workflow}
          title={data.sources.length === 0 ? "No sources attached" : "No synchronization policies"}
        >
          {#snippet actions()}
            {#if data?.sources.length === 0}
              <Button
                onclick={() => goto(`/connections?workspace=${workspaceId}`)}
                size="sm"
              >
                Attach sources<ArrowRight
                  aria-hidden="true"
                  size={14}
                />
              </Button>
            {/if}
          {/snippet}
        </EmptyState>
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
