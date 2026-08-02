<script lang="ts">
import { Plus, X } from "@lucide/svelte";
import { Button, Checkbox, Dialog, IconButton, Panel, Select, Spinner } from "$lib";
import {
  createSynchronizationPolicy,
  type SynchronizationPolicy,
  type SynchronizationPolicyDocument,
} from "$lib/api/syncPolicies";
import type { WorkspaceSourceDirectoryEntry } from "$lib/features/workspaces/workspaceSourceDirectory";

type Props = {
  workspaceId: string;
  sources: WorkspaceSourceDirectoryEntry[];
  oncreated: (policy: SynchronizationPolicy) => void;
};

type PolicyRule = SynchronizationPolicyDocument["rules"][number];

type RuleTemplate = {
  description: string;
  label: string;
  rule: PolicyRule;
};

const resolutionItems = [
  { label: "Direct mappings", value: "direct" },
  { label: "Transitive mappings", value: "transitive" },
];

const ruleTemplates: RuleTemplate[] = [
  ruleTemplate("Completed", "Require matching completion state", "trakkin.state:completed", "equal"),
  ruleTemplate("Watched", "Apply watched when any contributor is watched", "trakkin.state:watched", "any"),
  ruleTemplate(
    "Progress",
    "Use the longest contiguous contributed progress",
    "trakkin.state:progress",
    "coverage_contiguous_prefix",
  ),
  ruleTemplate("Rating", "Use the mean contributed rating", "trakkin.state:rating", "mean"),
  ruleTemplate("Rewatch count", "Use the highest contributed count", "trakkin.state:rewatch-count", "maximum"),
  ruleTemplate("Status", "Require contributors to agree on status", "trakkin.state:status", "equal"),
];

let { workspaceId, sources, oncreated }: Props = $props();

let open = $state(false);
let saving = $state(false);
let errorMessage = $state("");
let contributorIds = $state<Set<string>>(new Set());
let receiverIds = $state<Set<string>>(new Set());
let selectedFields = $state<Set<string>>(new Set());
let resolutionMode = $state<"direct" | "transitive">("direct");
let commandSequence = 0;

let canCreate = $derived(contributorIds.size > 0 && receiverIds.size > 0 && selectedFields.size > 0 && !saving);

function changeOpen(nextOpen: boolean) {
  commandSequence += 1;
  open = nextOpen;
  saving = false;
  if (!nextOpen) errorMessage = "";
}

function toggleSourceRole(role: "contributor" | "receiver", sourceId: string, checked: boolean) {
  const current = role === "contributor" ? contributorIds : receiverIds;
  const next = new Set(current);
  if (checked) next.add(sourceId);
  else next.delete(sourceId);
  if (role === "contributor") contributorIds = next;
  else receiverIds = next;
}

function toggleField(field: string, checked: boolean) {
  const next = new Set(selectedFields);
  if (checked) next.add(field);
  else next.delete(field);
  selectedFields = next;
}

async function createPolicy() {
  if (!canCreate) return;
  const sequence = ++commandSequence;
  const activeWorkspaceId = workspaceId;
  saving = true;
  errorMessage = "";
  try {
    const document: SynchronizationPolicyDocument = {
      schema: "trakkin.sync-policy/v1",
      contributors: sources.map((source) => source.source.id).filter((sourceId) => contributorIds.has(sourceId)),
      receivers: sources.map((source) => source.source.id).filter((sourceId) => receiverIds.has(sourceId)),
      resolutionMode,
      rules: ruleTemplates
        .filter((template) => selectedFields.has(template.rule.semanticField))
        .map((template) => template.rule),
    };
    const created = await createSynchronizationPolicy(activeWorkspaceId, document);
    if (!ownsCreation(sequence, activeWorkspaceId)) return;
    oncreated(created);
    contributorIds = new Set();
    receiverIds = new Set();
    selectedFields = new Set();
    resolutionMode = "direct";
    changeOpen(false);
  } catch (error) {
    if (ownsCreation(sequence, activeWorkspaceId)) {
      errorMessage = error instanceof Error ? error.message : "Synchronization policy could not be created";
    }
  } finally {
    if (ownsCreation(sequence, activeWorkspaceId)) saving = false;
  }
}

function ownsCreation(sequence: number, activeWorkspaceId: string): boolean {
  return sequence === commandSequence && workspaceId === activeWorkspaceId && open;
}

function ruleTemplate(
  label: string,
  description: string,
  semanticField: string,
  reducer: PolicyRule["reducer"],
): RuleTemplate {
  return {
    label,
    description,
    rule: {
      semanticField,
      reducer,
      conflict: "require_reducer",
      presence: {
        absent: "ignore",
        deleted: "ignore",
        stale: "block",
        unknown: "block",
      },
      translation: {
        allowLossy: false,
        quantizer: "exact",
      },
      writeConstraints: {
        allowClear: false,
        allowCreateMembership: true,
        requireStrongPrecondition: true,
      },
    },
  };
}
</script>

<Button
  onclick={() => changeOpen(true)}
  size="sm"
  variant="primary"
>
  <Plus
    aria-hidden="true"
    size={14}
  />
  New policy
</Button>

<Dialog.Root
  onOpenChange={changeOpen}
  bind:open
>
  <Dialog.Drawer style={open ? "translate: none" : undefined}>
    <header class="flex shrink-0 items-start justify-between gap-4 border-b border-border-strong px-4 py-4 sm:px-5">
      <div class="min-w-0">
        <Dialog.Title class="text-title font-semibold text-text-primary">Create sync policy</Dialog.Title>
        <Dialog.Description class="mt-1 text-label text-text-muted">
          Choose every contributor, receiver, and synchronized field explicitly.
        </Dialog.Description>
      </div>
      <IconButton
        label="Close policy creation"
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

      <section class="border-b border-border-subtle px-4 py-4 sm:px-5">
        <h2 class="text-label font-semibold text-text-primary">Source roles</h2>
        <p class="mt-1 text-meta text-text-muted">A source may contribute, receive, or do both.</p>
        <div class="mt-3 overflow-hidden rounded-md border border-border-subtle">
          <div
            class="hidden grid-cols-[minmax(0,1fr)_6rem_6rem] gap-2 border-b border-border-subtle bg-white/2 px-3 py-2 text-meta font-semibold uppercase text-text-disabled sm:grid"
          >
            <span>Source</span>
            <span class="text-center">Contributes</span>
            <span class="text-center">Receives</span>
          </div>
          {#each sources as source (source.source.id)}
            <div
              class="grid min-h-11 gap-3 border-b border-border-subtle px-3 py-3 last:border-b-0 sm:grid-cols-[minmax(0,1fr)_6rem_6rem] sm:items-center sm:gap-2 sm:py-2"
            >
              <div class="min-w-0">
                <p
                  class="wrap-break-word text-label font-medium text-text-primary sm:truncate"
                  title={source.source.displayName}
                >
                  {source.source.displayName}
                </p>
                <p class="truncate text-meta text-text-muted">{source.connection.displayName}</p>
              </div>
              <div class="grid grid-cols-2 gap-4 sm:contents">
                <div class="flex items-center justify-between gap-2 text-meta text-text-secondary sm:justify-center">
                  <span class="sm:sr-only">Contributes</span>
                  <Checkbox
                    aria-label={`${source.source.displayName} contributes`}
                    checked={contributorIds.has(source.source.id)}
                    onCheckedChange={(checked) => toggleSourceRole("contributor", source.source.id, Boolean(checked))}
                  />
                </div>
                <div class="flex items-center justify-between gap-2 text-meta text-text-secondary sm:justify-center">
                  <span class="sm:sr-only">Receives</span>
                  <Checkbox
                    aria-label={`${source.source.displayName} receives`}
                    checked={receiverIds.has(source.source.id)}
                    onCheckedChange={(checked) => toggleSourceRole("receiver", source.source.id, Boolean(checked))}
                  />
                </div>
              </div>
            </div>
          {:else}
            <p class="px-3 py-4 text-label text-text-muted">Attach a source before creating a policy.</p>
          {/each}
        </div>
      </section>

      <section class="border-b border-border-subtle px-4 py-4 sm:px-5">
        <h2 class="text-label font-semibold text-text-primary">Mapping scope</h2>
        <Select
          ariaLabel="Synchronization mapping scope"
          class="mt-3"
          items={resolutionItems}
          onValueChange={(value) => (resolutionMode = value as "direct" | "transitive")}
          triggerClass="w-full"
          value={resolutionMode}
        />
      </section>

      <section class="px-4 py-4 sm:px-5">
        <h2 class="text-label font-semibold text-text-primary">Synchronized fields</h2>
        <div class="mt-3 divide-y divide-border-subtle border-y border-border-subtle">
          {#each ruleTemplates as template (template.rule.semanticField)}
            <div class="flex min-h-12 items-start gap-3 py-3">
              <Checkbox
                aria-label={`Synchronize ${template.label}`}
                checked={selectedFields.has(template.rule.semanticField)}
                onCheckedChange={(checked) => toggleField(template.rule.semanticField, Boolean(checked))}
              />
              <span class="min-w-0">
                <span class="block text-label font-medium text-text-primary">{template.label}</span>
                <span class="mt-0.5 block text-meta text-text-muted">{template.description}</span>
              </span>
            </div>
          {/each}
        </div>
      </section>
    </div>

    <footer class="flex shrink-0 items-center justify-end gap-2 border-t border-border-strong px-4 py-3 sm:px-5">
      <Button
        onclick={() => changeOpen(false)}
        size="sm"
        variant="ghost"
        >Cancel</Button
      >
      <Button
        disabled={!canCreate}
        onclick={createPolicy}
        size="sm"
      >
        {#if saving}
          <Spinner size={14} />
        {/if}
        Create policy
      </Button>
    </footer>
  </Dialog.Drawer>
</Dialog.Root>
