<script lang="ts">
import {
  Check,
  Download,
  PackageOpen,
  Plus,
  RefreshCw,
  Search,
  ShieldCheck,
  Trash2,
  TriangleAlert,
  X,
} from "@lucide/svelte";
import { onDestroy, onMount } from "svelte";
import {
  AlertDialog,
  Button,
  Checkbox,
  Chip,
  Dialog,
  IconButton,
  PageHeader,
  Panel,
  Select,
  Spinner,
  TextInput,
} from "$lib";
import { ApiRequestError } from "$lib/api/client";
import {
  changeProviderUpdatePolicy,
  inspectProviderRelease,
  installProvider,
  listProviderSources,
  listProviders,
  type Provider,
  type ProviderReleaseInspection,
  type ProviderSource,
  type ProviderUpdatePolicy,
  refreshProviderSource,
  removeProvider,
  removeProviderSource,
  trustProviderSource,
  updateProvider,
} from "$lib/api/providers";
import ProviderReleaseReview from "./ProviderReleaseReview.svelte";

type RemovalTarget = { kind: "provider"; provider: Provider } | { kind: "source"; source: ProviderSource };
type SourceProviderEntry = { providerId: string; versions: string[]; installed?: Provider };

const policyItems = [
  { value: "manual", label: "Manual updates" },
  { value: "automatic", label: "Automatic updates" },
];

let sources = $state<ProviderSource[]>([]);
let providers = $state<Provider[]>([]);
let pending = $state<Record<string, boolean>>({});
let loading = $state(true);
let errorMessage = $state("");
let showSourceForm = $state(false);
let repositoryUrl = $state("");
let trustConfirmed = $state(false);
let sourceConfirmation = $state<ProviderSource>();
let installSourceId = $state("");
let installProviderId = $state("");
let providerVersion = $state("");
let installPolicy = $state<ProviderUpdatePolicy>("manual");
let releaseReviewOpen = $state(false);
let releaseInspection = $state<ProviderReleaseInspection>();
let inspectionLoading = $state(false);
let inspectionErrorMessage = $state("");
let removeOpen = $state(false);
let pendingRemoval = $state<RemovalTarget>();
let removalErrorMessage = $state("");
let loadSequence = 0;
let inspectionSequence = 0;
let inspectionController: AbortController | undefined;
const lifecycleController = new AbortController();

let versionItems = $derived(
  sources
    .find((source) => source.id === installSourceId)
    ?.availableProviders.find((available) => available.providerId === installProviderId)
    ?.versions.map((version) => ({ value: version, label: version })) ?? [],
);
let selectedInstalledProvider = $derived(
  providers.find((provider) => provider.sourceId === installSourceId && provider.providerId === installProviderId),
);

onMount(() => {
  void loadProviders(lifecycleController.signal);
});

onDestroy(() => {
  loadSequence += 1;
  inspectionSequence += 1;
  inspectionController?.abort();
  lifecycleController.abort();
});

async function loadProviders(signal?: AbortSignal) {
  const sequence = ++loadSequence;
  loading = true;
  errorMessage = "";
  try {
    const [nextSources, nextProviders] = await Promise.all([listProviderSources(signal), listProviders(signal)]);
    if (sequence !== loadSequence) return;
    sources = nextSources;
    providers = nextProviders;
    synchronizeInstallSelection();
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (sequence === loadSequence) errorMessage = messageFor(error, "Providers could not be loaded");
  } finally {
    if (sequence === loadSequence) loading = false;
  }
}

function toggleSourceForm() {
  showSourceForm = !showSourceForm;
  if (showSourceForm) cancelProviderInstall();
}

async function addSource() {
  if (!trustConfirmed) {
    errorMessage = "Confirm that you trust this source to publish executable provider code";
    return;
  }
  await runMutation("source:add", async (signal) => {
    sourceConfirmation = await trustProviderSource(
      {
        repositoryUrl: repositoryUrl.trim(),
      },
      signal,
    );
    repositoryUrl = "";
    trustConfirmed = false;
    showSourceForm = false;
  });
}

async function installSelectedProvider() {
  const inspection = releaseInspection;
  if (!installSourceId || !installProviderId || !providerVersion || !inspection) return;
  if (inspection.compatibility !== "compatible") return;
  const installed = providers.find((provider) => provider.providerId === installProviderId);
  if (installed?.version === providerVersion && installed.indexDigest === inspection.indexDigest) return;
  const key = installed ? `provider:${installed.providerId}` : "provider:install";
  await runMutation(key, async (signal) => {
    if (installed) {
      await updateProvider(
        installed.providerId,
        { version: providerVersion, indexDigest: inspection.indexDigest },
        signal,
      );
    } else {
      await installProvider(
        {
          sourceId: installSourceId,
          providerId: installProviderId,
          version: providerVersion,
          indexDigest: inspection.indexDigest,
          updatePolicy: installPolicy,
        },
        signal,
      );
    }
    cancelProviderInstall();
  });
}

async function refreshSource(source: ProviderSource) {
  await runMutation(`source:${source.id}`, (signal) => refreshProviderSource(source.id, signal));
}

function requestProviderUpdate(provider: Provider) {
  const update = provider.availableUpdate;
  if (!update) return;
  requestProviderRelease(provider.sourceId, provider.providerId, update.version);
}

async function changePolicy(provider: Provider, value: string) {
  const updatePolicy: ProviderUpdatePolicy = value === "automatic" ? "automatic" : "manual";
  if (updatePolicy === provider.updatePolicy) return;
  await runMutation(`provider:${provider.providerId}`, (signal) =>
    changeProviderUpdatePolicy(provider.providerId, { updatePolicy }, signal),
  );
}

function requestProviderRemoval(provider: Provider) {
  removalErrorMessage = "";
  pendingRemoval = { kind: "provider", provider };
  removeOpen = true;
}

function requestSourceRemoval(source: ProviderSource) {
  removalErrorMessage = "";
  pendingRemoval = { kind: "source", source };
  removeOpen = true;
}

async function confirmRemoval() {
  const target = pendingRemoval;
  if (!target) return;
  const key = removalKey(target);
  const succeeded = await runMutation(
    key,
    (signal) =>
      target.kind === "provider"
        ? removeProvider(target.provider.providerId, signal)
        : removeProviderSource(target.source.id, signal),
    (message) => (removalErrorMessage = message),
  );
  if (succeeded) {
    removeOpen = false;
    pendingRemoval = undefined;
  }
}

function changeRemoveOpen(open: boolean) {
  removeOpen = open;
  if (!open && !removalPending()) {
    pendingRemoval = undefined;
    removalErrorMessage = "";
  }
}

function removalKey(target: RemovalTarget): string {
  return target.kind === "provider" ? `provider:${target.provider.providerId}` : `source:${target.source.id}:remove`;
}

function removalPending(): boolean {
  return Boolean(pendingRemoval && pending[removalKey(pendingRemoval)]);
}

async function runMutation(
  key: string,
  mutation: (signal: AbortSignal) => Promise<unknown>,
  onError?: (message: string) => void,
): Promise<boolean> {
  const signal = lifecycleController.signal;
  if (signal.aborted || pending[key]) return false;
  pending = { ...pending, [key]: true };
  errorMessage = "";
  try {
    await mutation(signal);
    await loadProviders(signal);
    window.dispatchEvent(new CustomEvent("trakkin:providers-changed"));
    return true;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return false;
    const message = messageFor(error, "Provider operation failed");
    await loadProviders(signal);
    if (signal.aborted) return false;
    if (onError) onError(message);
    else errorMessage = message;
    return false;
  } finally {
    if (!signal.aborted) pending = { ...pending, [key]: false };
  }
}

function requestProviderInstall(sourceId: string, providerId: string) {
  requestProviderRelease(sourceId, providerId);
}

function requestProviderRelease(sourceId: string, providerId: string, version = "") {
  installSourceId = sourceId;
  installProviderId = providerId;
  providerVersion = version;
  releaseReviewOpen = true;
  showSourceForm = false;
  synchronizeInstallSelection();
  void inspectSelectedRelease();
}

function cancelProviderInstall() {
  inspectionSequence += 1;
  inspectionController?.abort();
  inspectionController = undefined;
  installSourceId = "";
  installProviderId = "";
  providerVersion = "";
  releaseReviewOpen = false;
  releaseInspection = undefined;
  inspectionLoading = false;
  inspectionErrorMessage = "";
}

function changeReleaseReviewOpen(open: boolean) {
  if (open) releaseReviewOpen = true;
  else cancelProviderInstall();
}

function synchronizeInstallSelection() {
  if (!installSourceId || !installProviderId) return;
  const source = sources.find((candidate) => candidate.id === installSourceId);
  const availableProvider = source?.availableProviders.find((available) => available.providerId === installProviderId);
  if (!source || !availableProvider) {
    cancelProviderInstall();
    return;
  }
  const versions = availableProvider.versions;
  if (!versions.includes(providerVersion)) providerVersion = versions[0] ?? "";
}

async function inspectSelectedRelease() {
  const sourceId = installSourceId;
  const providerId = installProviderId;
  const version = providerVersion;
  if (!sourceId || !providerId || !version) {
    releaseInspection = undefined;
    return;
  }
  const sequence = ++inspectionSequence;
  inspectionController?.abort();
  const controller = new AbortController();
  inspectionController = controller;
  releaseInspection = undefined;
  inspectionErrorMessage = "";
  inspectionLoading = true;
  try {
    const inspection = await inspectProviderRelease(sourceId, providerId, version, controller.signal);
    if (sequence === inspectionSequence) releaseInspection = inspection;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (sequence === inspectionSequence) {
      inspectionErrorMessage = messageFor(error, "Provider release could not be inspected");
    }
  } finally {
    if (sequence === inspectionSequence) inspectionLoading = false;
  }
}

function releaseActionLabel(installed: Provider | undefined): string {
  if (!installed) return "Install provider";
  if (!releaseInspection) return "Update provider";
  if (installed.version === releaseInspection.version && installed.indexDigest === releaseInspection.indexDigest) {
    return "Installed";
  }
  return installed.version === releaseInspection.version ? "Replace provider" : "Update provider";
}

function releaseActionDisabled(installed: Provider | undefined): boolean {
  if (releaseInspection?.compatibility !== "compatible") return true;
  return Boolean(
    installed &&
      installed.version === releaseInspection.version &&
      installed.indexDigest === releaseInspection.indexDigest,
  );
}

function releasePending(installed: Provider | undefined): boolean {
  return Boolean(pending[installed ? `provider:${installed.providerId}` : "provider:install"]);
}

function sourceProviderEntries(source: ProviderSource): SourceProviderEntry[] {
  const entries = source.availableProviders.map((available) => ({
    providerId: available.providerId,
    versions: available.versions,
    installed: providers.find(
      (provider) => provider.sourceId === source.id && provider.providerId === available.providerId,
    ),
  }));
  for (const installed of providers.filter((provider) => provider.sourceId === source.id)) {
    if (!entries.some((entry) => entry.providerId === installed.providerId)) {
      entries.push({ providerId: installed.providerId, versions: [], installed });
    }
  }
  return entries.sort((left, right) => {
    const leftLabel = left.installed?.name ?? left.providerId;
    const rightLabel = right.installed?.name ?? right.providerId;
    return leftLabel.localeCompare(rightLabel);
  });
}

function installedProviderCount(source: ProviderSource): number {
  return providers.filter((provider) => provider.sourceId === source.id).length;
}

function formatTimestamp(value: string): string {
  const timestamp = new Date(value);
  if (!Number.isFinite(timestamp.valueOf())) return "Unknown";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(timestamp);
}

function shortIdentity(value: string): string {
  const digest = value.includes(":") ? (value.split(":").at(-1) ?? value) : value;
  return `${digest.slice(0, 12)}${digest.length > 12 ? "..." : ""}`;
}

function removalTitle(): string {
  return pendingRemoval?.kind === "source" ? "Remove trusted source?" : "Remove provider?";
}

function removalDescription(): string {
  if (!pendingRemoval) return "This operation cannot be undone.";
  if (pendingRemoval.kind === "source") {
    return `${pendingRemoval.source.repositoryUrl} will no longer be trusted. Sources used by installed providers cannot be removed.`;
  }
  return `${pendingRemoval.provider.name ?? pendingRemoval.provider.providerId} will no longer be available. Remove its connections first.`;
}

function messageFor(error: unknown, fallback: string): string {
  if (error instanceof ApiRequestError && error.fieldProblems[0]) {
    return `${error.message}: ${error.fieldProblems[0].message}`;
  }
  return error instanceof Error && error.message ? error.message : fallback;
}
</script>

<div class="flex h-full min-h-0 flex-col">
  <PageHeader title="Providers">
    {#snippet actions()}
      <Button
        id="trust-provider-source"
        onclick={toggleSourceForm}
        size="sm"
        variant={showSourceForm ? "ghost" : "secondary"}
      >
        {#if showSourceForm}
          <X
            aria-hidden="true"
            size={14}
          />
          Close
        {:else}
          <Plus
            aria-hidden="true"
            size={14}
          />
          Trust source
        {/if}
      </Button>
    {/snippet}
  </PageHeader>

  <div class="min-h-0 flex-1 overflow-y-auto px-4 py-4 sm:px-6">
    <div class="mx-auto flex w-full max-w-6xl flex-col gap-4 pb-8">
      {#if errorMessage}
        <Panel
          class="flex items-center justify-between gap-3 px-4 py-3"
          role="alert"
          tone="danger"
        >
          <p class="text-label text-danger">{errorMessage}</p>
          <Button
            onclick={() => (errorMessage = "")}
            size="sm"
            variant="ghost"
            >Dismiss</Button
          >
        </Panel>
      {/if}

      {#if sourceConfirmation}
        <Panel
          class="flex items-start gap-3 px-4 py-3"
          role="status"
          tone="accent"
        >
          <Check
            aria-hidden="true"
            class="mt-0.5 shrink-0 text-accent"
            size={16}
          />
          <div class="min-w-0 flex-1">
            <p class="text-label font-[560] text-text-primary">Trusted provider repository</p>
            <p class="break-all font-mono text-meta text-text-muted">{sourceConfirmation.repositoryUrl}</p>
          </div>
          <Button
            aria-label="Dismiss source confirmation"
            onclick={() => (sourceConfirmation = undefined)}
            size="sm"
            variant="ghost"
          >
            <X
              aria-hidden="true"
              size={14}
            />
          </Button>
        </Panel>
      {/if}

      {#if showSourceForm}
        <Panel
          class="p-4 sm:p-5"
          element="section"
        >
          <div class="mb-4 flex items-center gap-2">
            <ShieldCheck
              aria-hidden="true"
              class="text-accent"
              size={16}
            />
            <h2 class="text-heading font-[620] text-text-primary">Trust provider source</h2>
          </div>
          <form
            class="grid gap-4"
            onsubmit={(event) => {
              event.preventDefault();
              void addSource();
            }}
          >
            <label
              class="grid gap-1.5 text-label text-text-muted"
              for="provider-source-url"
            >
              Repository URL
              <TextInput
                id="provider-source-url"
                placeholder="https://registry.example/acme/providers"
                required
                spellcheck={false}
                bind:value={repositoryUrl}
              />
            </label>
            <div class="flex items-start gap-2 rounded-md border border-warning/20 bg-warning/5 px-3 py-2.5">
              <Checkbox
                id="provider-source-confirmation"
                bind:checked={trustConfirmed}
              />
              <label
                class="text-label leading-5 text-text-secondary"
                for="provider-source-confirmation"
              >
                I trust this source to publish executable provider code.
              </label>
            </div>
            <div class="flex justify-end">
              <Button
                disabled={pending["source:add"] || !repositoryUrl.trim() || !trustConfirmed}
                type="submit"
                variant="primary"
              >
                {#if pending["source:add"]}
                  <Spinner size={14} />
                {:else}
                  <ShieldCheck
                    aria-hidden="true"
                    size={14}
                  />
                {/if}
                Trust source
              </Button>
            </div>
          </form>
        </Panel>
      {/if}

      <section
        aria-labelledby="provider-sources-heading"
        class="flex flex-col gap-2"
      >
        <div class="flex items-center justify-between gap-3 px-1">
          <h2
            class="text-heading font-[620] text-text-primary"
            id="provider-sources-heading"
          >
            Provider sources
          </h2>
          {#if !loading}
            <span class="text-meta text-text-muted">
              {sources.length} {sources.length === 1 ? "source" : "sources"}
            </span>
          {/if}
        </div>
        {#if loading && sources.length === 0}
          <Panel
            class="flex items-center justify-center gap-2 px-4 py-10"
            element="section"
          >
            <Spinner size={16} />
            <p class="text-label text-text-muted">Loading trusted sources</p>
          </Panel>
        {:else if sources.length === 0}
          <Panel
            class="flex flex-col items-center justify-center gap-3 px-4 py-10"
            element="section"
          >
            <ShieldCheck
              aria-hidden="true"
              class="text-text-muted"
              size={24}
            />
            <div class="text-center">
              <p class="text-label font-[560] text-text-primary">No trusted sources yet</p>
              <p class="mt-1 text-meta text-text-muted">Trust an OCI repository before installing its providers.</p>
            </div>
            <Button
              onclick={() => {
                cancelProviderInstall();
                showSourceForm = true;
              }}
              size="sm"
              variant="secondary"
            >
              <Plus
                aria-hidden="true"
                size={14}
              />
              Trust source
            </Button>
          </Panel>
        {:else}
          <div class="grid gap-4">
            {#each sources as source (source.id)}
              {@const sourceProviders = sourceProviderEntries(source)}
              {@const installedCount = installedProviderCount(source)}
              <Panel
                class="overflow-hidden"
                element="section"
              >
                <div class="flex flex-wrap items-center gap-3 bg-surface-muted/55 px-4 py-3 sm:px-5">
                  <div
                    class="flex h-9 w-9 shrink-0 items-center justify-center rounded-md border border-border bg-surface"
                  >
                    <ShieldCheck
                      aria-hidden="true"
                      class="text-accent"
                      size={16}
                    />
                  </div>
                  <div class="min-w-0 flex-1">
                    <div class="flex flex-wrap items-center gap-2">
                      <h3 class="break-all font-mono text-label font-[560] text-text-primary">
                        {source.repositoryUrl}
                      </h3>
                      <Chip
                        dot
                        tone="neutral"
                      >
                        {sourceProviders.length} {sourceProviders.length === 1 ? "provider" : "providers"}
                      </Chip>
                      {#if installedCount > 0}
                        <Chip
                          dot
                          tone="success"
                          >{installedCount}
                          installed</Chip
                        >
                      {/if}
                    </div>
                    {#if source.lastError}
                      <p
                        class="mt-1.5 flex items-start gap-1.5 text-meta text-danger"
                        role="alert"
                      >
                        <TriangleAlert
                          aria-hidden="true"
                          class="mt-0.5 shrink-0"
                          size={13}
                        />
                        <span class="wrap-break-word">Last refresh failed: {source.lastError}</span>
                      </p>
                    {/if}
                  </div>
                  <div class="flex w-full items-center justify-end gap-2 sm:w-auto sm:shrink-0">
                    <Button
                      aria-label={`Refresh ${source.repositoryUrl}`}
                      disabled={Boolean(pending[`source:${source.id}`]) || Boolean(pending[`source:${source.id}:remove`])}
                      onclick={() => refreshSource(source)}
                      size="sm"
                      variant="secondary"
                    >
                      {#if pending[`source:${source.id}`]}
                        <Spinner size={14} />
                      {:else}
                        <RefreshCw
                          aria-hidden="true"
                          size={14}
                        />
                      {/if}
                      Refresh
                    </Button>
                    <Button
                      aria-label={`Remove source ${source.repositoryUrl}`}
                      class="max-sm:size-8 max-sm:px-1"
                      disabled={Boolean(pending[`source:${source.id}:remove`]) || Boolean(pending[`source:${source.id}`])}
                      onclick={() => requestSourceRemoval(source)}
                      size="sm"
                      variant="danger"
                    >
                      {#if pending[`source:${source.id}:remove`]}
                        <Spinner size={14} />
                      {:else}
                        <Trash2
                          aria-hidden="true"
                          size={16}
                        />
                      {/if}
                      <span class="hidden sm:inline">Remove</span>
                    </Button>
                  </div>
                </div>

                {#if sourceProviders.length === 0}
                  <div class="border-t border-border px-4 py-8 text-center sm:px-5">
                    <PackageOpen
                      aria-hidden="true"
                      class="mx-auto text-text-muted"
                      size={22}
                    />
                    <p class="mt-2 text-label font-[560] text-text-primary">No providers published</p>
                    <p class="mt-1 text-meta text-text-muted">Refresh this source to check for new providers.</p>
                  </div>
                {:else}
                  <ul
                    aria-label={`Providers from ${source.repositoryUrl}`}
                    class="max-h-96 overflow-y-auto overscroll-y-contain border-t border-border scrollbar-gutter-stable"
                  >
                    {#each sourceProviders as entry (entry.providerId)}
                      {@const installed = entry.installed}
                      <li class="border-b border-border px-4 py-2 last:border-b-0 sm:px-5 sm:py-2.5">
                        <div class="flex flex-wrap items-center gap-2.5">
                          <div class="min-w-0 flex-1">
                            <div class="flex flex-wrap items-center gap-2">
                              <h4 class="text-label font-[560] text-text-primary">
                                {installed?.name ?? entry.providerId}
                              </h4>
                              {#if installed?.name && installed.name !== entry.providerId}
                                <code class="font-mono text-meta text-text-muted">{entry.providerId}</code>
                              {/if}
                              {#if installed}
                                <Chip
                                  dot
                                  tone="success"
                                  >Installed</Chip
                                >
                                {#if installed.availableUpdate}
                                  <Chip
                                    dot
                                    tone="warning"
                                    >{installed.availableUpdate.version}
                                    available</Chip
                                  >
                                {/if}
                              {/if}
                            </div>
                            {#if installed}
                              <p
                                class="mt-0.5 flex min-w-0 flex-wrap items-center gap-x-2 gap-y-0.5 font-mono text-meta text-text-muted"
                              >
                                <span>Version {installed.version}</span>
                                <span aria-hidden="true">&middot;</span>
                                <span title={installed.indexDigest}>Index {shortIdentity(installed.indexDigest)}</span>
                                <span aria-hidden="true">&middot;</span>
                                <time datetime={installed.updatedAt}
                                  >Updated {formatTimestamp(installed.updatedAt)}</time
                                >
                              </p>
                            {:else if entry.versions[0]}
                              <p class="mt-0.5 font-mono text-meta text-text-muted">
                                Latest
                                {entry.versions[0]}
                                {#if entry.versions.length > 1}
                                  &middot; {entry.versions.length} versions
                                {/if}
                              </p>
                            {/if}
                            {#if installed?.lastUpdateError}
                              <p
                                class="mt-1.5 flex items-start gap-1.5 text-meta text-danger"
                                role="alert"
                              >
                                <TriangleAlert
                                  aria-hidden="true"
                                  class="mt-0.5 shrink-0"
                                  size={13}
                                />
                                <span class="wrap-break-word">Update failed: {installed.lastUpdateError}</span>
                              </p>
                            {/if}
                          </div>
                          <div
                            class={installed
                              ? "flex w-full flex-wrap items-center justify-end gap-2 sm:w-auto sm:shrink-0"
                              : "flex shrink-0 items-center justify-end gap-2"}
                          >
                            {#if installed}
                              {#if installed.availableUpdate && installed.updatePolicy === "manual"}
                                <Button
                                  disabled={Boolean(pending[`provider:${entry.providerId}`])}
                                  onclick={() => requestProviderUpdate(installed)}
                                  size="sm"
                                  variant="primary"
                                >
                                  {#if pending[`provider:${entry.providerId}`]}
                                    <Spinner size={14} />
                                  {:else}
                                    <Download
                                      aria-hidden="true"
                                      size={14}
                                    />
                                  {/if}
                                  Update
                                </Button>
                              {/if}
                              <Button
                                aria-label={`Review current release for ${installed.name ?? entry.providerId}`}
                                class="max-sm:size-8 max-sm:px-1"
                                disabled={Boolean(pending[`provider:${entry.providerId}`])}
                                onclick={() => requestProviderRelease(installed.sourceId, installed.providerId, installed.version)}
                                size="sm"
                                variant="secondary"
                              >
                                <Search
                                  aria-hidden="true"
                                  size={14}
                                />
                                <span class="hidden sm:inline">Review</span>
                              </Button>
                              <Select
                                ariaLabel={`Update policy for ${installed.name ?? entry.providerId}`}
                                disabled={Boolean(pending[`provider:${entry.providerId}`])}
                                items={policyItems}
                                onValueChange={(value) => changePolicy(installed, value)}
                                triggerClass="w-44"
                                value={installed.updatePolicy}
                              />
                              <Button
                                aria-label={`Remove provider ${installed.name ?? entry.providerId}`}
                                class="max-sm:size-8 max-sm:px-1"
                                disabled={Boolean(pending[`provider:${entry.providerId}`])}
                                onclick={() => requestProviderRemoval(installed)}
                                size="sm"
                                variant="danger"
                              >
                                {#if pending[`provider:${entry.providerId}`]}
                                  <Spinner size={14} />
                                {:else}
                                  <Trash2
                                    aria-hidden="true"
                                    size={16}
                                  />
                                {/if}
                                <span class="hidden sm:inline">Remove</span>
                              </Button>
                            {:else}
                              <Button
                                aria-expanded={installSourceId === source.id && installProviderId === entry.providerId}
                                aria-label={installSourceId === source.id && installProviderId === entry.providerId
                                  ? `Cancel installing ${entry.providerId}`
                                  : `Install ${entry.providerId}`}
                                class="max-sm:size-8 max-sm:px-1"
                                onclick={() =>
                                  installSourceId === source.id && installProviderId === entry.providerId
                                    ? cancelProviderInstall()
                                    : requestProviderInstall(source.id, entry.providerId)}
                                size="sm"
                                variant={installSourceId === source.id && installProviderId === entry.providerId
                                  ? "ghost"
                                  : "secondary"}
                              >
                                {#if installSourceId === source.id && installProviderId === entry.providerId}
                                  <X
                                    aria-hidden="true"
                                    size={16}
                                  />
                                  <span class="hidden sm:inline">Cancel</span>
                                {:else}
                                  <Download
                                    aria-hidden="true"
                                    size={16}
                                  />
                                  <span class="hidden sm:inline">Install</span>
                                {/if}
                              </Button>
                            {/if}
                          </div>
                        </div>
                      </li>
                    {/each}
                  </ul>
                {/if}
              </Panel>
            {/each}
          </div>
        {/if}
      </section>
    </div>
  </div>

  <Dialog.Root
    onOpenChange={changeReleaseReviewOpen}
    bind:open={releaseReviewOpen}
  >
    <Dialog.Drawer style={releaseReviewOpen ? "translate: none" : undefined}>
      <form
        class="flex min-h-0 flex-1 flex-col"
        onsubmit={(event) => {
          event.preventDefault();
          void installSelectedProvider();
        }}
      >
        <header class="flex shrink-0 items-start justify-between gap-4 border-b border-border-strong px-4 py-4 sm:px-5">
          <div class="min-w-0">
            <Dialog.Title class="text-title font-semibold text-text-primary">
              {selectedInstalledProvider ? "Review provider release" : "Install provider"}
            </Dialog.Title>
            <Dialog.Description
              class="mt-1 flex min-w-0 flex-wrap items-center gap-x-2 font-mono text-meta text-text-muted"
            >
              <span class="wrap-break-word">{installProviderId}</span>
              {#if providerVersion}
                <span aria-hidden="true">&middot;</span>
                <span>v{providerVersion}</span>
              {/if}
            </Dialog.Description>
          </div>
          <IconButton
            label="Close provider release review"
            onclick={() => cancelProviderInstall()}
            size="sm"
            type="button"
          >
            <X
              aria-hidden="true"
              size={15}
            />
          </IconButton>
        </header>

        <div class="min-h-0 min-w-0 flex-1 overflow-x-hidden overflow-y-auto">
          {#if !selectedInstalledProvider}
            <section
              class="grid gap-3 border-b border-border-subtle bg-surface-muted/35 px-4 py-4 sm:grid-cols-2 sm:px-5"
            >
              <div class="grid gap-1.5 text-label text-text-muted">
                <span>Version</span>
                <Select
                  ariaLabel={`Version for ${installProviderId}`}
                  disabled={versionItems.length === 0}
                  items={versionItems}
                  onValueChange={(value) => {
                    providerVersion = value;
                    void inspectSelectedRelease();
                  }}
                  triggerClass="w-full"
                  value={providerVersion}
                />
              </div>
              <div class="grid gap-1.5 text-label text-text-muted">
                <span>Update policy</span>
                <Select
                  ariaLabel={`Update policy for ${installProviderId}`}
                  items={policyItems}
                  onValueChange={(value) => {
                    installPolicy = value as ProviderUpdatePolicy;
                  }}
                  triggerClass="w-full"
                  value={installPolicy}
                />
              </div>
            </section>
          {/if}

          {#if inspectionLoading}
            <div
              class="flex min-h-64 items-center justify-center gap-2 px-4 text-label text-text-muted sm:px-5"
              role="status"
            >
              <Spinner size={16} />
              Inspecting release...
            </div>
          {:else if inspectionErrorMessage}
            <div class="flex min-h-64 flex-col items-center justify-center gap-4 px-6 text-center">
              <Panel
                class="max-w-md px-4 py-3 text-label text-danger"
                role="alert"
                tone="danger"
              >
                {inspectionErrorMessage}
              </Panel>
              <Button
                onclick={() => void inspectSelectedRelease()}
                size="sm"
                type="button"
                variant="secondary"
              >
                <RefreshCw
                  aria-hidden="true"
                  size={14}
                />
                Retry inspection
              </Button>
            </div>
          {:else if releaseInspection}
            <div class="px-4 py-5 sm:px-5">
              <ProviderReleaseReview inspection={releaseInspection} />
            </div>
          {/if}
        </div>

        <footer
          class="flex shrink-0 items-center justify-end gap-2 border-t border-border-strong bg-surface-raised px-4 py-3 sm:px-5"
        >
          <Button
            onclick={() => cancelProviderInstall()}
            type="button"
            variant="ghost"
          >
            Cancel
          </Button>
          <Button
            disabled={releaseActionDisabled(selectedInstalledProvider) || releasePending(selectedInstalledProvider)}
            id="confirm-provider-install"
            type="submit"
            variant="primary"
          >
            {#if releasePending(selectedInstalledProvider)}
              <Spinner size={14} />
            {:else if selectedInstalledProvider}
              <Download
                aria-hidden="true"
                size={14}
              />
            {:else}
              <PackageOpen
                aria-hidden="true"
                size={14}
              />
            {/if}
            {releaseActionLabel(selectedInstalledProvider)}
          </Button>
        </footer>
      </form>
    </Dialog.Drawer>
  </Dialog.Root>

  <AlertDialog.Root
    onOpenChange={changeRemoveOpen}
    bind:open={removeOpen}
  >
    <AlertDialog.Content>
      <AlertDialog.Title class="text-title font-semibold text-text-primary">
        {removalTitle()}
      </AlertDialog.Title>
      <AlertDialog.Description class="mt-1 text-label leading-5 text-text-muted">
        {removalDescription()}
      </AlertDialog.Description>
      {#if removalErrorMessage}
        <p
          class="mt-4 text-label text-danger"
          role="alert"
        >
          {removalErrorMessage}
        </p>
      {/if}
      <div class="mt-5 flex justify-end gap-2">
        <AlertDialog.Cancel
          class="inline-flex h-8 items-center rounded-md border border-border-subtle px-3 text-label text-text-secondary hover:bg-white/4 disabled:pointer-events-none disabled:opacity-35"
          disabled={removalPending()}
          >Cancel</AlertDialog.Cancel
        ><AlertDialog.Action
          class="inline-flex h-8 items-center gap-1.5 rounded-md border border-danger/30 bg-danger/8 px-3 text-label text-danger hover:bg-danger/12 disabled:pointer-events-none disabled:opacity-35"
          disabled={removalPending()}
          onclick={(event) => { event.preventDefault(); void confirmRemoval(); }}
          >{#if removalPending()}
            <Spinner size={13} />
          {/if}
          Remove</AlertDialog.Action
        >
      </div>
    </AlertDialog.Content>
  </AlertDialog.Root>
</div>
