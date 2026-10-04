<script lang="ts">
import {
  Check,
  Download,
  PackageOpen,
  PackagePlus,
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
  Dialog,
  EmptyState,
  IconButton,
  PageHeader,
  Panel,
  Select,
  Spinner,
  Switch,
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
let availableProviders = $derived(
  sources
    .flatMap((source) =>
      source.availableProviders
        .filter(
          (available) =>
            !providers.some(
              (provider) => provider.sourceId === source.id && provider.providerId === available.providerId,
            ),
        )
        .map((available) => ({
          source,
          providerId: available.providerId,
          versions: available.versions,
        })),
    )
    .sort((left, right) => left.providerId.localeCompare(right.providerId)),
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

async function changePolicy(provider: Provider, automatic: boolean) {
  const updatePolicy: ProviderUpdatePolicy = automatic ? "automatic" : "manual";
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

function sourceLabel(sourceId: string): string {
  const repositoryUrl = sources.find((source) => source.id === sourceId)?.repositoryUrl;
  if (!repositoryUrl) return sourceId;
  try {
    const url = new URL(repositoryUrl);
    return `${url.host}${url.pathname}`.replace(/\/$/, "");
  } catch {
    return repositoryUrl;
  }
}

function shortIdentity(value: string): string {
  const digest = value.includes(":") ? value.slice(value.indexOf(":") + 1) : value;
  return `${digest.slice(0, 12)}${digest.length > 12 ? "..." : ""}`;
}

function formatTimestamp(value: string): string {
  const timestamp = new Date(value);
  if (!Number.isFinite(timestamp.valueOf())) return "Unknown";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(timestamp);
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
  <PageHeader title="Providers" />

  <div class="min-h-0 flex-1 overflow-y-auto p-4 sm:p-6">
    <div class="mx-auto flex w-full max-w-5xl flex-col gap-4">
      {#if errorMessage}
        <Panel
          class="flex items-center justify-between gap-3 px-4 py-3"
          role="alert"
          tone="danger"
        >
          <p class="text-label text-danger">{errorMessage}</p>
          <IconButton
            label="Dismiss error"
            onclick={() => (errorMessage = "")}
            size="sm"
            variant="ghost"
          >
            <X
              aria-hidden="true"
              size={14}
            />
          </IconButton>
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
          <IconButton
            label="Dismiss source confirmation"
            onclick={() => (sourceConfirmation = undefined)}
            size="sm"
            variant="ghost"
          >
            <X
              aria-hidden="true"
              size={14}
            />
          </IconButton>
        </Panel>
      {/if}

      <Panel
        aria-labelledby="trusted-sources-heading"
        class="overflow-hidden"
        element="section"
      >
        <header class="flex min-h-12 items-center justify-between gap-3 border-b border-border-subtle px-3 sm:px-4">
          <div class="flex min-w-0 items-center gap-2">
            <h2
              class="text-label font-[510] text-text-primary"
              id="trusted-sources-heading"
            >
              Trusted sources
            </h2>
            <span class="font-mono text-meta tabular-nums text-text-disabled">{sources.length}</span>
          </div>
          <Button
            aria-controls="provider-source-form"
            aria-expanded={showSourceForm}
            id="trust-provider-source"
            onclick={toggleSourceForm}
            size="sm"
            variant="ghost"
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
        </header>
        {#if showSourceForm}
          <form
            class="grid gap-4 border-b border-border-subtle px-3 py-4 sm:px-4"
            id="provider-source-form"
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
        {/if}
        {#if sources.length === 0}
          <p class="px-3 py-5 text-label text-text-muted sm:px-4">No trusted provider sources.</p>
        {:else}
          <ul
            aria-label="Trusted provider sources"
            class="divide-y divide-border-subtle"
          >
            {#each sources as source (source.id)}
              <li class="flex min-w-0 items-center gap-3 px-3 py-2.5 sm:px-4">
                <ShieldCheck
                  aria-hidden="true"
                  class="shrink-0 text-text-muted"
                  size={15}
                />
                <div class="min-w-0 flex-1">
                  <p
                    class="truncate font-mono text-label text-text-secondary"
                    title={source.repositoryUrl}
                  >
                    {source.repositoryUrl}
                  </p>
                  <p class="mt-0.5 text-meta text-text-muted">
                    {source.availableProviders.length}
                    provider{source.availableProviders.length === 1 ? "" : "s"}
                  </p>
                  {#if source.lastError}
                    <p
                      class="mt-1 text-meta text-danger"
                      role="alert"
                    >
                      Last refresh failed: {source.lastError}
                    </p>
                  {/if}
                </div>
                <IconButton
                  disabled={Boolean(pending[`source:${source.id}`]) || Boolean(pending[`source:${source.id}:remove`])}
                  label={`Refresh ${source.repositoryUrl}`}
                  onclick={() => refreshSource(source)}
                  size="sm"
                  title="Refresh source"
                >
                  {#if pending[`source:${source.id}`]}
                    <Spinner size={13} />
                  {:else}
                    <RefreshCw
                      aria-hidden="true"
                      size={13}
                    />
                  {/if}
                </IconButton>
                <IconButton
                  disabled={Boolean(pending[`source:${source.id}:remove`]) || Boolean(pending[`source:${source.id}`])}
                  label={`Remove source ${source.repositoryUrl}`}
                  onclick={() => requestSourceRemoval(source)}
                  size="sm"
                  title="Remove source"
                  variant="danger"
                >
                  <Trash2
                    aria-hidden="true"
                    size={13}
                  />
                </IconButton>
              </li>
            {/each}
          </ul>
        {/if}
      </Panel>

      <Panel
        aria-labelledby="provider-directory-heading"
        class="overflow-hidden"
        element="section"
      >
        <header class="flex min-h-12 items-center justify-between gap-3 border-b border-border-subtle px-3 sm:px-4">
          <div>
            <h2
              class="text-label font-[510] text-text-primary"
              id="provider-directory-heading"
            >
              Provider directory
            </h2>
            <p class="mt-0.5 text-meta text-text-muted">
              {providers.length}
              installed &middot; {availableProviders.length} available
            </p>
          </div>
        </header>

        {#if loading && providers.length === 0 && availableProviders.length === 0}
          <div class="flex min-h-40 items-center justify-center gap-2 text-label text-text-muted">
            <Spinner size={16} />
            Loading providers...
          </div>
        {:else if providers.length === 0 && availableProviders.length === 0}
          <EmptyState
            class="min-h-56 border-0"
            description="Trust a provider source to discover integrations."
            icon={PackageOpen}
            title="No providers available"
          >
            {#snippet actions()}
              <Button
                onclick={() => (showSourceForm = true)}
                size="sm"
                variant="secondary"
              >
                <ShieldCheck
                  aria-hidden="true"
                  size={14}
                />
                Trust source
              </Button>
            {/snippet}
          </EmptyState>
        {:else}
          <div
            aria-hidden="true"
            class="hidden h-8 grid-cols-[minmax(12rem,1.4fr)_minmax(9rem,1fr)_5rem_8rem_7rem] items-center gap-3 border-b border-border-subtle px-4 text-meta font-medium text-text-muted sm:grid"
          >
            <span>Provider</span>
            <span>Source</span>
            <span>Version</span>
            <span>Updates</span>
            <span class="text-right">Actions</span>
          </div>
          <ul
            aria-label={sources.length === 1 ? `Providers from ${sources[0].repositoryUrl}` : "Provider directory"}
            class="max-h-96 divide-y divide-border-subtle overflow-y-auto overscroll-y-contain scrollbar-gutter-stable"
          >
            {#each providers as provider (provider.providerId)}
              <li
                class="grid min-w-0 gap-3 px-3 py-3 sm:grid-cols-[minmax(12rem,1.4fr)_minmax(9rem,1fr)_5rem_8rem_7rem] sm:items-center sm:px-4 sm:py-2.5"
              >
                <div class="min-w-0">
                  <div class="flex min-w-0 items-center gap-2">
                    <h3 class="truncate text-label font-[510] text-text-primary">
                      {provider.name ?? provider.providerId}
                    </h3>
                    <span class={provider.availableUpdate ? "text-meta text-warning" : "text-meta text-text-disabled"}>
                      {provider.availableUpdate ? `${provider.availableUpdate.version} available` : "Installed"}
                    </span>
                  </div>
                  <p class="mt-0.5 truncate font-mono text-meta text-text-muted">{provider.providerId}</p>
                  <p class="mt-0.5 flex flex-wrap gap-x-2 font-mono text-meta text-text-disabled">
                    <span>Index {shortIdentity(provider.indexDigest)}</span>
                    <time datetime={provider.updatedAt}>Updated {formatTimestamp(provider.updatedAt)}</time>
                  </p>
                  {#if provider.lastUpdateError}
                    <p
                      class="mt-1 flex items-start gap-1.5 text-meta text-danger"
                      role="alert"
                    >
                      <TriangleAlert
                        aria-hidden="true"
                        class="mt-0.5 shrink-0"
                        size={12}
                      />
                      <span class="wrap-break-word">Update failed: {provider.lastUpdateError}</span>
                    </p>
                  {/if}
                </div>
                <div class="grid min-w-0 grid-cols-[4.5rem_minmax(0,1fr)] items-center gap-2 sm:block">
                  <span class="text-meta text-text-disabled sm:hidden">Source</span>
                  <p
                    class="min-w-0 truncate text-label text-text-muted"
                    title={sourceLabel(provider.sourceId)}
                  >
                    {sourceLabel(provider.sourceId)}
                  </p>
                </div>
                <div class="grid grid-cols-[4.5rem_minmax(0,1fr)] items-center gap-2 sm:block">
                  <span class="text-meta text-text-disabled sm:hidden">Version</span>
                  <p class="font-mono text-label text-text-secondary">{provider.version}</p>
                </div>
                <div class="grid grid-cols-[4.5rem_minmax(0,1fr)] items-center gap-2 sm:block">
                  <span class="text-meta text-text-disabled sm:hidden">Updates</span>
                  <div class="flex items-center gap-2">
                    <Switch
                      aria-label={`Automatic updates for ${provider.name ?? provider.providerId}`}
                      checked={provider.updatePolicy === "automatic"}
                      disabled={Boolean(pending[`provider:${provider.providerId}`])}
                      onCheckedChange={(checked) => void changePolicy(provider, Boolean(checked))}
                    />
                    <span class="text-meta text-text-muted">
                      {provider.updatePolicy === "automatic" ? "Auto" : "Manual"}
                    </span>
                  </div>
                </div>
                <div class="flex items-center justify-end gap-1">
                  {#if provider.availableUpdate && provider.updatePolicy === "manual"}
                    <IconButton
                      disabled={Boolean(pending[`provider:${provider.providerId}`])}
                      label={`Update ${provider.name ?? provider.providerId}`}
                      onclick={() => requestProviderUpdate(provider)}
                      size="sm"
                      title="Update provider"
                      variant="primary"
                    >
                      <Download
                        aria-hidden="true"
                        size={13}
                      />
                    </IconButton>
                  {/if}
                  <IconButton
                    disabled={Boolean(pending[`provider:${provider.providerId}`])}
                    label={`Review current release for ${provider.name ?? provider.providerId}`}
                    onclick={() => requestProviderRelease(provider.sourceId, provider.providerId, provider.version)}
                    size="sm"
                    title="Review release"
                  >
                    <Search
                      aria-hidden="true"
                      size={13}
                    />
                  </IconButton>
                  <IconButton
                    disabled={Boolean(pending[`provider:${provider.providerId}`])}
                    label={`Remove provider ${provider.name ?? provider.providerId}`}
                    onclick={() => requestProviderRemoval(provider)}
                    size="sm"
                    title="Remove provider"
                    variant="danger"
                  >
                    {#if pending[`provider:${provider.providerId}`]}
                      <Spinner size={13} />
                    {:else}
                      <Trash2
                        aria-hidden="true"
                        size={13}
                      />
                    {/if}
                  </IconButton>
                </div>
              </li>
            {/each}
            {#each availableProviders as entry (`${entry.source.id}:${entry.providerId}`)}
              <li
                class="grid min-w-0 gap-3 px-3 py-3 sm:grid-cols-[minmax(12rem,1.4fr)_minmax(9rem,1fr)_5rem_8rem_7rem] sm:items-center sm:px-4 sm:py-2.5"
              >
                <div class="min-w-0">
                  <div class="flex min-w-0 items-center gap-2">
                    <h3 class="truncate text-label font-[510] text-text-primary">{entry.providerId}</h3>
                    <span class="text-meta text-text-disabled">Available</span>
                  </div>
                </div>
                <div class="grid min-w-0 grid-cols-[4.5rem_minmax(0,1fr)] items-center gap-2 sm:block">
                  <span class="text-meta text-text-disabled sm:hidden">Source</span>
                  <p
                    class="min-w-0 truncate text-label text-text-muted"
                    title={entry.source.repositoryUrl}
                  >
                    {sourceLabel(entry.source.id)}
                  </p>
                </div>
                <div class="grid grid-cols-[4.5rem_minmax(0,1fr)] items-center gap-2 sm:block">
                  <span class="text-meta text-text-disabled sm:hidden">Version</span>
                  <p class="font-mono text-label text-text-secondary">{entry.versions[0] ?? "Unknown"}</p>
                </div>
                <div class="grid grid-cols-[4.5rem_minmax(0,1fr)] items-center gap-2 sm:block">
                  <span class="text-meta text-text-disabled sm:hidden">Updates</span>
                  <span class="text-label text-text-disabled">Not configured</span>
                </div>
                <div class="flex items-center justify-end gap-1">
                  <IconButton
                    disabled={entry.versions.length === 0}
                    label={`Preview release for ${entry.providerId}`}
                    onclick={() => requestProviderRelease(entry.source.id, entry.providerId)}
                    size="sm"
                    title="Preview release"
                  >
                    <Search
                      aria-hidden="true"
                      size={13}
                    />
                  </IconButton>
                  <IconButton
                    disabled={entry.versions.length === 0}
                    label={`Install ${entry.providerId}`}
                    onclick={() => requestProviderInstall(entry.source.id, entry.providerId)}
                    size="sm"
                    title="Install provider"
                    variant="success"
                  >
                    <PackagePlus
                      aria-hidden="true"
                      size={13}
                    />
                  </IconButton>
                </div>
              </li>
            {/each}
          </ul>
        {/if}
      </Panel>
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
              {selectedInstalledProvider ? "Review provider release" : "Preview provider release"}
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
              class="grid gap-3 border-b border-border-subtle bg-surface-raised/35 px-4 py-4 sm:grid-cols-2 sm:px-5"
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
              <div
                class="flex items-center justify-between gap-3 rounded-control border border-border-subtle bg-white/2 px-3 py-2"
              >
                <div>
                  <p class="text-label text-text-secondary">Automatic updates</p>
                  <p class="mt-0.5 text-meta text-text-muted">Install future compatible releases</p>
                </div>
                <Switch
                  aria-label={`Install updates automatically for ${installProviderId}`}
                  checked={installPolicy === "automatic"}
                  onCheckedChange={(checked) => {
                    installPolicy = checked ? "automatic" : "manual";
                  }}
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
