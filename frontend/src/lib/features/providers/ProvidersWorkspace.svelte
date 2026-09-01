<script lang="ts">
import { Check, Download, PackageOpen, Plus, RefreshCw, ShieldCheck, Trash2, X } from "@lucide/svelte";
import { onDestroy, onMount, tick } from "svelte";
import { AlertDialog, Button, Chip, PageHeader, Panel, Select, Spinner, TextInput } from "$lib";
import { ApiRequestError } from "$lib/api/client";
import {
  activateProvider,
  installProvider,
  listProviderInstallations,
  listProviderReleases,
  listProviderRepositories,
  type ProviderInstallation,
  type ProviderRelease,
  type ProviderRepository,
  refreshProviderRepository,
  registerProviderRepository,
  removeProviderInstallation,
} from "$lib/api/providers";

let repositories = $state<ProviderRepository[]>([]);
let releases = $state<ProviderRelease[]>([]);
let installations = $state<ProviderInstallation[]>([]);
let selectedTargets = $state<Record<string, string>>({});
let pending = $state<Record<string, boolean>>({});
let loading = $state(true);
let errorMessage = $state("");
let showRepositoryForm = $state(false);
let repositoryUrl = $state("");
let repositoryConfirmation = $state<{ repository: ProviderRepository; rootSha256: string }>();
let removeOpen = $state(false);
let pendingRemoval = $state<ProviderInstallation>();
let removalErrorMessage = $state("");
let loadSequence = 0;
const lifecycleController = new AbortController();

onMount(() => {
  void loadProviders(lifecycleController.signal);
});

onDestroy(() => {
  loadSequence += 1;
  lifecycleController.abort();
});

async function loadProviders(signal?: AbortSignal) {
  const sequence = ++loadSequence;
  loading = true;
  errorMessage = "";
  try {
    const [nextRepositories, nextReleases, nextInstallations] = await Promise.all([
      listProviderRepositories(signal),
      listProviderReleases(signal),
      listProviderInstallations(signal),
    ]);
    if (sequence !== loadSequence) return;
    repositories = nextRepositories;
    releases = nextReleases;
    installations = nextInstallations;
    selectedTargets = Object.fromEntries(
      nextReleases.map((release) => {
        const key = releaseKey(release);
        const selected = selectedTargets[key];
        const installed = release.installedTargets.find((target) => release.targets.includes(target));
        const fallback =
          release.activeTarget && release.targets.includes(release.activeTarget)
            ? release.activeTarget
            : (installed ?? release.targets[0] ?? "");
        return [key, selected && release.targets.includes(selected) ? selected : fallback];
      }),
    );
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (sequence === loadSequence) errorMessage = messageFor(error, "Providers could not be loaded");
  } finally {
    if (sequence === loadSequence) loading = false;
  }
}

async function addRepository() {
  const url = repositoryUrl.trim();
  if (!url) {
    errorMessage = "Enter a repository invite URL";
    return;
  }
  await runMutation("repository:add", async (signal) => {
    const repository = await registerProviderRepository({ url }, signal);
    repositoryConfirmation = {
      repository,
      rootSha256: new URL(url).hash.slice("#sha256=".length),
    };
    repositoryUrl = "";
    showRepositoryForm = false;
    await tick();
    document.querySelector<HTMLButtonElement>("#add-provider-repository")?.focus();
  });
}

async function refreshRepository(repository: ProviderRepository) {
  await runMutation(`repository:${repository.id}`, (signal) => refreshProviderRepository(repository.id, signal));
}

async function installRelease(release: ProviderRelease) {
  const target = selectedTarget(release);
  if (!target) return;
  await runMutation(`release:${releaseKey(release)}`, (signal) =>
    installProvider(
      {
        repositoryId: release.repositoryId,
        providerId: release.providerId,
        version: release.version,
        target,
      },
      signal,
    ),
  );
}

async function activateInstallation(installation: ProviderInstallation) {
  await runMutation(providerLifecycleKey(installation.providerId), (signal) =>
    activateProvider(installation.providerId, installation.id, signal),
  );
}

async function removeInstallation(installation: ProviderInstallation) {
  removalErrorMessage = "";
  pendingRemoval = installation;
  removeOpen = true;
}

async function confirmRemoval() {
  const installation = pendingRemoval;
  if (!installation) return;
  removalErrorMessage = "";
  const succeeded = await runMutation(
    providerLifecycleKey(installation.providerId),
    (signal) => removeProviderInstallation(installation.id, signal),
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

function removalPending(): boolean {
  return Boolean(pendingRemoval && pending[providerLifecycleKey(pendingRemoval.providerId)]);
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
    if (onError) onError(message);
    else errorMessage = message;
    return false;
  } finally {
    if (!signal.aborted) pending = { ...pending, [key]: false };
  }
}

function releaseKey(release: ProviderRelease): string {
  return `${release.repositoryId}:${release.providerId}:${release.version}`;
}

function providerLifecycleKey(providerId: string): string {
  return `provider:${providerId}:lifecycle`;
}

function repositoryName(repositoryId: string): string {
  return repositories.find((repository) => repository.id === repositoryId)?.displayName ?? repositoryId;
}

function targetItems(release: ProviderRelease) {
  return release.targets.map((target) => ({ value: target, label: target }));
}

function selectedTarget(release: ProviderRelease): string {
  const selected = selectedTargets[releaseKey(release)];
  return selected && release.targets.includes(selected) ? selected : (release.targets[0] ?? "");
}

function selectedTargetInstalled(release: ProviderRelease): boolean {
  return release.installedTargets.includes(selectedTarget(release));
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
        id="add-provider-repository"
        onclick={() => (showRepositoryForm = !showRepositoryForm)}
        size="sm"
        variant={showRepositoryForm ? "ghost" : "secondary"}
      >
        {#if showRepositoryForm}
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
          Add repository
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

      {#if repositoryConfirmation}
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
            <p class="text-label font-[560] text-text-primary">
              Trusted {repositoryConfirmation.repository.displayName}
            </p>
            <p class="truncate font-mono text-meta text-text-muted">{repositoryConfirmation.repository.id}</p>
            <p class="mt-1 text-meta text-text-muted">
              Root SHA-256
              <span class="ml-1 break-all font-mono text-text-primary">{repositoryConfirmation.rootSha256}</span>
            </p>
          </div>
          <Button
            aria-label="Dismiss repository confirmation"
            onclick={() => (repositoryConfirmation = undefined)}
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

      {#if showRepositoryForm}
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
            <h2 class="text-heading font-[620] text-text-primary">Trust repository</h2>
          </div>
          <form
            class="grid gap-3"
            onsubmit={(event) => { event.preventDefault(); void addRepository(); }}
          >
            <label
              class="grid gap-1.5 text-label text-text-muted"
              for="provider-repository-url"
            >
              Repository invite URL
              <TextInput
                autocomplete="url"
                id="provider-repository-url"
                placeholder="https://providers.example/repository.json#sha256=..."
                required
                spellcheck={false}
                type="url"
                bind:value={repositoryUrl}
              />
            </label>
            <div class="flex justify-end">
              <Button
                disabled={pending["repository:add"] || !repositoryUrl.trim()}
                type="submit"
                variant="primary"
              >
                {#if pending["repository:add"]}
                  <Spinner size={14} />
                {:else}
                  <ShieldCheck
                    aria-hidden="true"
                    size={14}
                  />
                {/if}
                Trust repository
              </Button>
            </div>
          </form>
        </Panel>
      {/if}

      <Panel
        class="overflow-hidden"
        element="section"
      >
        <div class="flex min-h-12 items-center justify-between border-b border-border-subtle px-4 py-2.5">
          <div>
            <h2 class="text-heading font-[620] text-text-primary">Repositories</h2>
            <p class="text-meta text-text-muted">{repositories.length} trusted</p>
          </div>
          <Button
            disabled={loading}
            onclick={() => loadProviders(lifecycleController.signal)}
            size="sm"
            variant="ghost"
          >
            <RefreshCw
              aria-hidden="true"
              class={loading ? "animate-spin" : ""}
              size={14}
            />
            Reload
          </Button>
        </div>
        {#if loading && repositories.length === 0}
          <div
            class="flex h-24 items-center justify-center gap-2 text-label text-text-muted"
            role="status"
          >
            <Spinner />Loading providers
          </div>
        {:else if repositories.length === 0}
          <p class="px-4 py-6 text-label text-text-muted">No repositories</p>
        {:else}
          {#each repositories as repository (repository.id)}
            <div class="flex flex-wrap items-center gap-3 border-b border-border-subtle px-4 py-3 last:border-b-0">
              <div class="min-w-0 flex-1">
                <div class="flex flex-wrap items-center gap-2">
                  <p class="truncate text-label font-[560] text-text-primary">{repository.displayName}</p>
                  {#if repository.builtIn}
                    <Chip
                      dot
                      tone="accent"
                      >Built in</Chip
                    >
                  {/if}
                  {#if !repository.enabled}
                    <Chip
                      dot
                      tone="warning"
                      >Disabled</Chip
                    >
                  {/if}
                </div>
                <p class="mt-0.5 truncate font-mono text-meta text-text-muted">{repository.id}</p>
              </div>
              <div class="text-right text-meta text-text-muted">
                <p>
                  {repository.catalogSequence === null ? "Not refreshed" : `Sequence ${repository.catalogSequence}`}
                </p>
                {#if repository.refreshedAt}
                  <p>{new Date(repository.refreshedAt).toLocaleString()}</p>
                {/if}
              </div>
              <Button
                aria-label={`Refresh ${repository.displayName}`}
                disabled={pending[`repository:${repository.id}`] || !repository.enabled}
                onclick={() => refreshRepository(repository)}
                size="sm"
              >
                <RefreshCw
                  aria-hidden="true"
                  class={pending[`repository:${repository.id}`] ? "animate-spin" : ""}
                  size={14}
                />
                Refresh
              </Button>
            </div>
          {/each}
        {/if}
      </Panel>

      <Panel
        class="overflow-hidden"
        element="section"
      >
        <div class="border-b border-border-subtle px-4 py-3">
          <h2 class="text-heading font-[620] text-text-primary">Catalog</h2>
          <p class="text-meta text-text-muted">{releases.length} releases</p>
        </div>
        {#if releases.length === 0}
          <p class="px-4 py-6 text-label text-text-muted">Refresh a repository to load its catalog</p>
        {:else}
          {#each releases as release (releaseKey(release))}
            <div
              class="flex flex-col gap-3 border-b border-border-subtle px-4 py-3 last:border-b-0 lg:flex-row lg:items-center"
            >
              <div class="min-w-0 flex-1">
                <div class="flex flex-wrap items-center gap-2">
                  <p class="wrap-break-word text-label font-[580] text-text-primary">{release.name}</p>
                  <Chip>{release.version}</Chip>
                  {#if release.activeTarget}
                    <Chip
                      dot
                      tone="success"
                      >Active {release.activeTarget}</Chip
                    >
                  {:else if release.installedTargets.length > 0}
                    <Chip
                      dot
                      tone="info"
                      >Installed</Chip
                    >
                  {/if}
                </div>
                <p class="mt-1 wrap-break-word text-label text-text-secondary">{release.summary}</p>
                <p class="mt-1 break-all font-mono text-meta text-text-muted">
                  {release.providerId}
                  · {repositoryName(release.repositoryId)}
                </p>
              </div>
              <div class="flex min-w-0 flex-wrap items-center gap-2">
                {#if release.targets.length > 1}
                  <Select
                    ariaLabel={`Target for ${release.name}`}
                    items={targetItems(release)}
                    onValueChange={(value) => (selectedTargets = { ...selectedTargets, [releaseKey(release)]: value })}
                    triggerClass="max-w-72"
                    value={selectedTarget(release)}
                  />
                {:else if release.targets[0]}
                  <span class="truncate font-mono text-meta text-text-muted">{release.targets[0]}</span>
                {/if}
                {#if !selectedTargetInstalled(release)}
                  <Button
                    aria-label={`Install ${release.providerId} ${release.version} for ${selectedTarget(release)}`}
                    disabled={pending[`release:${releaseKey(release)}`] || release.targets.length === 0}
                    onclick={() => installRelease(release)}
                    size="sm"
                    variant="primary"
                  >
                    {#if pending[`release:${releaseKey(release)}`]}
                      <Spinner size={14} />
                    {:else}
                      <Download
                        aria-hidden="true"
                        size={14}
                      />
                    {/if}
                    Install
                  </Button>
                {/if}
              </div>
            </div>
          {/each}
        {/if}
      </Panel>

      <Panel
        class="overflow-hidden"
        element="section"
      >
        <div class="border-b border-border-subtle px-4 py-3">
          <h2 class="text-heading font-[620] text-text-primary">Installed versions</h2>
          <p class="text-meta text-text-muted">{installations.length} retained</p>
        </div>
        {#if installations.length === 0}
          <div class="flex items-center gap-2 px-4 py-6 text-label text-text-muted">
            <PackageOpen
              aria-hidden="true"
              size={16}
            />
            No provider versions installed
          </div>
        {:else}
          {#each installations as installation (installation.id)}
            <div class="flex flex-wrap items-center gap-3 border-b border-border-subtle px-4 py-3 last:border-b-0">
              <div class="min-w-0 flex-1">
                <div class="flex flex-wrap items-center gap-2">
                  <p class="break-all font-mono text-label font-[560] text-text-primary">{installation.providerId}</p>
                  <Chip>{installation.version}</Chip>
                  {#if installation.active}
                    <Chip
                      dot
                      tone="success"
                      >Active</Chip
                    >
                  {/if}
                </div>
                <p class="mt-1 truncate font-mono text-meta text-text-muted">
                  {installation.target}
                  · {installation.packageSha256.slice(0, 12)}
                </p>
              </div>
              {#if installation.active}
                <Button
                  disabled
                  size="sm"
                  variant="ghost"
                  ><Check
                    aria-hidden="true"
                    size={14}
                  />Active</Button
                >
              {:else}
                <Button
                  aria-busy={pending[providerLifecycleKey(installation.providerId)]}
                  aria-label={`Activate ${installation.providerId} ${installation.version} for ${installation.target}`}
                  disabled={pending[providerLifecycleKey(installation.providerId)]}
                  onclick={() => activateInstallation(installation)}
                  size="sm"
                >
                  {#if pending[providerLifecycleKey(installation.providerId)]}
                    <Spinner size={14} />
                  {:else}
                    <Check
                      aria-hidden="true"
                      size={14}
                    />
                  {/if}
                  Activate
                </Button>
                <Button
                  aria-label={`Remove ${installation.providerId} ${installation.version} for ${installation.target}`}
                  disabled={pending[providerLifecycleKey(installation.providerId)]}
                  onclick={() => removeInstallation(installation)}
                  size="sm"
                  variant="danger"
                >
                  <Trash2
                    aria-hidden="true"
                    size={14}
                  />Remove
                </Button>
              {/if}
            </div>
          {/each}
        {/if}
      </Panel>
    </div>
  </div>
</div>

<AlertDialog.Root
  onOpenChange={changeRemoveOpen}
  bind:open={removeOpen}
>
  <AlertDialog.Content>
    <AlertDialog.Title class="text-title font-semibold text-text-primary">Remove provider version?</AlertDialog.Title>
    <AlertDialog.Description class="mt-1 text-label leading-5 text-text-muted">
      {pendingRemoval?.providerId ?? "This provider"}
      {pendingRemoval?.version ?? ""}
      for {pendingRemoval?.target ?? "this target"} will no longer be available for new launches. Existing sessions
      retain their current generation until they stop.
    </AlertDialog.Description>
    {#if removalErrorMessage}
      <p
        class="mt-3 text-label text-danger"
        role="alert"
      >
        {removalErrorMessage}
      </p>
    {/if}
    <div class="mt-5 flex justify-end gap-2">
      <AlertDialog.Cancel
        class="inline-flex h-8 items-center rounded-md border border-border-subtle px-3 text-label text-text-secondary hover:bg-white/4"
        disabled={removalPending()}
        >Cancel</AlertDialog.Cancel
      ><AlertDialog.Action
        class="inline-flex h-8 items-center gap-1.5 rounded-md border border-danger/30 bg-danger/8 px-3 text-label text-danger hover:bg-danger/12 disabled:pointer-events-none disabled:opacity-35"
        disabled={!pendingRemoval || removalPending()}
        onclick={(event) => { event.preventDefault(); void confirmRemoval(); }}
        >{#if removalPending()}
          <Spinner size={13} />
        {/if}
        Remove version</AlertDialog.Action
      >
    </div>
  </AlertDialog.Content>
</AlertDialog.Root>
