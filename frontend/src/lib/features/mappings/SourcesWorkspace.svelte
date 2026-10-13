<script lang="ts">
import { ArrowLeft, CloudDownload, HardDrive, Pencil, Plus, RefreshCw, Trash2, X } from "@lucide/svelte";
import { onDestroy, onMount } from "svelte";
import { AlertDialog, Button, Dialog, IconButton, PageHeader, Spinner, Switch, TextInput } from "$lib";
import { ApiRequestError } from "$lib/api/client";
import {
  deleteMappingSource,
  getMappingSources,
  type MappingSource,
  refreshMappingSource,
  saveMappingSource,
} from "$lib/api/mappings";

let sources = $state<MappingSource[]>([]);
let fingerprint = $state("");
let loading = $state(true);
let busy = $state(false);
let errorMessage = $state("");
let statusMessage = $state("");
let editorOpen = $state(false);
let editing = $state<MappingSource>();
let name = $state("");
let url = $state("");
let branch = $state("main");
let intervalMinutes = $state(60);
let enabled = $state(true);
let editorError = $state("");
let removeTarget = $state<MappingSource>();
let removeOpen = $state(false);
let returnFocus: HTMLElement | undefined;
let sequence = 0;
const lifecycle = new AbortController();

onMount(() => {
  void load();
});
onDestroy(() => {
  sequence += 1;
  lifecycle.abort();
});

function message(error: unknown): string {
  return error instanceof Error ? error.message : "Mapping sources are unavailable";
}
function date(value: string | null | undefined): string {
  return value ? new Date(value).toLocaleString() : "Not checked";
}

async function load(): Promise<void> {
  const current = ++sequence;
  loading = true;
  errorMessage = "";
  try {
    const snapshot = await getMappingSources(lifecycle.signal);
    if (current === sequence) {
      fingerprint = snapshot.fingerprint;
      sources = snapshot.sources;
    }
  } catch (error) {
    if (current === sequence && !lifecycle.signal.aborted) errorMessage = message(error);
  } finally {
    if (current === sequence) loading = false;
  }
}

async function reportFailure(error: unknown): Promise<void> {
  if (lifecycle.signal.aborted) return;
  if (error instanceof ApiRequestError && error.code === "mappings_changed") await load();
  errorMessage = message(error);
}

function rememberFocus(): void {
  returnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : undefined;
}
function restoreFocus(event: Event): void {
  event.preventDefault();
  returnFocus?.focus();
}

function edit(source?: MappingSource): void {
  if (!fingerprint || busy) return;
  rememberFocus();
  editing = source;
  name = source?.displayName ?? "";
  url = source?.url ?? "";
  branch = source?.branch ?? "main";
  intervalMinutes = (source?.pollIntervalSeconds ?? 3600) / 60;
  enabled = source?.enabled ?? true;
  editorError = "";
  editorOpen = true;
}

async function save(event: SubmitEvent): Promise<void> {
  event.preventDefault();
  if (!fingerprint || busy) return;
  busy = true;
  editorError = "";
  try {
    const next = await saveMappingSource(
      {
        expectedFingerprint: fingerprint,
        displayName: name.trim(),
        url: url.trim(),
        branch: branch.trim(),
        pollIntervalSeconds: Math.round(intervalMinutes * 60),
        enabled,
      },
      editing?.id,
      lifecycle.signal,
    );
    fingerprint = next.fingerprint;
    await load();
    busy = false;
    editorOpen = false;
  } catch (error) {
    editorError = message(error);
    if (error instanceof ApiRequestError && error.code === "mappings_changed") await load();
  } finally {
    busy = false;
  }
}

async function toggle(source: MappingSource, checked: boolean): Promise<void> {
  if (!fingerprint || busy || !source.url || !source.branch || !source.pollIntervalSeconds) return;
  busy = true;
  errorMessage = "";
  statusMessage = "";
  sources = sources.map((candidate) => (candidate.id === source.id ? { ...source, enabled: checked } : candidate));
  try {
    const next = await saveMappingSource(
      {
        expectedFingerprint: fingerprint,
        displayName: source.displayName,
        url: source.url,
        branch: source.branch,
        pollIntervalSeconds: source.pollIntervalSeconds,
        enabled: checked,
      },
      source.id,
      lifecycle.signal,
    );
    fingerprint = next.fingerprint;
    await load();
  } catch (error) {
    sources = sources.map((candidate) => (candidate.id === source.id ? source : candidate));
    await reportFailure(error);
  } finally {
    busy = false;
  }
}

async function refresh(source: MappingSource): Promise<void> {
  if (!fingerprint || busy) return;
  busy = true;
  errorMessage = "";
  try {
    fingerprint = (await refreshMappingSource(source.id, fingerprint, lifecycle.signal)).fingerprint;
    statusMessage = source.kind === "local" ? "Local repository reloaded" : `Fetch scheduled for ${source.displayName}`;
    await load();
  } catch (error) {
    await reportFailure(error);
  } finally {
    busy = false;
  }
}

function confirmRemove(source: MappingSource): void {
  rememberFocus();
  removeTarget = source;
  removeOpen = true;
}

async function remove(): Promise<void> {
  if (!fingerprint || !removeTarget || busy) return;
  busy = true;
  errorMessage = "";
  try {
    fingerprint = (await deleteMappingSource(removeTarget.id, fingerprint, lifecycle.signal)).fingerprint;
    removeOpen = false;
    await load();
  } catch (error) {
    await reportFailure(error);
  } finally {
    busy = false;
  }
}
</script>

<svelte:head><title>Trakkin &middot; Mapping sources</title></svelte:head>

<div class="flex h-full min-h-0 flex-col">
  <PageHeader title="Mapping sources">
    {#snippet actions()}
      <IconButton
        href="/mappings"
        label="Mappings"
        size="sm"
        ><ArrowLeft
          aria-hidden="true"
          size={14}
        /></IconButton
      >
      <IconButton
        disabled={!fingerprint || busy}
        label="Add mapping source"
        onclick={() => edit()}
        size="sm"
        ><Plus
          aria-hidden="true"
          size={14}
        /></IconButton
      >
      <IconButton
        disabled={loading || busy}
        label="Reload sources"
        onclick={() => {
  statusMessage = "";
  void load();
}}
        size="sm"
        ><RefreshCw
          aria-hidden="true"
          size={14}
        /></IconButton
      >
    {/snippet}
  </PageHeader>
  {#if errorMessage}
    <p
      class="border-b border-danger/20 px-4 py-3 text-label text-danger"
      role="alert"
    >
      {errorMessage}
    </p>
  {/if}
  {#if statusMessage}
    <p
      class="border-b border-border-subtle px-4 py-3 text-label text-text-muted"
      role="status"
    >
      {statusMessage}
    </p>
  {/if}
  {#if loading && !fingerprint}
    <div
      class="flex min-h-48 items-center justify-center gap-2 text-label text-text-muted"
      role="status"
    >
      <Spinner size={16} />Loading sources...
    </div>
  {:else if fingerprint}
    <ul
      aria-label="Mapping sources"
      class="min-h-0 flex-1 divide-y divide-border-subtle overflow-y-auto"
    >
      {#each sources as source (source.id)}
        <li class="min-w-0 px-4 py-4 sm:px-6">
          <div class="flex items-start gap-3">
            {#if source.kind === "local"}
              <HardDrive
                aria-hidden="true"
                class="mt-1 shrink-0 text-accent"
                size={18}
              />
            {:else}
              <CloudDownload
                aria-hidden="true"
                class="mt-1 shrink-0 text-text-muted"
                size={18}
              />
            {/if}
            <div class="min-w-0 flex-1">
              <h2 class="wrap-break-word text-label font-semibold">{source.displayName}</h2>
              <p class="mt-1 text-meta text-text-muted">
                {source.cachedRecords.toLocaleString()}
                records /
                {source.kind === "local" ? "Local / Highest precedence" : source.enabled ? "Enabled" : "Disabled"}
              </p>
              {#if source.url}
                <p class="mt-1 break-all font-mono text-meta text-text-faint">{source.url}</p>
              {:else if source.repositoryPath}
                <p class="mt-1 break-all font-mono text-meta text-text-faint">{source.repositoryPath}</p>
              {/if}
            </div>
            {#if source.kind === "upstream"}
              <Switch
                aria-label={`Enable ${source.displayName}`}
                checked={source.enabled}
                disabled={busy}
                onCheckedChange={(checked) => void toggle(source, checked)}
              />
            {/if}
          </div>
          <div class="mt-3 flex flex-wrap items-center justify-between gap-2 pl-7">
            <div class="min-w-0 text-meta text-text-muted">
              <p
                class="break-all"
                title={source.revision ?? undefined}
              >
                {source.branch ? `${source.branch} / ` : ""}
                {source.revision ? `Commit ${source.revision.slice(0, 12)}` : "Awaiting first update"}
              </p>
              {#if source.kind === "upstream"}
                <p class="mt-1">Every {(source.pollIntervalSeconds ?? 3600) / 60} min</p>
                <p class="mt-1">Last check: {date(source.lastCheckedAt)}</p>
                {#if source.enabled && source.nextCheckAt}
                  <p class="mt-1">Next check: {date(source.nextCheckAt)}</p>
                {/if}
              {/if}
            </div>
            <div class="flex shrink-0 gap-0.5">
              {#if source.kind === "local"}
                <IconButton
                  disabled={busy}
                  label="Reload Local repository"
                  onclick={() => void refresh(source)}
                  size="sm"
                  ><RefreshCw
                    aria-hidden="true"
                    size={14}
                  /></IconButton
                >
                <IconButton
                  href="/mappings"
                  label="Open local mappings"
                  size="sm"
                  ><Pencil
                    aria-hidden="true"
                    size={14}
                  /></IconButton
                >
              {:else}
                <IconButton
                  disabled={busy || !source.enabled}
                  label={`Refresh ${source.displayName}`}
                  onclick={() => void refresh(source)}
                  size="sm"
                  ><CloudDownload
                    aria-hidden="true"
                    size={14}
                  /></IconButton
                >
                <IconButton
                  disabled={busy}
                  label={`Edit ${source.displayName}`}
                  onclick={() => edit(source)}
                  size="sm"
                  ><Pencil
                    aria-hidden="true"
                    size={14}
                  /></IconButton
                >
                <IconButton
                  disabled={busy}
                  label={`Remove ${source.displayName}`}
                  onclick={() => confirmRemove(source)}
                  size="sm"
                  ><Trash2
                    aria-hidden="true"
                    size={14}
                  /></IconButton
                >
              {/if}
            </div>
          </div>
          {#if source.lastError}
            <p
              class="mt-3 wrap-break-word pl-7 text-meta text-danger"
              role="alert"
            >
              {source.lastError}
              ({source.consecutiveFailures}
              failed checks)
            </p>
          {/if}
        </li>
      {/each}
    </ul>
  {/if}
</div>

<Dialog.Root bind:open={editorOpen}>
  <Dialog.Drawer
    escapeKeydownBehavior={busy ? "ignore" : "close"}
    interactOutsideBehavior={busy ? "ignore" : "close"}
    onCloseAutoFocus={restoreFocus}
  >
    <form
      class="flex h-full min-h-0 flex-col"
      onsubmit={save}
    >
      <div class="flex shrink-0 items-center justify-between border-b border-border-subtle px-5 py-4">
        <Dialog.Title class="text-title font-semibold"
          >{editing ? "Edit mapping source" : "Add mapping source"}</Dialog.Title
        >
        <IconButton
          disabled={busy}
          label="Close source editor"
          onclick={() => (editorOpen = false)}
          size="sm"
          ><X
            aria-hidden="true"
            size={16}
          /></IconButton
        >
      </div>
      <Dialog.Description class="sr-only">Upstream mapping source settings</Dialog.Description>
      <div class="min-h-0 flex-1 space-y-4 overflow-y-auto p-5">
        <div class="space-y-2">
          <label
            class="text-label"
            for="source-name"
            >Name</label
          ><TextInput
            class="w-full"
            id="source-name"
            maxlength={128}
            required
            bind:value={name}
          />
        </div>
        <div class="space-y-2">
          <label
            class="text-label"
            for="source-url"
            >Git repository URL</label
          ><TextInput
            class="w-full"
            id="source-url"
            maxlength={2048}
            required
            type="url"
            bind:value={url}
          />
        </div>
        <div class="space-y-2">
          <label
            class="text-label"
            for="source-branch"
            >Branch</label
          >
          <TextInput
            class="w-full font-mono"
            id="source-branch"
            maxlength={128}
            required
            spellcheck={false}
            bind:value={branch}
          />
        </div>
        <div class="space-y-2">
          <label
            class="text-label"
            for="source-interval"
            >Update interval (minutes)</label
          ><TextInput
            id="source-interval"
            max={10080}
            min={1}
            required
            step="any"
            type="number"
            bind:value={intervalMinutes}
          />
        </div>
        <div class="flex items-center justify-between gap-3">
          <label
            class="text-label"
            for="source-enabled"
            >Enabled</label
          ><Switch
            id="source-enabled"
            bind:checked={enabled}
          />
        </div>
        {#if editorError}
          <p
            class="wrap-break-word text-label text-danger"
            role="alert"
          >
            {editorError}
          </p>
        {/if}
      </div>
      <div class="flex shrink-0 justify-end gap-2 border-t border-border-subtle p-4">
        <Button
          disabled={busy}
          onclick={() => (editorOpen = false)}
          >Cancel</Button
        >
        <Button
          disabled={busy || !name.trim() || !url.trim() || !branch.trim()}
          type="submit"
          >{#if busy}
            <Spinner size={14} />
          {/if}
          Save</Button
        >
      </div>
    </form>
  </Dialog.Drawer>
</Dialog.Root>

<AlertDialog.Root bind:open={removeOpen}>
  <AlertDialog.Content>
    {#snippet header()}
      <AlertDialog.Title class="wrap-anywhere text-heading font-medium"
        >Remove {removeTarget?.displayName}?</AlertDialog.Title
      >
    {/snippet}
    <AlertDialog.Description class="text-label leading-6 text-text-muted"
      >Cached records from this source will be removed. Local decisions will be kept.</AlertDialog.Description
    >
    {#snippet footer()}
      <AlertDialog.Cancel disabled={busy}>Cancel</AlertDialog.Cancel
      ><AlertDialog.Action
        disabled={busy}
        onclick={() => void remove()}
        >Remove</AlertDialog.Action
      >
    {/snippet}
  </AlertDialog.Content>
</AlertDialog.Root>
