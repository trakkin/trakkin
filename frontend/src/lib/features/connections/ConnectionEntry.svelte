<script lang="ts">
import {
  CheckCircle2,
  ChevronDown,
  Copy,
  ExternalLink,
  KeyRound,
  Pencil,
  RefreshCw,
  Save,
  Server,
  Trash2,
  X,
} from "@lucide/svelte";
import { onDestroy, tick, untrack } from "svelte";
import { Button, Chip, IconButton, Panel, Select, Spinner } from "$lib";
import {
  type AdapterField,
  type AuthenticationStep,
  type Connection,
  type ConnectionInspection,
  type ConnectionSource,
  cancelConnectionAuthentication,
  continueConnectionAuthentication,
  discoverConnectionSources,
  inspectConnection,
  listConnectionSources,
  reconfigureConnection,
  renameConnection,
  startConnectionAuthentication,
} from "$lib/api/connections";
import type { Workspace } from "$lib/api/workspaces";
import { authenticationPolling, mergePrompt } from "./authenticationPolling";
import ConnectionSourceRow from "./ConnectionSourceRow.svelte";
import { authenticationInput, configurationInput, type FieldDrafts, inspectionDrafts } from "./fields";
import SchemaFields from "./SchemaFields.svelte";

let {
  connection,
  initialSources,
  workspace,
  expanded,
  renaming,
  onaction,
  onconfigured,
  onrenamed,
  onrefresh,
  onworkspaceupdated,
}: {
  connection: Connection;
  initialSources: ConnectionSource[];
  workspace?: Workspace;
  expanded: boolean;
  renaming: boolean;
  onaction: (action: "expand" | "rename" | "cancelRename" | "delete") => void;
  onconfigured: (connection: Connection) => void;
  onrenamed: (connection: Connection) => void;
  onrefresh: (signal: AbortSignal) => Promise<void>;
  onworkspaceupdated: (workspace: Workspace, sourceId: string) => void;
} = $props();

let sources = $state<ConnectionSource[]>([]);
let inspection = $state<ConnectionInspection>();
let authentication = $state<AuthenticationStep>();
let methodKey = $state("");
let configuration = $state<FieldDrafts>({});
let authenticationDrafts = $state<FieldDrafts>({});
let configurationMissing = $state<string[]>([]);
let authenticationMissing = $state<string[]>([]);
let loading = $state(false);
let saving = $state(false);
let authenticating = $state(false);
let discovering = $state(false);
let renameBusy = $state(false);
let renameDraft = $state("");
let renameInput = $state<HTMLInputElement>();
let copied = $state(false);
let error = $state("");
let renameSession = 0;
let configurationRevision = 0;
let authenticationRevision = 0;
let copyTimer: ReturnType<typeof setTimeout> | undefined;
const controller = new AbortController();
const polling = authenticationPolling(controller.signal, () => {
  void continueAuthentication(true);
});
let method = $derived(inspection?.authenticationMethods.find((candidate) => candidate.key === methodKey));
let accountName = $derived(authentication?.accounts[0]?.displayName ?? sources[0]?.accountDisplayName ?? "");
let setupPending = $derived(
  Boolean(
    inspection?.authenticationMethods.length &&
      !inspection.configuredSecretKeys.length &&
      authentication?.status !== "completed",
  ),
);
let statusLabel = $derived(
  connection.status === "unavailable"
    ? "Unavailable"
    : connection.status === "disabled"
      ? "Disabled"
      : accountName
        ? "Connected"
        : "Setup required",
);
let statusTone: "danger" | "neutral" | "success" | "warning" = $derived(
  connection.status === "unavailable"
    ? "danger"
    : connection.status === "disabled"
      ? "neutral"
      : accountName
        ? "success"
        : "warning",
);

$effect(() => {
  if (!inspection) sources = initialSources;
});
$effect(() => {
  if (expanded)
    untrack(() => {
      void load();
    });
});
$effect(() => {
  const editing = renaming;
  const session = ++renameSession;
  renameBusy = false;
  if (!editing) return;
  renameDraft = untrack(() => connection.displayName);
  void tick().then(() => {
    if (session === renameSession && !controller.signal.aborted) {
      renameInput?.focus();
      renameInput?.select();
    }
  });
});
onDestroy(() => {
  controller.abort();
  if (copyTimer !== undefined) clearTimeout(copyTimer);
});

function report(cause: unknown) {
  if (!controller.signal.aborted) error = cause instanceof Error ? cause.message : "The connection operation failed";
}

async function load() {
  if (inspection || loading) return;
  loading = true;
  error = "";
  const revision = configurationRevision;
  try {
    const [nextInspection, nextSources] = await Promise.all([
      inspectConnection(connection.id, controller.signal),
      listConnectionSources(connection.id, controller.signal),
    ]);
    if (controller.signal.aborted) return;
    inspection = nextInspection;
    sources = nextSources;
    if (revision === configurationRevision) configuration = inspectionDrafts(nextInspection);
    methodKey = nextInspection.authenticationMethods[0]?.key ?? "";
  } catch (cause) {
    report(cause);
  } finally {
    if (!controller.signal.aborted) loading = false;
  }
}

function updateConfiguration(key: string, value: string) {
  configurationRevision += 1;
  configuration = { ...configuration, [key]: value };
  configurationMissing = configurationMissing.filter((candidate) => candidate !== key);
}

function updateAuthentication(key: string, value: string) {
  authenticationRevision += 1;
  authenticationDrafts = { ...authenticationDrafts, [key]: value };
  authenticationMissing = authenticationMissing.filter((candidate) => candidate !== key);
}

function inputs(fields: AdapterField[]) {
  const result = authenticationInput(fields, authenticationDrafts);
  authenticationMissing = result.missing;
  return result.value;
}

function configurationInputs() {
  if (!inspection) return;
  const result = configurationInput(inspection, configuration);
  configurationMissing = result.missing;
  return result.value;
}

function invalidateAuthentication() {
  polling.clear();
  authentication = undefined;
  if (copyTimer !== undefined) clearTimeout(copyTimer);
  copyTimer = undefined;
  copied = false;
}

async function saveConfiguration() {
  if (!inspection || saving || authenticating) return;
  error = "";
  try {
    const input = configurationInputs();
    if (!input) return;
    const revision = configurationRevision;
    saving = true;
    const updated = await reconfigureConnection(connection.id, input, controller.signal);
    if (controller.signal.aborted) return;
    invalidateAuthentication();
    onconfigured(updated);
    const next = await inspectConnection(connection.id, controller.signal);
    if (controller.signal.aborted) return;
    inspection = next;
    if (revision === configurationRevision) configuration = inspectionDrafts(next);
  } catch (cause) {
    report(cause);
  } finally {
    if (!controller.signal.aborted) saving = false;
  }
}

async function saveRename() {
  if (renameBusy) return;
  const displayName = renameDraft.trim();
  if (!displayName || displayName === connection.displayName) {
    onaction("cancelRename");
    return;
  }
  const session = renameSession;
  renameBusy = true;
  error = "";
  try {
    const updated = await renameConnection(connection.id, displayName, controller.signal);
    if (controller.signal.aborted) return;
    onrenamed(updated);
    if (session === renameSession && renaming) onaction("cancelRename");
  } catch (cause) {
    if (session === renameSession && renaming) report(cause);
  } finally {
    if (!controller.signal.aborted && session === renameSession) renameBusy = false;
  }
}

async function startAuthentication() {
  if (!method || !inspection || authenticating || saving) return;
  error = "";
  try {
    const values = inputs(method.fields);
    if (!values) return;
    const input = inspection.fields.length ? configurationInputs() : undefined;
    if (inspection.fields.length && !input) return;
    const key = method.key;
    const configRevision = configurationRevision;
    const authRevision = authenticationRevision;
    authenticating = true;
    if (input) {
      const updated = await reconfigureConnection(connection.id, input, controller.signal);
      if (controller.signal.aborted) return;
      invalidateAuthentication();
      onconfigured(updated);
      inspection = {
        ...inspection,
        configuredSecretKeys: [
          ...new Set([...inspection.configuredSecretKeys, ...input.secretPatches.map((patch) => patch.key)]),
        ],
      };
      if (configRevision === configurationRevision) {
        configuration = Object.fromEntries(
          Object.entries(configuration).filter(
            ([key]) => !inspection?.fields.some((field) => field.key === key && field.secret),
          ),
        );
      }
    }
    const step = await startConnectionAuthentication(connection.id, key, values, controller.signal);
    if (controller.signal.aborted) return;
    if (authRevision === authenticationRevision) authenticationDrafts = {};
    await storeAuthentication(step);
  } catch (cause) {
    report(cause);
  } finally {
    if (!controller.signal.aborted) authenticating = false;
  }
}

async function continueAuthentication(automatic = false) {
  const current = authentication;
  if (!current || !["waiting", "inputRequired"].includes(current.status)) return;
  if (authenticating || saving) {
    if (automatic) polling.schedule(current);
    return;
  }
  if (!automatic) error = "";
  try {
    const values = automatic ? [] : inputs(current.prompt?.fields ?? []);
    if (!values) return;
    polling.clear();
    const revision = authenticationRevision;
    authenticating = true;
    const next = await continueConnectionAuthentication(connection.id, current.id, values, controller.signal);
    if (controller.signal.aborted) return;
    if (revision === authenticationRevision) authenticationDrafts = {};
    await storeAuthentication(mergePrompt(current, next));
  } catch (cause) {
    report(cause);
    if (authentication === current) polling.schedule(current);
  } finally {
    if (!controller.signal.aborted) authenticating = false;
  }
}

async function cancelAuthentication() {
  const current = authentication;
  if (!current || authenticating) return;
  polling.clear();
  authenticating = true;
  error = "";
  try {
    const next = await cancelConnectionAuthentication(connection.id, current.id, controller.signal);
    if (!controller.signal.aborted) authentication = next;
  } catch (cause) {
    report(cause);
    if (authentication === current) polling.schedule(current);
  } finally {
    if (!controller.signal.aborted) authenticating = false;
  }
}

async function storeAuthentication(step: AuthenticationStep) {
  authentication = step;
  polling.schedule(step);
  if (step.status === "completed") await discover();
}

async function discover() {
  if (discovering) return;
  discovering = true;
  error = "";
  try {
    const next = await discoverConnectionSources(connection.id, controller.signal);
    if (controller.signal.aborted) return;
    sources = next;
    await onrefresh(controller.signal);
  } catch (cause) {
    report(cause);
  } finally {
    if (!controller.signal.aborted) discovering = false;
  }
}

async function copyCode(code: string) {
  try {
    await navigator.clipboard.writeText(code);
    if (controller.signal.aborted) return;
    copied = true;
    if (copyTimer !== undefined) clearTimeout(copyTimer);
    copyTimer = setTimeout(() => {
      copied = false;
      copyTimer = undefined;
    }, 1500);
  } catch (cause) {
    report(cause);
  }
}
</script>

<Panel
  class="overflow-hidden"
  element="section"
>
  <header class="relative flex items-center gap-3 px-4 py-3.5 transition-colors hover:bg-white/3 sm:px-5">
    <button
      aria-expanded={expanded}
      aria-label={`${expanded ? "Collapse" : "Expand"} ${connection.displayName}`}
      class="absolute inset-0 cursor-pointer outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-accent/30"
      onclick={() => onaction("expand")}
      type="button"
    ></button>
    <span
      class="pointer-events-none relative flex size-9 shrink-0 items-center justify-center rounded-md border border-border-subtle bg-white/3 text-text-muted"
      ><Server
        aria-hidden="true"
        size={17}
      /></span
    >
    <span class="pointer-events-none relative min-w-0 flex-1">
      <span class="flex min-w-0 items-center gap-0.5">
        {#if renaming}
          <form
            class="pointer-events-auto flex min-w-0 max-w-sm flex-1 items-center gap-1"
            onsubmit={(event) => { event.preventDefault(); void saveRename(); }}
          >
            <input
              aria-label={`Rename ${connection.displayName}`}
              class="h-6 min-w-0 flex-1 border-b border-accent/45 bg-transparent text-label text-text-primary outline-none"
              disabled={renameBusy}
              required
              bind:this={renameInput}
              bind:value={renameDraft}
            >
            <IconButton
              disabled={renameBusy}
              label="Save connection name"
              size="sm"
              type="submit"
              ><CheckCircle2
                aria-hidden="true"
                size={11}
              /></IconButton
            >
            <IconButton
              label="Cancel renaming"
              onclick={() => onaction("cancelRename")}
              size="sm"
              type="button"
              ><X
                aria-hidden="true"
                size={11}
              /></IconButton
            >
          </form>
        {:else}
          <button
            class="pointer-events-auto truncate text-left text-label font-[510] text-text-primary"
            onclick={() => onaction("expand")}
            type="button"
          >
            {connection.displayName}
          </button>
          <IconButton
            class="pointer-events-auto opacity-40 hover:opacity-100"
            label={`Rename ${connection.displayName}`}
            onclick={() => onaction("rename")}
            size="sm"
            ><Pencil
              aria-hidden="true"
              size={11}
            /></IconButton
          >
        {/if}
      </span>
      <span class="mt-0.5 block truncate text-meta text-text-muted"
        >{connection.providerId}{accountName ? ` / ${accountName}` : ""}</span
      >
    </span>
    <Chip
      status
      tone={statusTone}
      >{statusLabel}</Chip
    >
    <IconButton
      class="pointer-events-auto relative"
      label={`${expanded ? "Collapse" : "Expand"} ${connection.displayName}`}
      onclick={() => onaction("expand")}
      size="sm"
      ><ChevronDown
        aria-hidden="true"
        class={expanded ? "rotate-180" : ""}
        size={16}
      /></IconButton
    >
    <IconButton
      class="pointer-events-auto relative"
      label={`Delete ${connection.displayName}`}
      onclick={() => onaction("delete")}
      size="sm"
      variant="danger"
      ><Trash2
        aria-hidden="true"
        size={13}
      /></IconButton
    >
  </header>
  {#if error}
    <div
      class="flex items-start justify-between gap-3 border-t border-border-subtle px-4 py-3"
      role="alert"
    >
      <p class="text-label text-danger">{error}</p>
      <IconButton
        label="Dismiss error"
        onclick={() => (error = "")}
        size="sm"
        variant="danger"
        ><X
          aria-hidden="true"
          size={15}
        /></IconButton
      >
    </div>
  {/if}
  <div hidden={!expanded}>
    {#if loading}
      <div
        class="flex min-h-28 items-center justify-center gap-2 border-t border-border-subtle text-label text-text-muted"
        role="status"
      >
        <Spinner size={15} />Loading setup...
      </div>
    {:else if !inspection}
      <div class="border-t border-border-subtle px-4 py-4">
        <Button
          onclick={load}
          size="sm"
          ><RefreshCw
            aria-hidden="true"
            size={13}
          />Retry setup</Button
        >
      </div>
    {:else}
      <section class="border-t border-border-subtle px-4 py-4 sm:px-5">
        <div class="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h3 class="text-label font-[510] text-text-primary">Configuration</h3>
            <p class="mt-0.5 text-meta text-text-muted">Provider settings and credential replacement</p>
          </div>
          {#if !setupPending}
            <Button
              disabled={saving || authenticating}
              onclick={saveConfiguration}
              size="sm"
              variant="ghost"
            >
              {#if saving}
                <Spinner size={13} />
              {:else}
                <Save
                  aria-hidden="true"
                  size={13}
                />
              {/if}
              Save
            </Button>
          {/if}
        </div>
        <div class="mt-4">
          <SchemaFields
            drafts={configuration}
            fields={inspection.fields}
            id={`${connection.id}-configuration`}
            missing={configurationMissing}
            onupdate={updateConfiguration}
            savedSecrets={inspection.configuredSecretKeys}
          />
        </div>
        {#if inspection.fields.length === 0}
          <p class="mt-3 text-label text-text-muted">This provider has no configurable fields.</p>
        {/if}
      </section>
      <section class="border-t border-border-subtle px-4 py-4 sm:px-5">
        <h3 class="text-label font-[510] text-text-primary">Authentication</h3>
        {#if authentication?.status === "completed"}
          <p class="mt-3 flex items-center gap-2 text-label text-text-secondary">
            <CheckCircle2
              aria-hidden="true"
              class="text-success"
              size={16}
            />Connected as {accountName || "provider account"}
          </p>
        {:else if authentication && ["failed", "cancelled", "expired"].includes(authentication.status)}
          <div class="mt-3 flex flex-wrap items-center justify-between gap-3">
            <p class="text-label text-text-muted">Authentication {authentication.status}.</p>
            <Button
              onclick={() => { authentication = undefined; authenticationDrafts = {}; authenticationMissing = []; }}
              size="sm"
              >Try again</Button
            >
          </div>
        {:else if authentication}
          <div class="mt-4 grid gap-4">
            {#if authentication.prompt?.message}
              <p class="text-label text-text-secondary">{authentication.prompt.message}</p>
            {/if}
            {#if authentication.prompt?.verificationUrl || authentication.prompt?.userCode}
              <div class="flex flex-wrap items-center gap-2">
                {#if authentication.prompt?.verificationUrl}
                  <a
                    class="inline-flex h-8 items-center gap-2 rounded-md border border-accent/35 bg-accent/10 px-3 text-label text-accent"
                    href={authentication.prompt.verificationUrl}
                    rel="noreferrer"
                    target="_blank"
                    ><ExternalLink
                      aria-hidden="true"
                      size={13}
                    />Open provider</a
                  >
                {/if}
                {#if authentication.prompt?.userCode}
                  <code
                    class="rounded-md border border-border-subtle bg-black/15 px-2.5 py-1.5 font-mono text-label text-text-primary"
                    >{authentication.prompt.userCode}</code
                  >
                  <Button
                    onclick={() => copyCode(authentication?.prompt?.userCode ?? "")}
                    size="sm"
                    variant="ghost"
                    >{#if copied}
                      <CheckCircle2
                        aria-hidden="true"
                        size={13}
                      />Copied
                    {:else}
                      <Copy
                        aria-hidden="true"
                        size={13}
                      />Copy
                    {/if}</Button
                  >
                {/if}
              </div>
            {/if}
            <SchemaFields
              drafts={authenticationDrafts}
              fields={authentication.prompt?.fields ?? []}
              id={`${connection.id}-authentication`}
              missing={authenticationMissing}
              onupdate={updateAuthentication}
            />
            <div class="flex gap-2">
              <Button
                disabled={authenticating}
                onclick={() => continueAuthentication()}
                size="sm"
                >{#if authenticating || authentication.status === "waiting"}
                  <Spinner size={13} />
                {/if}
                {authentication.prompt?.fields.length ? "Continue" : "Check sign-in"}</Button
              >
              <Button
                disabled={authenticating}
                onclick={cancelAuthentication}
                size="sm"
                variant="ghost"
                >Cancel</Button
              >
            </div>
          </div>
        {:else if inspection.authenticationMethods.length === 0}
          <p class="mt-3 text-label text-text-muted">This adapter does not require interactive authentication.</p>
        {:else if method}
          <div class="mt-4 grid gap-4">
            {#if inspection.authenticationMethods.length > 1}
              <Select
                ariaLabel="Authentication method"
                disabled={authenticating}
                items={inspection.authenticationMethods.map((method) => ({ value: method.key, label: method.displayName }))}
                onValueChange={(value) => { methodKey = value; authenticationRevision += 1; authenticationDrafts = {}; authenticationMissing = []; }}
                triggerClass="w-full sm:w-64"
                value={methodKey}
              />
            {/if}
            <SchemaFields
              drafts={authenticationDrafts}
              fields={method.fields}
              id={`${connection.id}-authentication`}
              missing={authenticationMissing}
              onupdate={updateAuthentication}
            />
            <Button
              class="w-fit"
              disabled={authenticating || saving}
              onclick={startAuthentication}
              size="sm"
              ><KeyRound
                aria-hidden="true"
                size={13}
              />Connect</Button
            >
          </div>
        {/if}
      </section>
      <section class="border-t border-border-subtle">
        <header class="flex flex-wrap items-center justify-between gap-3 px-4 py-3 sm:px-5">
          <div>
            <h3 class="text-label font-[510] text-text-primary">Sources</h3>
            <p class="mt-0.5 text-meta text-text-muted">{sources.length} discovered</p>
          </div>
          <Button
            disabled={discovering}
            onclick={discover}
            size="sm"
            variant="ghost"
            ><RefreshCw
              aria-hidden="true"
              class={discovering ? "animate-spin" : ""}
              size={13}
            />Discover</Button
          >
        </header>
        {#if sources.length === 0}
          <p class="border-t border-border-subtle px-4 py-5 text-label text-text-muted sm:px-5">
            No sources discovered.
          </p>
        {:else}
          <div class="divide-y divide-border-subtle border-t border-border-subtle">
            {#each sources as source (source.id)}
              <ConnectionSourceRow
                {onworkspaceupdated}
                {source}
                {workspace}
              />
            {/each}
          </div>
        {/if}
      </section>
    {/if}
  </div>
</Panel>
