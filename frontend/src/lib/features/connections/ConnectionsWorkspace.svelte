<script lang="ts">
import {
  CheckCircle2,
  ChevronDown,
  Copy,
  ExternalLink,
  KeyRound,
  Pencil,
  Plus,
  RefreshCw,
  Save,
  Server,
  Trash2,
  X,
} from "@lucide/svelte";
import { onDestroy, onMount, tick } from "svelte";
import { page } from "$app/state";
import {
  AlertDialog,
  Button,
  Checkbox,
  Chip,
  EmptyState,
  IconButton,
  PageHeader,
  Panel,
  Select,
  Spinner,
  TextInput,
} from "$lib";
import {
  type AdapterField,
  type AdapterSetting,
  type AdapterValue,
  type AdapterValueDisplay,
  type AuthenticationMethod,
  type AuthenticationStep,
  type Connection,
  type ConnectionDeletionImpact,
  type ConnectionInspection,
  type ConnectionSource,
  cancelConnectionAuthentication,
  continueConnectionAuthentication,
  createConnection,
  deleteConnection,
  discoverConnectionSources,
  getConnectionDeletionImpact,
  inspectConnection,
  listConnectionSources,
  listConnections,
  reconfigureConnection,
  renameConnection,
  retryAfterMilliseconds,
  startConnectionAuthentication,
} from "$lib/api/connections";
import { pollCatalog, waitForTask } from "$lib/api/operations";
import { listProviders, type Provider } from "$lib/api/providers";
import { attachSource, detachSource, listWorkspaces, type Workspace, type WorkspaceSource } from "$lib/api/workspaces";

const fixtureScenarios = [
  "hierarchical-catalog-assets-v1",
  "empty-targeted-receiver-v1",
  "edition-hierarchy-v1",
  "flat-catalog-revisions-v1",
  "adversarial-v1",
  "dual-connection-isolation-v1",
];
const scenarioItems = fixtureScenarios.map((value) => ({
  value,
  label: value.replace(/-v\d+$/, "").replaceAll("-", " "),
}));
const revisionItems = [
  { value: "v1", label: "Revision v1" },
  { value: "v2", label: "Revision v2" },
];
const instanceItems = [
  { value: "alpha", label: "Instance alpha" },
  { value: "beta", label: "Instance beta" },
];

let connections = $state<Connection[]>([]);
let workspaces = $state<Workspace[]>([]);
let providerItems = $state<{ value: string; label: string }[]>([]);
let selectedProviderId = $state("");
let selectedWorkspaceId = $state("");
let showComposer = $state(false);
let connectionName = $state("");
let fixtureScenario = $state(fixtureScenarios[0]);
let fixtureRevision = $state("v1");
let fixtureInstance = $state("alpha");
let fixtureToken = $state("fixture-secret");
let expandedConnectionId = $state("");
let editingConnectionId = $state("");
let renameDraft = $state("");
let renameInput = $state<HTMLInputElement>();
let pendingDelete = $state<Connection | null>(null);
let deletionImpact = $state<ConnectionDeletionImpact | null>(null);
let deleteOpen = $state(false);
let inspections = $state<Record<string, ConnectionInspection | undefined>>({});
let sources = $state<Record<string, ConnectionSource[]>>({});
let authentications = $state<Record<string, AuthenticationStep | undefined>>({});
let selectedMethods = $state<Record<string, string>>({});
let drafts = $state<Record<string, Record<string, string>>>({});
let configurationDrafts = $state<Record<string, Record<string, string>>>({});
let secretReplacementDrafts = $state<Record<string, Record<string, string>>>({});
let invalidFields = $state<Record<string, true>>({});
let pending = $state<Record<string, boolean>>({});
let copiedCodeConnectionId = $state("");
let errorMessage = $state("");
let loading = $state(true);
let directoryLoaded = $state(false);
let directorySequence = 0;
let composerSession = 0;
let renameSession = 0;
let deleteSession = 0;
const lifecycleController = new AbortController();
let copyTimer: ReturnType<typeof setTimeout> | undefined;

const pollTimers = new Map<string, ReturnType<typeof setTimeout>>();

let selectedWorkspace = $derived(workspaces.find((workspace) => workspace.id === selectedWorkspaceId));

onMount(() => {
  void loadDirectory(lifecycleController.signal);
});

onDestroy(() => {
  directorySequence += 1;
  lifecycleController.abort();
  for (const timer of pollTimers.values()) clearTimeout(timer);
  if (copyTimer) clearTimeout(copyTimer);
});

$effect(() => {
  fixtureToken =
    fixtureScenario === "dual-connection-isolation-v1" ? `fixture-secret-${fixtureInstance}` : "fixture-secret";
});

async function loadDirectory(signal?: AbortSignal) {
  const sequence = ++directorySequence;
  loading = true;
  errorMessage = "";
  try {
    const [nextConnections, nextWorkspaces, providers] = await Promise.all([
      listConnections(signal),
      listWorkspaces(signal),
      listProviders(signal),
    ]);
    if (sequence !== directorySequence) return;
    const sourceResults = await Promise.allSettled(
      nextConnections.map(
        async (connection) => [connection.id, await listConnectionSources(connection.id, signal)] as const,
      ),
    );
    if (sequence !== directorySequence) return;
    connections = nextConnections;
    sources = {
      ...sources,
      ...Object.fromEntries(sourceResults.flatMap((result) => (result.status === "fulfilled" ? [result.value] : []))),
    };
    workspaces = nextWorkspaces;
    providerItems = installedProviderItems(providers);
    if (!providerItems.some((provider) => provider.value === selectedProviderId)) {
      selectedProviderId = providerItems[0]?.value ?? "";
    }
    selectedWorkspaceId = page.url.searchParams.get("workspace") ?? workspaces[0]?.id ?? "";
    changeComposerOpen(connections.length === 0);
    directoryLoaded = true;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (sequence === directorySequence) errorMessage = messageFor(error, "Connections could not be loaded");
  } finally {
    if (sequence === directorySequence) loading = false;
  }
}

function retryDirectory(): void {
  void loadDirectory(lifecycleController.signal);
}

function installedProviderItems(providers: Provider[]) {
  return providers.map((provider) => ({
    value: provider.providerId,
    label: `${provider.name ?? provider.providerId} ${provider.version}`,
  }));
}

async function addConnection() {
  const session = composerSession;
  const displayName = connectionName.trim();
  if (!displayName || !selectedProviderId) return;
  const fixtureSelected = selectedProviderId === "dev.trakkin.fixture";
  await runPending(
    createPendingKey(session),
    async () => {
      const connection = await createConnection(
        {
          providerId: selectedProviderId,
          displayName,
          settings: fixtureSelected
            ? [
                textSetting("scenario", fixtureScenario),
                textSetting("revision", fixtureRevision),
                textSetting("fault", "none"),
                textSetting("instance", fixtureInstance),
              ]
            : [],
          secrets: fixtureSelected
            ? [{ key: "fixture_token", value: Array.from(new TextEncoder().encode(fixtureToken)) }]
            : [],
        },
        lifecycleController.signal,
      );
      directorySequence += 1;
      connections = [...connections, connection];
      if (!ownsComposerSession(session)) return;
      connectionName = "";
      composerSession += 1;
      showComposer = false;
      expandedConnectionId = connection.id;
      await loadConnection(connection.id, true);
    },
    false,
    () => ownsComposerSession(session),
  );
}

function changeComposerOpen(open: boolean): void {
  composerSession += 1;
  showComposer = open;
}

function ownsComposerSession(session: number): boolean {
  return lifecycleActive() && session === composerSession && showComposer;
}

function createPendingKey(session = composerSession): string {
  return `create:${session}`;
}

async function toggleConnection(connectionId: string) {
  if (expandedConnectionId === connectionId) {
    expandedConnectionId = "";
    return;
  }
  expandedConnectionId = connectionId;
  await loadConnection(connectionId);
}

async function loadConnection(connectionId: string, force = false) {
  if (!force && inspections[connectionId] && sources[connectionId]) return;
  await runPending(`load:${connectionId}`, async () => {
    const [inspection, connectionSources] = await Promise.all([
      inspectConnection(connectionId, lifecycleController.signal),
      listConnectionSources(connectionId, lifecycleController.signal),
    ]);
    if (!lifecycleActive()) return;
    inspections = { ...inspections, [connectionId]: inspection };
    sources = { ...sources, [connectionId]: connectionSources };
    configurationDrafts = {
      ...configurationDrafts,
      [connectionId]: Object.fromEntries(
        inspection.settings.map((setting) => [setting.key, valueToDraft(setting.value) ?? ""]),
      ),
    };
    selectedMethods = {
      ...selectedMethods,
      [connectionId]: selectedMethods[connectionId] ?? inspection.authenticationMethods[0]?.key ?? "",
    };
  });
}

async function startRename(connection: Connection) {
  const session = ++renameSession;
  editingConnectionId = connection.id;
  renameDraft = connection.displayName;
  await tick();
  if (!ownsRenameSession(session, connection.id)) return;
  renameInput?.focus();
  renameInput?.select();
}

function cancelRename() {
  renameSession += 1;
  editingConnectionId = "";
  renameDraft = "";
}

async function saveRename(connection: Connection) {
  const session = renameSession;
  const connectionId = connection.id;
  const displayName = renameDraft.trim();
  if (!displayName || displayName === connection.displayName) {
    cancelRename();
    return;
  }
  await runPending(
    renamePendingKey(connection.id, session),
    async () => {
      const renamed = await renameConnection(connection.id, displayName, lifecycleController.signal);
      directorySequence += 1;
      connections = connections.map((candidate) => (candidate.id === renamed.id ? renamed : candidate));
      if (ownsRenameSession(session, connectionId)) cancelRename();
    },
    false,
    () => ownsRenameSession(session, connectionId),
  );
}

async function saveConfiguration(connection: Connection) {
  const inspection = inspections[connection.id];
  if (!inspection) return;
  const input = configurationInput(connection.id, inspection);
  if (!input) return;
  await runPending(`configuration:${connection.id}`, async () => {
    const updated = await reconfigureConnection(connection.id, input, lifecycleController.signal);
    if (!lifecycleActive()) return;
    connections = connections.map((candidate) => (candidate.id === updated.id ? updated : candidate));
    secretReplacementDrafts = { ...secretReplacementDrafts, [connection.id]: {} };
    await loadConnection(connection.id, true);
  });
}

function ownsRenameSession(session: number, connectionId: string): boolean {
  return lifecycleActive() && session === renameSession && editingConnectionId === connectionId;
}

function renamePendingKey(connectionId: string, session = renameSession): string {
  return `rename:${connectionId}:${session}`;
}

async function requestDelete(connection: Connection) {
  const session = ++deleteSession;
  pendingDelete = connection;
  deletionImpact = null;
  deleteOpen = true;
  await runPending(
    deleteImpactPendingKey(connection.id, session),
    async () => {
      const impact = await getConnectionDeletionImpact(connection.id, lifecycleController.signal);
      if (ownsDeleteSession(session, connection.id)) deletionImpact = impact;
    },
    false,
    () => ownsDeleteSession(session, connection.id),
  );
}

async function confirmDelete() {
  const connection = pendingDelete;
  if (!connection) return;
  const session = deleteSession;
  await runPending(
    deletePendingKey(connection.id, session),
    async () => {
      await deleteConnection(connection.id, lifecycleController.signal);
      directorySequence += 1;
      clearPolling(connection.id);
      connections = connections.filter((candidate) => candidate.id !== connection.id);
      if (expandedConnectionId === connection.id) expandedConnectionId = "";
      if (ownsDeleteSession(session, connection.id)) closeDeleteSession();
    },
    false,
    () => ownsDeleteSession(session, connection.id),
  );
}

function changeDeleteOpen(open: boolean): void {
  if (open) {
    deleteOpen = true;
    return;
  }
  closeDeleteSession();
}

function closeDeleteSession(): void {
  deleteSession += 1;
  deleteOpen = false;
  pendingDelete = null;
  deletionImpact = null;
}

function ownsDeleteSession(session: number, connectionId: string): boolean {
  return lifecycleActive() && session === deleteSession && pendingDelete?.id === connectionId;
}

function deleteImpactPendingKey(connectionId: string, session = deleteSession): string {
  return `delete-impact:${connectionId}:${session}`;
}

function deletePendingKey(connectionId: string, session = deleteSession): string {
  return `delete:${connectionId}:${session}`;
}

function methodFor(connectionId: string): AuthenticationMethod | undefined {
  const methodKey = selectedMethods[connectionId];
  return inspections[connectionId]?.authenticationMethods.find((method) => method.key === methodKey);
}

function selectMethod(connectionId: string, method: string) {
  selectedMethods = { ...selectedMethods, [connectionId]: method };
  drafts = { ...drafts, [connectionId]: {} };
}

async function startAuthentication(connection: Connection) {
  const method = methodFor(connection.id);
  const inspection = inspections[connection.id];
  if (!method || !inspection) return;
  const values = inputsFor(connection.id, method.fields);
  if (!values) return;
  const configuration = inspection.fields.length > 0 ? configurationInput(connection.id, inspection) : undefined;
  if (inspection.fields.length > 0 && !configuration) return;
  await runPending(`authentication:${connection.id}`, async () => {
    if (configuration) {
      const updated = await reconfigureConnection(connection.id, configuration, lifecycleController.signal);
      if (!lifecycleActive()) return;
      connections = connections.map((candidate) => (candidate.id === updated.id ? updated : candidate));
      secretReplacementDrafts = { ...secretReplacementDrafts, [connection.id]: {} };
    }
    const step = await startConnectionAuthentication(connection.id, method.key, values, lifecycleController.signal);
    if (!lifecycleActive()) return;
    drafts = { ...drafts, [connection.id]: {} };
    await storeAuthentication(connection, step);
  });
}

async function continueAuthentication(connection: Connection, automatic = false) {
  const current = authentications[connection.id];
  if (!current || !["waiting", "inputRequired"].includes(current.status)) return;
  const fields = current.prompt?.fields ?? [];
  const values = automatic ? [] : inputsFor(connection.id, fields);
  if (!values) return;
  await runPending(
    `authentication:${connection.id}`,
    async () => {
      const next = await continueConnectionAuthentication(
        connection.id,
        current.id,
        values,
        lifecycleController.signal,
      );
      if (!lifecycleActive()) return;
      drafts = { ...drafts, [connection.id]: {} };
      await storeAuthentication(connection, mergePrompt(current, next));
    },
    automatic,
  );
}

async function cancelAuthentication(connection: Connection) {
  const current = authentications[connection.id];
  if (!current) return;
  await runPending(`authentication:${connection.id}`, async () => {
    const cancelled = await cancelConnectionAuthentication(connection.id, current.id, lifecycleController.signal);
    if (!lifecycleActive()) return;
    clearPolling(connection.id);
    authentications = { ...authentications, [connection.id]: cancelled };
  });
}

async function storeAuthentication(connection: Connection, step: AuthenticationStep) {
  if (!lifecycleActive()) return;
  authentications = { ...authentications, [connection.id]: step };
  if (step.status === "completed") {
    clearPolling(connection.id);
    await discover(connection, true);
    return;
  }
  schedulePolling(connection, step);
}

function schedulePolling(connection: Connection, step: AuthenticationStep) {
  clearPolling(connection.id);
  if (!lifecycleActive() || step.status !== "waiting" || (step.prompt?.fields.length ?? 0) > 0) return;
  pollTimers.set(
    connection.id,
    setTimeout(() => {
      if (lifecycleActive()) void continueAuthentication(connection, true);
    }, retryAfterMilliseconds(step)),
  );
}

function clearPolling(connectionId: string) {
  const timer = pollTimers.get(connectionId);
  if (timer) clearTimeout(timer);
  pollTimers.delete(connectionId);
}

function mergePrompt(current: AuthenticationStep, next: AuthenticationStep): AuthenticationStep {
  return next.status === "waiting" && !next.prompt ? { ...next, prompt: current.prompt } : next;
}

async function discover(connection: Connection, quiet = false) {
  const sequence = ++directorySequence;
  await runPending(
    `discover:${connection.id}`,
    async () => {
      const discovered = await discoverConnectionSources(connection.id, lifecycleController.signal);
      if (!ownsDirectory(sequence)) return;
      sources = { ...sources, [connection.id]: discovered };
      const nextConnections = await listConnections(lifecycleController.signal);
      if (!ownsDirectory(sequence)) return;
      connections = nextConnections;
    },
    quiet,
    () => ownsDirectory(sequence),
  );
}

function ownsDirectory(sequence: number): boolean {
  return lifecycleActive() && sequence === directorySequence;
}

function attachmentFor(sourceId: string): WorkspaceSource | undefined {
  return selectedWorkspace?.sources.find((attachment) => attachment.sourceId === sourceId);
}

async function changeAttachment(source: ConnectionSource, enabled: boolean) {
  const workspace = selectedWorkspace;
  if (!workspace) return;
  await runPending(`source:${source.id}`, async () => {
    const updated = enabled ? await attachSource(workspace.id, source.id) : await detachSource(workspace.id, source.id);
    workspaces = workspaces.map((candidate) => (candidate.id === updated.id ? updated : candidate));
    window.dispatchEvent(new Event("trakkin:workspaces-changed"));
  });
}

async function scanCatalog(source: ConnectionSource) {
  const workspace = selectedWorkspace;
  if (!workspace || !attachmentFor(source.id)) return;
  await runPending(
    `scan:${workspace.id}:${source.id}`,
    async () => {
      const signal = lifecycleController.signal;
      const result = await waitForTask(workspace.id, await pollCatalog(workspace.id, source.id, signal), signal);
      if (result.status !== "succeeded") {
        throw new Error(
          result.status === "cancelled"
            ? "The catalog scan was cancelled."
            : "The catalog scan did not finish. Check the source connection and try again.",
        );
      }
    },
    false,
    () => lifecycleActive() && selectedWorkspaceId === workspace.id,
  );
}

function inputsFor(connectionId: string, fields: AdapterField[]): AdapterSetting[] | undefined {
  if (
    markMissingFields(
      connectionId,
      fields,
      "authentication",
      (field) => field.required && !fieldValue(connectionId, field, "authentication").trim(),
    )
  )
    return;
  try {
    return fields.flatMap((field) => {
      const raw = fieldValue(connectionId, field);
      return raw === "" && !field.required ? [] : [{ key: field.key, value: adapterValue(field, raw) }];
    });
  } catch (error) {
    errorMessage = messageFor(error, "Authentication input is invalid");
  }
}

function configurationInput(connectionId: string, inspection: ConnectionInspection) {
  const configuredSecrets = new Set(inspection.configuredSecretKeys);
  if (
    markMissingFields(connectionId, inspection.fields, "configuration", (field) => {
      if (!field.required) return false;
      const raw = fieldValue(connectionId, field, "configuration").trim();
      return field.secret ? !raw && !configuredSecrets.has(field.key) : !raw;
    })
  )
    return;
  try {
    return {
      settings: inspection.fields.flatMap((field) => {
        if (field.secret) return [];
        const raw = fieldValue(connectionId, field, "configuration");
        return raw === "" && !field.required ? [] : [{ key: field.key, value: adapterValue(field, raw) }];
      }),
      secretPatches: inspection.fields.flatMap((field) => {
        if (!field.secret) return [];
        const raw = fieldValue(connectionId, field, "configuration");
        return raw === "" ? [] : [{ key: field.key, value: Array.from(new TextEncoder().encode(raw)) }];
      }),
    };
  } catch (error) {
    errorMessage = messageFor(error, "Connection configuration is invalid");
  }
}

function adapterValue(field: AdapterField, raw: string): AdapterValue {
  switch (field.valueKind) {
    case "integer": {
      const value = Number(raw);
      if (!Number.isSafeInteger(value)) throw new Error(`${field.displayName} must be an integer`);
      return { kind: "integer", value };
    }
    case "timestamp": {
      const value = Number(raw);
      if (!Number.isSafeInteger(value)) throw new Error(`${field.displayName} must be an integer`);
      return { kind: "timestampMilliseconds", value };
    }
    case "boolean":
      return { kind: "boolean", value: raw === "true" };
    case "bytes":
      return { kind: "bytes", value: Array.from(new TextEncoder().encode(raw)) };
    case "decimal":
      return { kind: "decimal", value: raw };
    default:
      return { kind: "text", value: raw };
  }
}

function textSetting(key: string, value: string): AdapterSetting {
  return { key, value: { kind: "text", value } };
}

type DraftScope = "authentication" | "configuration";

function updateDraft(connectionId: string, key: string, value: string, scope: DraftScope) {
  const invalidKey = fieldValidationKey(connectionId, key, scope);
  if (invalidFields[invalidKey]) {
    const nextInvalidFields = { ...invalidFields };
    delete nextInvalidFields[invalidKey];
    invalidFields = nextInvalidFields;
  }
  if (scope === "authentication") {
    drafts = { ...drafts, [connectionId]: { ...drafts[connectionId], [key]: value } };
  } else if (inspections[connectionId]?.fields.find((field) => field.key === key)?.secret) {
    secretReplacementDrafts = {
      ...secretReplacementDrafts,
      [connectionId]: { ...secretReplacementDrafts[connectionId], [key]: value },
    };
  } else {
    configurationDrafts = {
      ...configurationDrafts,
      [connectionId]: { ...configurationDrafts[connectionId], [key]: value },
    };
  }
}

function markMissingFields(
  connectionId: string,
  fields: AdapterField[],
  scope: DraftScope,
  isMissing: (field: AdapterField) => boolean,
): boolean {
  const scopePrefix = `${connectionId}:${scope}:`;
  const retainedFields = Object.entries(invalidFields).filter(([key]) => !key.startsWith(scopePrefix));
  const missingFields = fields.filter(isMissing);
  invalidFields = Object.fromEntries([
    ...retainedFields,
    ...missingFields.map((field) => [fieldValidationKey(connectionId, field.key, scope), true] as const),
  ]);
  return missingFields.length > 0;
}

function fieldValidationKey(connectionId: string, fieldKey: string, scope: DraftScope): string {
  return `${connectionId}:${scope}:${fieldKey}`;
}

function fieldInvalid(connectionId: string, fieldKey: string, scope: DraftScope): boolean {
  return invalidFields[fieldValidationKey(connectionId, fieldKey, scope)] ?? false;
}

function fieldValue(connectionId: string, field: AdapterField, scope: DraftScope = "authentication"): string {
  if (scope === "authentication") {
    return drafts[connectionId]?.[field.key] ?? valueToDraft(field.defaultValue) ?? "";
  }
  if (field.secret) return secretReplacementDrafts[connectionId]?.[field.key] ?? "";
  return configurationDrafts[connectionId]?.[field.key] ?? valueToDraft(field.defaultValue) ?? "";
}

function valueToDraft(value: AdapterValue | AdapterValueDisplay | null | undefined): string | undefined {
  if (!value) return;
  if (value.kind === "bytes") return new TextDecoder().decode(Uint8Array.from(value.value));
  return String(value.value);
}

function fieldPlaceholder(connectionId: string, field: AdapterField, scope: DraftScope): string | undefined {
  if (fieldSecretSaved(connectionId, field, scope)) return "Enter replacement";
  return field.placeholder || undefined;
}

function fieldSecretSaved(connectionId: string, field: AdapterField, scope: DraftScope): boolean {
  return (
    scope === "configuration" &&
    field.secret &&
    (inspections[connectionId]?.configuredSecretKeys.includes(field.key) ?? false)
  );
}

function fieldItems(field: AdapterField) {
  if (field.options.length > 0) {
    return field.options.flatMap((option) => {
      const value = valueToDraft(option.value);
      return value === undefined ? [] : [{ value, label: option.displayName }];
    });
  }
  return field.valueKind === "boolean"
    ? [
        { value: "true", label: "Yes" },
        { value: "false", label: "No" },
      ]
    : [];
}

function methodItems(connectionId: string) {
  return (inspections[connectionId]?.authenticationMethods ?? []).map((method) => ({
    value: method.key,
    label: method.displayName,
  }));
}

function authenticationSetupPending(connectionId: string, inspection: ConnectionInspection): boolean {
  return (
    inspection.authenticationMethods.length > 0 &&
    inspection.configuredSecretKeys.length === 0 &&
    authentications[connectionId]?.status !== "completed"
  );
}

function accountName(connectionId: string): string {
  return (
    authentications[connectionId]?.accounts[0]?.displayName ?? sources[connectionId]?.[0]?.accountDisplayName ?? ""
  );
}

function statusLabel(connection: Connection): string {
  if (connection.status === "unavailable") return "Unavailable";
  if (connection.status === "disabled") return "Disabled";
  return accountName(connection.id) ? "Connected" : "Setup required";
}

function statusTone(connection: Connection): "success" | "warning" | "danger" | "neutral" {
  if (connection.status === "unavailable") return "danger";
  if (connection.status === "disabled") return "neutral";
  return accountName(connection.id) ? "success" : "warning";
}

function sourceKind(source: ConnectionSource): string {
  return source.kind.name.replaceAll("_", " ");
}

async function copyCode(connectionId: string, code: string) {
  await navigator.clipboard.writeText(code);
  if (!lifecycleActive()) return;
  copiedCodeConnectionId = connectionId;
  if (copyTimer) clearTimeout(copyTimer);
  copyTimer = setTimeout(() => {
    if (lifecycleActive() && copiedCodeConnectionId === connectionId) copiedCodeConnectionId = "";
    copyTimer = undefined;
  }, 1_500);
}

function lifecycleActive(): boolean {
  return !lifecycleController.signal.aborted;
}

async function runPending(key: string, operation: () => Promise<void>, quiet = false, owns = lifecycleActive) {
  if (pending[key]) return;
  pending = { ...pending, [key]: true };
  if (!quiet) errorMessage = "";
  try {
    await operation();
  } catch (error) {
    if (!quiet && owns()) errorMessage = messageFor(error, "The connection operation failed");
  } finally {
    if (lifecycleActive()) pending = { ...pending, [key]: false };
  }
}

function messageFor(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}
</script>

{#snippet configurationFields(connectionId: string, fields: AdapterField[], scope: DraftScope)}
  {#if fields.length > 0}
    <div class="grid gap-3 sm:grid-cols-2">
      {#each fields as field (field.key)}
        <label
          class="grid min-w-0 gap-1.5 text-label text-text-secondary"
          for={`${connectionId}-${field.key}`}
        >
          <span class="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
            <span
              >{field.displayName}
              {#if field.required}
                <span class="text-danger"> *</span>
              {/if}</span
            >
            {#if fieldSecretSaved(connectionId, field, scope)}
              <span class="inline-flex items-center gap-1 text-meta font-[520] text-success">
                <KeyRound
                  aria-hidden="true"
                  size={11}
                />Secret saved
              </span>
            {/if}
          </span>
          {#if fieldItems(field).length > 0}
            <Select
              ariaInvalid={fieldInvalid(connectionId, field.key, scope)}
              ariaLabel={field.displayName}
              items={fieldItems(field)}
              onValueChange={(value) => updateDraft(connectionId, field.key, value, scope)}
              placeholder={field.placeholder || `Select ${field.displayName}`}
              triggerClass="w-full min-w-0"
              value={fieldValue(connectionId, field, scope)}
            />
          {:else}
            <TextInput
              aria-invalid={fieldInvalid(connectionId, field.key, scope)}
              aria-label={field.displayName}
              autocomplete={field.secret ? "off" : undefined}
              class="w-full"
              id={`${connectionId}-${field.key}`}
              inputmode={field.valueKind === "integer" || field.valueKind === "decimal" ? "numeric" : undefined}
              oninput={(event) => updateDraft(connectionId, field.key, event.currentTarget.value, scope)}
              placeholder={fieldPlaceholder(connectionId, field, scope)}
              required={field.required}
              type={field.secret ? "password" : field.valueKind === "integer" ? "number" : "text"}
              value={fieldValue(connectionId, field, scope)}
            />
          {/if}
        </label>
      {/each}
    </div>
  {/if}
{/snippet}

<div class="flex h-full min-h-0 flex-col">
  <PageHeader title="Connections">
    {#snippet actions()}
      {#if directoryLoaded}
        <Button
          aria-expanded={showComposer}
          onclick={() => changeComposerOpen(!showComposer)}
          size="sm"
          variant={showComposer ? "ghost" : "secondary"}
        >
          {#if showComposer}
            <X
              aria-hidden="true"
              size={14}
            />Close
          {:else}
            <Plus
              aria-hidden="true"
              size={14}
            />New connection
          {/if}
        </Button>
      {/if}
    {/snippet}
  </PageHeader>

  <div class="min-h-0 flex-1 overflow-auto">
    <div class="mx-auto grid w-full max-w-5xl gap-4 p-4 sm:p-6">
      {#if directoryLoaded}
        <div class="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p class="text-label text-text-muted">
              {connections.length} {connections.length === 1 ? "connection" : "connections"}
            </p>
            {#if selectedWorkspace}
              <p class="mt-0.5 text-meta text-text-disabled">Attaching sources to {selectedWorkspace.name}</p>
            {/if}
          </div>
        </div>
      {/if}

      {#if errorMessage && !directoryLoaded}
        <Panel
          class="grid min-h-48 place-items-center px-4 py-6 text-center text-label text-danger"
          role="alert"
          tone="danger"
        >
          <div>
            <p>{errorMessage}</p>
            <Button
              class="mt-4"
              onclick={retryDirectory}
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
      {:else if errorMessage}
        <Panel
          class="flex items-start justify-between gap-3 px-4 py-3"
          role="alert"
          tone="danger"
        >
          <p class="text-label text-danger">{errorMessage}</p>
          <IconButton
            label="Dismiss error"
            onclick={() => (errorMessage = "")}
            size="sm"
            variant="danger"
            ><X
              aria-hidden="true"
              size={15}
            /></IconButton
          >
        </Panel>
      {/if}

      {#if directoryLoaded && showComposer}
        <Panel
          class="p-4 sm:p-5"
          tone="accent"
        >
          <form
            class="grid gap-4 lg:grid-cols-2"
            onsubmit={(event) => { event.preventDefault(); void addConnection(); }}
          >
            <div class="grid gap-1.5 text-label text-text-secondary">
              <span>Provider</span>
              <Select
                ariaLabel="Provider"
                disabled={providerItems.length === 0}
                items={providerItems}
                onValueChange={(value) => (selectedProviderId = value)}
                triggerClass="w-full"
                value={selectedProviderId}
              />
            </div>
            <label
              class="grid gap-1.5 text-label text-text-secondary"
              for="connection-name"
            >
              Connection name<TextInput
                aria-label="Connection name"
                id="connection-name"
                placeholder="Personal connection"
                required
                bind:value={connectionName}
              /></label
            >
            {#if providerItems.length === 0}
              <p
                class="text-label text-warning lg:col-span-2"
                role="status"
              >
                Install and activate a provider before creating a connection.
              </p>
            {/if}
            {#if selectedProviderId === "dev.trakkin.fixture"}
              <div class="grid gap-1.5 text-label text-text-secondary">
                <span>Fixture scenario</span>
                <Select
                  ariaLabel="Fixture scenario"
                  items={scenarioItems}
                  onValueChange={(value) => (fixtureScenario = value)}
                  triggerClass="w-full"
                  value={fixtureScenario}
                />
              </div>
              <div class="grid gap-1.5 text-label text-text-secondary">
                <span>Revision</span>
                <Select
                  ariaLabel="Fixture revision"
                  items={revisionItems}
                  onValueChange={(value) => (fixtureRevision = value)}
                  triggerClass="w-full"
                  value={fixtureRevision}
                />
              </div>
              <div class="grid gap-1.5 text-label text-text-secondary">
                <span>Instance</span>
                <Select
                  ariaLabel="Fixture instance"
                  items={instanceItems}
                  onValueChange={(value) => (fixtureInstance = value)}
                  triggerClass="w-full"
                  value={fixtureInstance}
                />
              </div>
              <label
                class="grid gap-1.5 text-label text-text-secondary lg:col-span-2"
                for="fixture-token"
              >
                Fixture token<TextInput
                  aria-label="Fixture token"
                  autocomplete="off"
                  id="fixture-token"
                  required
                  type="password"
                  bind:value={fixtureToken}
                /></label
              >
            {/if}
            <div class="flex justify-end lg:col-span-2">
              <Button
                disabled={!selectedProviderId || !connectionName.trim() || (selectedProviderId === "dev.trakkin.fixture" && !fixtureToken) || pending[createPendingKey()]}
                type="submit"
                variant="primary"
              >
                {#if pending[createPendingKey()]}
                  <Spinner size={15} />
                {:else}
                  <Plus
                    aria-hidden="true"
                    size={15}
                  />
                {/if}
                Create connection
              </Button>
            </div>
          </form>
        </Panel>
      {/if}

      {#if loading && !directoryLoaded}
        <div
          class="flex min-h-48 items-center justify-center gap-2 text-label text-text-muted"
          role="status"
        >
          <Spinner size={16} />Loading connections...
        </div>
      {:else if directoryLoaded && connections.length === 0 && !showComposer}
        <EmptyState
          icon={Server}
          title="No connections"
        >
          {#snippet actions()}
            <Button
              onclick={() => changeComposerOpen(true)}
              size="sm"
              ><Plus
                aria-hidden="true"
                size={14}
              />New connection</Button
            >
          {/snippet}
        </EmptyState>
      {:else if directoryLoaded}
        <div class="grid gap-3">
          {#each connections as connection (connection.id)}
            {@const inspection = inspections[connection.id]}
            {@const connectionSources = sources[connection.id] ?? []}
            {@const authentication = authentications[connection.id]}
            {@const selectedMethod = methodFor(connection.id)}
            <Panel
              class="overflow-hidden"
              element="section"
            >
              <header class="relative flex items-center gap-3 px-4 py-3.5 transition-colors hover:bg-white/3 sm:px-5">
                <button
                  aria-expanded={expandedConnectionId === connection.id}
                  aria-label={`${expandedConnectionId === connection.id ? "Collapse" : "Expand"} ${connection.displayName}`}
                  class="absolute inset-0 cursor-pointer outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-accent/30"
                  onclick={() => toggleConnection(connection.id)}
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
                    {#if editingConnectionId === connection.id}
                      <form
                        class="pointer-events-auto flex min-w-0 max-w-sm flex-1 items-center gap-1"
                        onsubmit={(event) => { event.preventDefault(); void saveRename(connection); }}
                      >
                        <input
                          aria-label={`Rename ${connection.displayName}`}
                          class="h-6 min-w-0 flex-1 border-b border-accent/45 bg-transparent text-label text-text-primary outline-none"
                          disabled={pending[renamePendingKey(connection.id)]}
                          required
                          bind:this={renameInput}
                          bind:value={renameDraft}
                        >
                        <IconButton
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
                          onclick={cancelRename}
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
                        onclick={() => toggleConnection(connection.id)}
                        type="button"
                      >
                        {connection.displayName}
                      </button>
                      <IconButton
                        class="pointer-events-auto opacity-40 hover:opacity-100"
                        label={`Rename ${connection.displayName}`}
                        onclick={() => startRename(connection)}
                        size="sm"
                        ><Pencil
                          aria-hidden="true"
                          size={11}
                        /></IconButton
                      >
                    {/if}
                  </span>
                  <span class="mt-0.5 block truncate text-meta text-text-muted"
                    >{connection.providerId}{accountName(connection.id) ? ` / ${accountName(connection.id)}` : ""}</span
                  >
                </span>
                <Chip
                  status
                  tone={statusTone(connection)}
                  >{statusLabel(connection)}</Chip
                >
                <IconButton
                  class="pointer-events-auto relative"
                  label={`${expandedConnectionId === connection.id ? "Collapse" : "Expand"} ${connection.displayName}`}
                  onclick={() => toggleConnection(connection.id)}
                  size="sm"
                  ><ChevronDown
                    aria-hidden="true"
                    class={expandedConnectionId === connection.id ? "rotate-180" : ""}
                    size={16}
                  /></IconButton
                >
                <IconButton
                  class="pointer-events-auto relative"
                  label={`Delete ${connection.displayName}`}
                  onclick={() => requestDelete(connection)}
                  size="sm"
                  variant="danger"
                  ><Trash2
                    aria-hidden="true"
                    size={13}
                  /></IconButton
                >
              </header>

              {#if expandedConnectionId === connection.id}
                {#if pending[`load:${connection.id}`]}
                  <div
                    class="flex min-h-28 items-center justify-center gap-2 border-t border-border-subtle text-label text-text-muted"
                    role="status"
                  >
                    <Spinner size={15} />Loading setup...
                  </div>
                {:else}
                  <section class="border-t border-border-subtle px-4 py-4 sm:px-5">
                    <div class="flex flex-wrap items-center justify-between gap-3">
                      <div>
                        <h3 class="text-label font-[510] text-text-primary">Configuration</h3>
                        <p class="mt-0.5 text-meta text-text-muted">Provider settings and credential replacement</p>
                      </div>
                      {#if inspection && !authenticationSetupPending(connection.id, inspection)}
                        <Button
                          disabled={pending[`configuration:${connection.id}`]}
                          onclick={() => saveConfiguration(connection)}
                          size="sm"
                          variant="ghost"
                        >
                          {#if pending[`configuration:${connection.id}`]}
                            <Spinner size={13} />
                          {:else}
                            <Save
                              aria-hidden="true"
                              size={13}
                            />
                          {/if}
                          Save</Button
                        >
                      {/if}
                    </div>
                    <div class="mt-4">
                      {@render configurationFields(connection.id, inspection?.fields ?? [], "configuration")}
                    </div>
                    {#if inspection?.fields.length === 0}
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
                        />Connected as {accountName(connection.id) || "provider account"}
                      </p>
                    {:else if authentication && ["failed", "cancelled", "expired"].includes(authentication.status)}
                      <div class="mt-3 flex flex-wrap items-center justify-between gap-3">
                        <p class="text-label text-text-muted">Authentication {authentication.status}.</p>
                        <Button
                          onclick={() => (authentications = { ...authentications, [connection.id]: undefined })}
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
                              ><Button
                                onclick={() => copyCode(connection.id, authentication.prompt?.userCode ?? "")}
                                size="sm"
                                variant="ghost"
                                >{#if copiedCodeConnectionId === connection.id}
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
                        {@render configurationFields(connection.id, authentication.prompt?.fields ?? [], "authentication")}
                        <div class="flex gap-2">
                          <Button
                            disabled={pending[`authentication:${connection.id}`]}
                            onclick={() => continueAuthentication(connection)}
                            size="sm"
                            >{#if pending[`authentication:${connection.id}`] || authentication.status === "waiting"}
                              <Spinner size={13} />
                            {/if}
                            {authentication.prompt?.fields.length ? "Continue" : "Check sign-in"}</Button
                          ><Button
                            disabled={pending[`authentication:${connection.id}`]}
                            onclick={() => cancelAuthentication(connection)}
                            size="sm"
                            variant="ghost"
                            >Cancel</Button
                          >
                        </div>
                      </div>
                    {:else if inspection && inspection.authenticationMethods.length === 0}
                      <p class="mt-3 text-label text-text-muted">
                        This adapter does not require interactive authentication.
                      </p>
                    {:else if selectedMethod}
                      <div class="mt-4 grid gap-4">
                        {#if (inspection?.authenticationMethods.length ?? 0) > 1}
                          <Select
                            ariaLabel="Authentication method"
                            items={methodItems(connection.id)}
                            onValueChange={(value) => selectMethod(connection.id, value)}
                            triggerClass="w-full sm:w-64"
                            value={selectedMethods[connection.id]}
                          />
                        {/if}
                        {@render configurationFields(connection.id, selectedMethod.fields, "authentication")}
                        <Button
                          class="w-fit"
                          disabled={pending[`authentication:${connection.id}`]}
                          onclick={() => startAuthentication(connection)}
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
                        <p class="mt-0.5 text-meta text-text-muted">{connectionSources.length} discovered</p>
                      </div>
                      <Button
                        disabled={pending[`discover:${connection.id}`]}
                        onclick={() => discover(connection)}
                        size="sm"
                        variant="ghost"
                        ><RefreshCw
                          aria-hidden="true"
                          class={pending[`discover:${connection.id}`] ? "animate-spin" : ""}
                          size={13}
                        />Discover</Button
                      >
                    </header>
                    {#if connectionSources.length === 0}
                      <p class="border-t border-border-subtle px-4 py-5 text-label text-text-muted sm:px-5">
                        No sources discovered.
                      </p>
                    {:else}
                      <div class="divide-y divide-border-subtle border-t border-border-subtle">
                        {#each connectionSources as source (source.id)}
                          {@const attachment = attachmentFor(source.id)}
                          <div class="grid gap-3 px-4 py-3 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-center sm:px-5">
                            <div class="min-w-0">
                              <p class="truncate text-label font-[510] text-text-secondary">{source.displayName}</p>
                              <p class="mt-1 text-meta text-text-disabled">
                                {sourceKind(source)}
                                / {source.availability} / {source.accountDisplayName}
                              </p>
                            </div>
                            <div class="flex flex-wrap items-center justify-end gap-3">
                              <Button
                                disabled={pending[`scan:${source.id}`] || !selectedWorkspace || !attachment || source.availability !== "available"}
                                onclick={() => scanCatalog(source)}
                                size="sm"
                                variant="ghost"
                                ><RefreshCw
                                  aria-hidden="true"
                                  class={pending[`scan:${source.id}`] ? "animate-spin" : ""}
                                  size={13}
                                />Scan</Button
                              >
                              <div class="flex items-center gap-2 text-label text-text-muted">
                                <Checkbox
                                  aria-label={`Attach ${source.displayName} to the workspace`}
                                  checked={Boolean(attachment)}
                                  disabled={pending[`source:${source.id}`] || !selectedWorkspace || source.availability !== "available"}
                                  onCheckedChange={(enabled) => changeAttachment(source, enabled)}
                                /><span>Attached</span>
                              </div>
                            </div>
                          </div>
                        {/each}
                      </div>
                    {/if}
                  </section>
                {/if}
              {/if}
            </Panel>
          {/each}
        </div>
      {/if}
    </div>
  </div>
</div>

<AlertDialog.Root
  onOpenChange={changeDeleteOpen}
  bind:open={deleteOpen}
>
  <AlertDialog.Content>
    <AlertDialog.Title class="text-title font-semibold text-text-primary"
      >Delete connection permanently?</AlertDialog.Title
    >
    <AlertDialog.Description class="mt-1 text-label leading-5 text-text-muted"
      >{pendingDelete?.displayName ?? "This connection"}
      and its stored authentication data will be permanently removed.</AlertDialog.Description
    >
    {#if pendingDelete && pending[deleteImpactPendingKey(pendingDelete.id)]}
      <div
        class="mt-4 flex items-center gap-2 text-label text-text-muted"
        role="status"
      >
        <Spinner size={14} />Checking deletion impact...
      </div>
    {:else if deletionImpact}
      <p class="mt-4 text-label leading-5 text-text-secondary">
        This removes {deletionImpact.sourceCount} sources, {deletionImpact.workspaceAttachmentCount} workspace
        attachments, and {deletionImpact.taskCount} durable tasks.
        {#if deletionImpact.activeTaskCount > 0}
          <span class="text-danger">Active work must finish first.</span>
        {/if}
      </p>
    {/if}
    <div class="mt-5 flex justify-end gap-2">
      <AlertDialog.Cancel
        class="inline-flex h-8 items-center rounded-md border border-border-subtle px-3 text-label text-text-secondary hover:bg-white/4"
        >Cancel</AlertDialog.Cancel
      ><AlertDialog.Action
        class="inline-flex h-8 items-center gap-1.5 rounded-md border border-danger/30 bg-danger/8 px-3 text-label text-danger hover:bg-danger/12 disabled:pointer-events-none disabled:opacity-35"
        disabled={!pendingDelete || !deletionImpact || deletionImpact.activeTaskCount > 0 || Boolean(pendingDelete && pending[deletePendingKey(pendingDelete.id)])}
        onclick={(event) => { event.preventDefault(); void confirmDelete(); }}
        >{#if pendingDelete && pending[deletePendingKey(pendingDelete.id)]}
          <Spinner size={13} />
        {/if}
        Delete permanently</AlertDialog.Action
      >
    </div>
  </AlertDialog.Content>
</AlertDialog.Root>
