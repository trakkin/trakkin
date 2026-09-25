<script lang="ts">
import { RefreshCw, RotateCcw, ScrollText, Search, X } from "@lucide/svelte";
import { onDestroy, onMount, tick } from "svelte";
import {
  Button,
  EmptyState,
  IconButton,
  PageHeader,
  PageToolbar,
  Panel,
  Select,
  Spinner,
  Switch,
  TextInput,
} from "$lib";
import { type LogEntry, listLogs, streamLogs } from "$lib/api/logs";

const levelItems = [
  { label: "All levels", value: "ALL" },
  { label: "Error", value: "ERROR" },
  { label: "Warning", value: "WARN" },
  { label: "Info", value: "INFO" },
  { label: "Debug", value: "DEBUG" },
  { label: "Trace", value: "TRACE" },
];
const pageSize = 200;
const maximumLoadedEntries = 5_000;

let entries = $state<LogEntry[]>([]);
let loading = $state(true);
let reloading = $state(false);
let loadingOlder = $state(false);
let errorMessage = $state("");
let searchQuery = $state("");
let levelFilter = $state("ALL");
let live = $state(true);
let liveStatus = $state<"connecting" | "live" | "paused" | "reconnecting">("connecting");
let hasMoreBefore = $state(false);
let lastUpdatedAt = $state<Date | null>(null);
let logViewport = $state<HTMLDivElement>();
let requestSequence = 0;
let requestController: AbortController | undefined;
let historyController: AbortController | undefined;
let stopStream: (() => void) | undefined;
let mounted = false;

let normalizedSearch = $derived(searchQuery.trim().toLocaleLowerCase());
let filteredEntries = $derived(
  entries.filter((entry) => {
    if (levelFilter !== "ALL" && entry.level.toUpperCase() !== levelFilter) return false;
    if (!normalizedSearch) return true;
    return searchableText(entry).includes(normalizedSearch);
  }),
);

onMount(() => {
  mounted = true;
  void refreshLogs();
});

onDestroy(() => {
  mounted = false;
  requestSequence += 1;
  requestController?.abort();
  historyController?.abort();
  stopStreaming();
});

async function refreshLogs() {
  const sequence = ++requestSequence;
  const followLatest = entries.length === 0 || isNearLatest();
  stopStreaming();
  requestController?.abort();
  historyController?.abort();
  requestController = new AbortController();
  if (entries.length === 0) loading = true;
  else reloading = true;
  errorMessage = "";
  try {
    const result = await listLogs({ limit: pageSize }, requestController.signal);
    if (sequence !== requestSequence) return;
    entries = result.entries;
    hasMoreBefore = result.hasMoreBefore;
    lastUpdatedAt = new Date();
    loading = false;
    reloading = false;
    await tick();
    if (sequence === requestSequence && followLatest) scrollToLatest();
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    if (sequence === requestSequence) {
      errorMessage = error instanceof Error ? error.message : "Logs could not be loaded";
    }
  } finally {
    if (sequence === requestSequence) {
      loading = false;
      reloading = false;
      if (mounted && live) startStreaming();
    }
  }
}

async function loadOlderLogs() {
  const before = entries[0]?.cursor;
  if (!before || loadingOlder || !hasMoreBefore) return;
  const previousScrollHeight = logViewport?.scrollHeight ?? 0;
  loadingOlder = true;
  errorMessage = "";
  historyController?.abort();
  historyController = new AbortController();
  try {
    const result = await listLogs({ before, limit: pageSize }, historyController.signal);
    const knownCursors = new Set(entries.map((entry) => entry.cursor));
    entries = [...result.entries.filter((entry) => !knownCursors.has(entry.cursor)), ...entries];
    hasMoreBefore = result.hasMoreBefore;
    await tick();
    if (logViewport) logViewport.scrollTop += logViewport.scrollHeight - previousScrollHeight;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return;
    errorMessage = error instanceof Error ? error.message : "Older logs could not be loaded";
  } finally {
    loadingOlder = false;
  }
}

function startStreaming() {
  stopStreaming();
  if (!mounted || !live) return;
  liveStatus = "connecting";
  stopStream = streamLogs(entries.at(-1)?.cursor, {
    onEntry: (entry) => void appendStreamEntry(entry),
    onError: () => (liveStatus = "reconnecting"),
    onOpen: () => (liveStatus = "live"),
    onReset: () => void refreshLogs(),
  });
}

function stopStreaming() {
  stopStream?.();
  stopStream = undefined;
}

async function appendStreamEntry(entry: LogEntry) {
  if (entries.some((existing) => existing.cursor === entry.cursor)) return;
  const followLatest = isNearLatest();
  const nextEntries = [...entries, entry];
  if (nextEntries.length > maximumLoadedEntries) {
    entries = nextEntries.slice(-maximumLoadedEntries);
    hasMoreBefore = true;
  } else {
    entries = nextEntries;
  }
  lastUpdatedAt = new Date();
  await tick();
  if (followLatest) scrollToLatest();
}

function setLive(enabled: boolean) {
  live = enabled;
  if (enabled) startStreaming();
  else {
    stopStreaming();
    liveStatus = "paused";
  }
}

function isNearLatest(): boolean {
  if (!logViewport) return true;
  return logViewport.scrollHeight - logViewport.scrollTop - logViewport.clientHeight < 48;
}

function scrollToLatest() {
  if (logViewport) logViewport.scrollTop = logViewport.scrollHeight;
}

function entryKey(entry: LogEntry, index: number): string {
  return entry.cursor || `${entry.timestamp}:${entry.target}:${index}`;
}

function searchableText(entry: LogEntry): string {
  return stripAnsi(JSON.stringify(entry)).toLocaleLowerCase();
}

function messageFor(entry: LogEntry): string {
  const message = entry.fields.message;
  return stripAnsi(typeof message === "string" ? message : formatValue(message));
}

function inlineFields(entry: LogEntry): Array<[string, unknown]> {
  const fields = Object.entries(entry.fields).filter(([key]) => key !== "message");
  const span = Object.entries(entry.span ?? {}).map(([name, value]) => [`span.${name}`, value] as [string, unknown]);
  const spans: Array<[string, unknown]> = entry.spans?.length ? [["spans", entry.spans]] : [];
  return [...fields, ...span, ...spans];
}

function formatValue(value: unknown): string {
  if (typeof value === "string") return stripAnsi(value);
  if (value === undefined) return "undefined";
  return JSON.stringify(value);
}

function formatInlineValue(value: unknown): string {
  const formatted = formatValue(value);
  return typeof value === "string" && /\s/.test(formatted) ? JSON.stringify(formatted) : formatted;
}

function stripAnsi(value: string): string {
  let result = "";
  let index = 0;
  while (index < value.length) {
    if (value.charCodeAt(index) === 27 && value[index + 1] === "[") {
      index += 2;
      while (index < value.length) {
        const code = value.charCodeAt(index);
        index += 1;
        if (code >= 64 && code <= 126) break;
      }
      continue;
    }
    result += value[index];
    index += 1;
  }
  return result;
}

function formatTimestamp(value: string): string {
  const timestamp = new Date(value);
  if (!Number.isFinite(timestamp.valueOf())) return value;
  const year = timestamp.getUTCFullYear();
  const month = String(timestamp.getUTCMonth() + 1).padStart(2, "0");
  const day = String(timestamp.getUTCDate()).padStart(2, "0");
  const hours = String(timestamp.getUTCHours()).padStart(2, "0");
  const minutes = String(timestamp.getUTCMinutes()).padStart(2, "0");
  const seconds = String(timestamp.getUTCSeconds()).padStart(2, "0");
  const milliseconds = String(timestamp.getUTCMilliseconds()).padStart(3, "0");
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${milliseconds}Z`;
}

function levelClass(level: string): string {
  switch (level.toUpperCase()) {
    case "ERROR":
      return "text-danger";
    case "WARN":
      return "text-warning";
    case "INFO":
      return "text-info";
    case "DEBUG":
      return "text-text-secondary";
    default:
      return "text-text-muted";
  }
}
</script>

<svelte:head><title>Trakkin &middot; Logs</title></svelte:head>

<div class="flex h-full min-h-0 flex-col">
  <PageHeader title="Logs">
    {#snippet actions()}
      <IconButton
        disabled={reloading}
        label="Refresh logs"
        onclick={refreshLogs}
        size="sm"
        title="Refresh logs"
        variant="ghost"
      >
        <span class={["inline-flex", reloading && "animate-spin"]}>
          <RefreshCw
            aria-hidden="true"
            size={13}
          />
        </span>
      </IconButton>
    {/snippet}
  </PageHeader>

  <PageToolbar label="Log filters">
    <div class="flex min-w-0 flex-wrap items-center gap-2">
      <div class="relative w-full min-w-0 sm:min-w-60 sm:flex-1">
        <Search
          aria-hidden="true"
          class="pointer-events-none absolute top-1/2 left-2.5 z-10 -translate-y-1/2 text-text-muted"
          size={14}
        />
        <TextInput
          aria-label="Search logs"
          class="w-full pr-8 pl-8"
          oninput={(event) => (searchQuery = event.currentTarget.value)}
          placeholder="Search message, source, or context"
          value={searchQuery}
        />
        {#if searchQuery}
          <button
            aria-label="Clear log search"
            class="absolute top-1/2 right-1.5 flex size-6 -translate-y-1/2 items-center justify-center rounded-control text-text-muted outline-none hover:bg-white/5 hover:text-text-secondary focus-visible:ring-1 focus-visible:ring-accent"
            onclick={() => (searchQuery = "")}
            type="button"
          >
            <X
              aria-hidden="true"
              size={12}
            />
          </button>
        {/if}
      </div>
      <Select
        ariaLabel="Log level filter"
        class="min-w-0 flex-1 sm:w-44 sm:flex-none"
        items={levelItems}
        onValueChange={(value) => (levelFilter = value)}
        triggerClass="w-full"
        value={levelFilter}
      />
      <div class="flex h-8 shrink-0 items-center gap-2 px-1 text-label text-text-secondary">
        <Switch
          aria-label="Live log updates"
          checked={live}
          onCheckedChange={(checked) => setLive(checked)}
        />
        <span>Live updates</span>
      </div>
    </div>
  </PageToolbar>

  <div class="flex min-h-0 flex-1 flex-col p-4 sm:p-6">
    {#if loading}
      <div
        class="flex min-h-48 flex-1 items-center justify-center gap-2 text-label text-text-muted"
        role="status"
      >
        <Spinner size={16} />
        Loading logs...
      </div>
    {:else if errorMessage && entries.length === 0}
      <Panel
        class="grid min-h-48 flex-1 place-items-center px-4 py-6 text-center text-label text-danger"
        role="alert"
        tone="danger"
      >
        <div>
          <p>{errorMessage}</p>
          <Button
            class="mt-4"
            onclick={refreshLogs}
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
    {:else if filteredEntries.length === 0}
      <EmptyState
        aria-label="Runtime logs"
        class="flex-1"
        description={normalizedSearch || levelFilter !== "ALL" ? "No logs match the current filters." : "No runtime logs have been recorded."}
        icon={ScrollText}
        title="No matching logs"
      >
        {#snippet actions()}
          {#if normalizedSearch || levelFilter !== "ALL"}
            <Button
              onclick={() => { searchQuery = ""; levelFilter = "ALL"; }}
              size="sm"
            >
              <RotateCcw
                aria-hidden="true"
                size={14}
              />Clear filters
            </Button>
          {/if}
        {/snippet}
      </EmptyState>
    {:else}
      <section
        aria-label="Runtime logs"
        class="flex min-h-0 flex-1 flex-col overflow-hidden rounded-panel border border-border-strong bg-surface-panel shadow-[inset_0_1px_rgb(255_255_255/3%)]"
      >
        {#if errorMessage}
          <div
            class="shrink-0 border-b border-danger/20 bg-danger/6 px-4 py-2 text-label text-danger"
            role="alert"
          >
            {errorMessage}
          </div>
        {/if}
        {#if hasMoreBefore}
          <div class="flex h-10 shrink-0 items-center justify-center border-b border-border-subtle">
            <Button
              disabled={loadingOlder}
              onclick={() => void loadOlderLogs()}
              size="sm"
              variant="ghost"
            >
              {#if loadingOlder}
                <Spinner size={12} />
              {/if}
              Load older logs
            </Button>
          </div>
        {/if}
        <div
          class="min-h-0 flex-1 overflow-auto"
          role="feed"
          bind:this={logViewport}
        >
          {#each filteredEntries as entry, index (entryKey(entry, index))}
            {@const message = messageFor(entry)}
            <article
              aria-label={`${entry.level} log: ${message}`}
              class="flex h-8 w-max min-w-full items-center gap-3 whitespace-nowrap border-b border-border-subtle px-3 font-mono text-meta last:border-b-0 sm:px-4"
            >
              <time
                class="shrink-0 text-text-disabled"
                datetime={entry.timestamp}
                title={entry.timestamp}
              >
                {formatTimestamp(entry.timestamp)}
              </time>
              <span class="w-12 shrink-0 font-[650] {levelClass(entry.level)}">
                {entry.level}
              </span>
              <span class="shrink-0 text-text-muted">{entry.target}:</span>
              <span class="shrink-0 text-text-primary">{message}</span>
              {#each inlineFields(entry) as [name, value] (name)}
                <span class="shrink-0 text-text-disabled">
                  {name}=<span class="text-text-muted">{formatInlineValue(value)}</span>
                </span>
              {/each}
            </article>
          {/each}
        </div>
        <footer
          class="flex h-10 shrink-0 items-center justify-between gap-3 border-t border-border-strong px-3 sm:px-4"
        >
          <span
            aria-live="polite"
            class="text-meta text-text-muted"
          >
            {filteredEntries.length}
            of {entries.length} events
            {#if hasMoreBefore}
              · more retained
            {/if}
          </span>
          <span class="text-meta text-text-disabled">
            {liveStatus === "live"
              ? "Live"
              : liveStatus === "reconnecting"
                ? "Reconnecting..."
                : liveStatus === "connecting"
                  ? "Connecting..."
                  : lastUpdatedAt
                    ? `Paused at ${lastUpdatedAt.toLocaleTimeString()}`
                    : "Paused"}
          </span>
        </footer>
      </section>
    {/if}
  </div>
</div>
