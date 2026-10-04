<script lang="ts">
import { Plus } from "@lucide/svelte";
import { onDestroy } from "svelte";
import { Button, Panel, Select, Spinner, TextInput } from "$lib";
import { type Connection, createConnection } from "$lib/api/connections";

let {
  open,
  providers,
  oncreated,
  onclose,
}: {
  open: boolean;
  providers: { value: string; label: string }[];
  oncreated: (connection: Connection, reveal: boolean) => void;
  onclose: () => void;
} = $props();

const scenarios = [
  "hierarchical-catalog-assets-v1",
  "empty-targeted-receiver-v1",
  "edition-hierarchy-v1",
  "flat-catalog-revisions-v1",
  "adversarial-v1",
  "dual-connection-isolation-v1",
];
const scenarioItems = scenarios.map((value) => ({ value, label: value.replace(/-v\d+$/, "").replaceAll("-", " ") }));
const revisionItems = [
  { value: "v1", label: "Revision v1" },
  { value: "v2", label: "Revision v2" },
];
const instanceItems = [
  { value: "alpha", label: "Instance alpha" },
  { value: "beta", label: "Instance beta" },
];
let providerId = $state("");
let name = $state("");
let scenario = $state(scenarios[0]);
let revision = $state("v1");
let instance = $state("alpha");
let token = $state("fixture-secret");
let busy = $state(false);
let error = $state("");
let session = 0;
let draftRevision = 0;
const controller = new AbortController();
onDestroy(() => controller.abort());

$effect(() => {
  if (!providers.some((provider) => provider.value === providerId)) providerId = providers[0]?.value ?? "";
});
$effect(() => {
  token = scenario === "dual-connection-isolation-v1" ? `fixture-secret-${instance}` : "fixture-secret";
});
$effect(() => {
  void open;
  session += 1;
  busy = false;
  error = "";
});

async function submit() {
  if (busy || !name.trim() || !providerId) return;
  const current = session;
  const submittedRevision = draftRevision;
  const fixture = providerId === "dev.trakkin.fixture";
  busy = true;
  error = "";
  try {
    const connection = await createConnection(
      {
        providerId,
        displayName: name.trim(),
        settings: fixture
          ? Object.entries({ scenario, revision, fault: "none", instance }).map(([key, value]) => ({
              key,
              value: { kind: "text" as const, value },
            }))
          : [],
        secrets: fixture ? [{ key: "fixture_token", value: Array.from(new TextEncoder().encode(token)) }] : [],
      },
      controller.signal,
    );
    if (controller.signal.aborted) return;
    const owns = current === session && submittedRevision === draftRevision && open;
    oncreated(connection, owns);
    if (owns) {
      name = "";
      onclose();
    }
  } catch (cause) {
    if (!controller.signal.aborted && current === session && submittedRevision === draftRevision && open) {
      error = cause instanceof Error ? cause.message : "The connection could not be created";
    }
  } finally {
    if (!controller.signal.aborted && current === session) busy = false;
  }
}
</script>

{#if open}
  <Panel
    class="p-4 sm:p-5"
    tone="accent"
  >
    <form
      class="grid gap-4 lg:grid-cols-2"
      onsubmit={(event) => { event.preventDefault(); void submit(); }}
    >
      <div class="grid gap-1.5 text-label text-text-secondary">
        <span>Provider</span>
        <Select
          ariaLabel="Provider"
          disabled={providers.length === 0}
          items={providers}
          onValueChange={(value) => { providerId = value; draftRevision += 1; }}
          triggerClass="w-full"
          value={providerId}
        />
      </div>
      <label
        class="grid gap-1.5 text-label text-text-secondary"
        for="connection-name"
      >
        Connection name<TextInput
          aria-label="Connection name"
          id="connection-name"
          oninput={() => { draftRevision += 1; }}
          placeholder="Personal connection"
          required
          bind:value={name}
        />
      </label>
      {#if providers.length === 0}
        <p
          class="text-label text-warning lg:col-span-2"
          role="status"
        >
          Install and activate a provider before creating a connection.
        </p>
      {/if}
      {#if providerId === "dev.trakkin.fixture"}
        <div class="grid gap-1.5 text-label text-text-secondary">
          <span>Fixture scenario</span>
          <Select
            ariaLabel="Fixture scenario"
            items={scenarioItems}
            onValueChange={(value) => { scenario = value; draftRevision += 1; }}
            triggerClass="w-full"
            value={scenario}
          />
        </div>
        <div class="grid gap-1.5 text-label text-text-secondary">
          <span>Revision</span>
          <Select
            ariaLabel="Fixture revision"
            items={revisionItems}
            onValueChange={(value) => { revision = value; draftRevision += 1; }}
            triggerClass="w-full"
            value={revision}
          />
        </div>
        <div class="grid gap-1.5 text-label text-text-secondary">
          <span>Instance</span>
          <Select
            ariaLabel="Fixture instance"
            items={instanceItems}
            onValueChange={(value) => { instance = value; draftRevision += 1; }}
            triggerClass="w-full"
            value={instance}
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
            oninput={() => { draftRevision += 1; }}
            required
            type="password"
            bind:value={token}
          />
        </label>
      {/if}
      {#if error}
        <p
          class="text-label text-danger lg:col-span-2"
          role="alert"
        >
          {error}
        </p>
      {/if}
      <div class="flex justify-end lg:col-span-2">
        <Button
          disabled={!providerId || !name.trim() || (providerId === "dev.trakkin.fixture" && !token) || busy}
          type="submit"
          variant="primary"
        >
          {#if busy}
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
