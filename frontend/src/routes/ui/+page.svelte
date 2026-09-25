<script lang="ts">
import { ArrowRight, Copy, MoreHorizontal, Plus, RefreshCw, Trash2 } from "@lucide/svelte";
import {
  Button,
  type ButtonSize,
  type ButtonVariant,
  Checkbox,
  Chip,
  type ChipTone,
  IconButton,
  PageHeader,
  Select,
  Switch,
  Tabs,
  Textarea,
  TextInput,
  Tooltip,
} from "$lib";
import ThemePaletteLab from "./ThemePaletteLab.svelte";

const buttonVariants: ButtonVariant[] = ["primary", "secondary", "ghost", "danger"];
const buttonSizes: ButtonSize[] = ["sm", "md", "lg"];
const selectItems = [
  { value: "ready", label: "Ready" },
  { value: "pending", label: "Pending" },
  { value: "disabled", label: "Disabled", disabled: true },
];
const chipExamples: { label: string; tone: ChipTone }[] = [
  { label: "Trakkin", tone: "accent" },
  { label: "Performance", tone: "success" },
  { label: "Needs review", tone: "warning" },
  { label: "Blocked", tone: "danger" },
  { label: "iOS", tone: "info" },
  { label: "Unassigned", tone: "neutral" },
];
const edgeSelectItems = Array.from({ length: 20 }, (_, index) => ({
  value: `option-${index + 1}`,
  label: `Viewport option ${index + 1}`,
}));
const tableRows: [string, string, string, ChipTone][] = [
  ["Living Room Server", "Fixture Media", "Ready", "success"],
  ["Watch history", "Trakt", "Pending", "warning"],
  ["Archive", "Media server", "Unavailable", "danger"],
];

let selectedStatus = $state("ready");
let edgeSelect = $state("option-1");
let tab = $state("preview");
let inputValue = $state("Living Room Server");
let textareaValue = $state("Sync progress after each completed playback session.");
let switchOff = $state(false);
let switchOn = $state(true);
let checkboxOff = $state(false);
let checkboxOn = $state(true);
let checkboxMixed = $state(false);
let indeterminate = $state(true);
</script>

<svelte:head>
  <title>Trakkin &middot; UI</title>
  <meta
    content="Development reference for Trakkin UI components."
    name="description"
  >
</svelte:head>

<div class="flex h-full min-h-0 flex-col text-text-primary">
  <PageHeader title="UI" />
  <main class="min-h-0 flex-1 overflow-y-auto">
    <div class="mx-auto w-full max-w-6xl">
      <section class="grid gap-5 border-b border-border-subtle px-4 py-7 sm:px-8 lg:grid-cols-[10rem_minmax(0,1fr)]">
        <div>
          <h2 class="text-label font-[510] text-text-secondary">Foundation</h2>
          <p class="mt-1 text-meta text-text-disabled">Type and semantic color</p>
        </div>
        <div class="min-w-0 space-y-6">
          <ThemePaletteLab />
          <div class="grid gap-3 sm:grid-cols-2">
            <div>
              <p class="text-title font-[640]">Provider connections</p>
              <p class="mt-1 text-label text-text-muted">Title &middot; 20px / 640</p>
            </div>
            <div>
              <p class="text-body text-text-secondary">Values are validated by the running provider.</p>
              <p class="mt-1 text-meta text-text-disabled">Body &middot; 14px / 400</p>
            </div>
          </div>
          <div
            class="grid grid-cols-2 gap-px overflow-hidden rounded-panel border border-border-subtle bg-border-subtle sm:grid-cols-4"
          >
            {#each [
              ["Canvas", "bg-surface-canvas"],
              ["Panel", "bg-surface-panel"],
              ["Raised", "bg-surface-raised"],
              ["Accent", "bg-accent"],
            ] as [label, colorClass]}
              <div class="bg-surface-panel p-3">
                <span class="block h-8 rounded-control border border-border-subtle {colorClass}"></span>
                <span class="mt-2 block text-meta text-text-muted">{label}</span>
              </div>
            {/each}
          </div>
        </div>
      </section>

      <section class="grid gap-5 border-b border-border-subtle px-4 py-7 sm:px-8 lg:grid-cols-[10rem_minmax(0,1fr)]">
        <div>
          <h2 class="text-label font-[510] text-text-secondary">Chips</h2>
          <p class="mt-1 text-meta text-text-disabled">Labels and semantic states</p>
        </div>
        <div class="flex min-w-0 flex-wrap items-center gap-2">
          <Chip>Default</Chip>
          {#each chipExamples as chip}
            <Chip
              status
              tone={chip.tone}
              >{chip.label}</Chip
            >
          {/each}
        </div>
      </section>

      <section class="grid gap-5 border-b border-border-subtle px-4 py-7 sm:px-8 lg:grid-cols-[10rem_minmax(0,1fr)]">
        <div>
          <h2 class="text-label font-[510] text-text-secondary">Buttons</h2>
          <p class="mt-1 text-meta text-text-disabled">Variants, sizes, icons</p>
        </div>
        <div class="min-w-0 divide-y divide-border-subtle overflow-hidden rounded-panel border border-border-subtle">
          {#each buttonVariants as variant}
            <div class="grid min-h-14 items-center gap-3 px-3 py-3 sm:grid-cols-[6rem_minmax(0,1fr)]">
              <span class="text-meta capitalize text-text-muted">{variant}</span>
              <div class="flex flex-wrap items-center gap-2">
                {#each buttonSizes as size}
                  <Button
                    {size}
                    {variant}
                  >
                    {variant}
                    {#if variant === "primary"}
                      <ArrowRight
                        aria-hidden="true"
                        size={14}
                      />
                    {/if}
                  </Button>
                {/each}
                <Button
                  disabled
                  size="sm"
                  {variant}
                  >Disabled</Button
                >
              </div>
            </div>
          {/each}
          <div class="grid min-h-14 items-center gap-3 px-3 py-3 sm:grid-cols-[6rem_minmax(0,1fr)]">
            <span class="text-meta text-text-muted">Icon</span>
            <div class="flex items-center gap-2">
              {#each buttonSizes as size}
                <Tooltip.Root>
                  <Tooltip.Trigger>
                    {#snippet child({ props })}
                      <IconButton
                        {...props}
                        label={`Add item &middot; ${size}`}
                        {size}
                      >
                        <Plus
                          aria-hidden="true"
                          size={14}
                        />
                      </IconButton>
                    {/snippet}
                  </Tooltip.Trigger>
                  <Tooltip.Content>Add item</Tooltip.Content>
                </Tooltip.Root>
              {/each}
              <IconButton
                label="More actions"
                variant="secondary"
              >
                <MoreHorizontal
                  aria-hidden="true"
                  size={15}
                />
              </IconButton>
              <IconButton
                label="Delete item"
                variant="danger"
              >
                <Trash2
                  aria-hidden="true"
                  size={14}
                />
              </IconButton>
            </div>
          </div>
        </div>
      </section>

      <section class="grid gap-5 border-b border-border-subtle px-4 py-7 sm:px-8 lg:grid-cols-[10rem_minmax(0,1fr)]">
        <div>
          <h2 class="text-label font-[510] text-text-secondary">Form controls</h2>
          <p class="mt-1 text-meta text-text-disabled">Inputs, textarea, select</p>
        </div>
        <div class="grid min-w-0 gap-5 lg:grid-cols-2">
          <div class="space-y-4">
            {#each buttonSizes as size}
              <div>
                <span class="mb-1.5 block text-meta text-text-muted capitalize">Input &middot; {size}</span>
                <TextInput
                  aria-label={`Input ${size}`}
                  controlSize={size}
                  bind:value={inputValue}
                />
              </div>
            {/each}
            <div>
              <span class="mb-1.5 block text-meta text-text-muted">Invalid</span>
              <TextInput
                aria-invalid="true"
                aria-label="Invalid input"
                value="Invalid endpoint"
              />
              <span class="mt-1.5 block text-meta text-danger">Enter an absolute HTTP or HTTPS URL.</span>
            </div>
            <div>
              <span class="mb-1.5 block text-meta text-text-muted">Disabled</span>
              <TextInput
                aria-label="Disabled input"
                disabled
                value="Unavailable"
              />
            </div>
          </div>
          <div class="space-y-4">
            <div>
              <span class="mb-1.5 block text-meta text-text-muted">Textarea</span>
              <Textarea
                aria-label="Description"
                bind:value={textareaValue}
              />
            </div>
            <div>
              <span class="mb-1.5 block text-meta text-text-muted">Textarea invalid</span>
              <Textarea
                aria-invalid="true"
                aria-label="Invalid description"
                value="Sync every state without a conflict policy."
              />
              <span class="mt-1.5 block text-meta text-danger">Choose how conflicting states should resolve.</span>
            </div>
            <div>
              <span class="mb-1.5 block text-meta text-text-muted">Select</span>
              <Select
                ariaLabel="Connection status"
                items={selectItems}
                triggerClass="w-full"
                bind:value={selectedStatus}
              />
            </div>
            <div>
              <span class="mb-1.5 block text-meta text-text-muted">Select invalid</span>
              <Select
                ariaInvalid
                ariaLabel="Invalid provider"
                items={selectItems}
                placeholder="Choose a provider"
                triggerClass="w-full"
              />
              <span class="mt-1.5 block text-meta text-danger">Select a provider before continuing.</span>
            </div>
            <div>
              <span class="mb-1.5 block text-meta text-text-muted">Select placeholder</span>
              <Select
                ariaLabel="Provider"
                items={selectItems}
                placeholder="Choose a provider"
                triggerClass="w-full"
              />
            </div>
            <div>
              <span class="mb-1.5 block text-meta text-text-muted">Select disabled</span>
              <Select
                ariaLabel="Disabled select"
                disabled
                items={selectItems}
                triggerClass="w-full"
                value="ready"
              />
            </div>
          </div>
        </div>
      </section>

      <section class="grid gap-5 border-b border-border-subtle px-4 py-7 sm:px-8 lg:grid-cols-[10rem_minmax(0,1fr)]">
        <div>
          <h2 class="text-label font-[510] text-text-secondary">Selection</h2>
          <p class="mt-1 text-meta text-text-disabled">Switch and checkbox</p>
        </div>
        <div class="grid min-w-0 gap-5 sm:grid-cols-2">
          <div class="divide-y divide-border-subtle overflow-hidden rounded-panel border border-border-subtle">
            {#each [
              ["Unchecked", "off"],
              ["Checked", "on"],
              ["Disabled", "disabled"],
            ] as [label, state]}
              <div class="flex h-10 items-center justify-between px-3">
                <span class="text-label text-text-secondary">{label}</span>
                {#if state === "off"}
                  <Switch
                    aria-label="Unchecked switch"
                    bind:checked={switchOff}
                  />
                {:else if state === "on"}
                  <Switch
                    aria-label="Checked switch"
                    bind:checked={switchOn}
                  />
                {:else}
                  <Switch
                    aria-label="Disabled switch"
                    checked
                    disabled
                  />
                {/if}
              </div>
            {/each}
          </div>
          <div class="divide-y divide-border-subtle overflow-hidden rounded-panel border border-border-subtle">
            <div class="flex h-10 items-center gap-2.5 px-3 text-label text-text-secondary">
              <Checkbox
                aria-label="Unchecked checkbox"
                bind:checked={checkboxOff}
              />
              Unchecked
            </div>
            <div class="flex h-10 items-center gap-2.5 px-3 text-label text-text-secondary">
              <Checkbox
                aria-label="Checked checkbox"
                bind:checked={checkboxOn}
              />
              Checked
            </div>
            <div class="flex h-10 items-center gap-2.5 px-3 text-label text-text-secondary">
              <Checkbox
                aria-label="Indeterminate checkbox"
                bind:checked={checkboxMixed}
                bind:indeterminate
              />
              Indeterminate
            </div>
            <div class="flex h-10 items-center gap-2.5 px-3 text-label text-text-disabled">
              <Checkbox
                aria-label="Disabled checkbox"
                checked
                disabled
              />
              Disabled
            </div>
          </div>
        </div>
      </section>

      <section class="grid gap-5 border-b border-border-subtle px-4 py-7 sm:px-8 lg:grid-cols-[10rem_minmax(0,1fr)]">
        <div>
          <h2 class="text-label font-[510] text-text-secondary">Navigation</h2>
          <p class="mt-1 text-meta text-text-disabled">Tabs and tooltip</p>
        </div>
        <div class="min-w-0 space-y-6">
          <Tabs.Root bind:value={tab}>
            <Tabs.List aria-label="Design sheet tabs">
              <Tabs.Trigger value="preview">Preview</Tabs.Trigger>
              <Tabs.Trigger value="usage">Usage</Tabs.Trigger>
              <Tabs.Trigger
                disabled
                value="disabled"
                >Disabled</Tabs.Trigger
              >
            </Tabs.List>
            <Tabs.Content value="preview">
              <div
                class="rounded-panel border border-border-subtle bg-surface-panel px-3 py-4 text-label text-text-secondary"
              >
                Component preview content
              </div>
            </Tabs.Content>
            <Tabs.Content value="usage">
              <div
                class="rounded-panel border border-border-subtle bg-surface-panel px-3 py-4 text-label text-text-secondary"
              >
                Usage guidance content
              </div>
            </Tabs.Content>
          </Tabs.Root>
          <div class="flex items-center gap-3">
            <Tooltip.Root>
              <Tooltip.Trigger>
                {#snippet child({ props })}
                  <Button
                    {...props}
                    size="sm"
                    variant="secondary"
                  >
                    <Copy
                      aria-hidden="true"
                      size={13}
                    />
                    Hover for tooltip
                  </Button>
                {/snippet}
              </Tooltip.Trigger>
              <Tooltip.Content side="right">Copy connection identifier</Tooltip.Content>
            </Tooltip.Root>
          </div>
        </div>
      </section>

      <section class="grid gap-5 border-b border-border-subtle px-4 py-7 sm:px-8 lg:grid-cols-[10rem_minmax(0,1fr)]">
        <div>
          <h2 class="text-label font-[510] text-text-secondary">Overlays</h2>
          <p class="mt-1 text-meta text-text-disabled">Viewport collision cases</p>
        </div>
        <div class="grid min-w-0 gap-3">
          <div class="flex h-32 items-start justify-end rounded-panel border border-border-subtle p-3">
            <Tooltip.Root>
              <Tooltip.Trigger>
                {#snippet child({ props })}
                  <Button
                    {...props}
                    size="sm"
                    variant="secondary"
                    >Right-edge tooltip</Button
                  >
                {/snippet}
              </Tooltip.Trigger>
              <Tooltip.Content side="right">This tooltip flips before leaving the viewport.</Tooltip.Content>
            </Tooltip.Root>
          </div>
          <div class="flex h-32 items-end justify-end rounded-panel border border-border-subtle p-3">
            <Select
              ariaLabel="Viewport collision select"
              items={edgeSelectItems}
              triggerClass="w-44"
              bind:value={edgeSelect}
            />
          </div>
        </div>
      </section>

      <section class="grid gap-5 px-4 py-7 sm:px-8 lg:grid-cols-[10rem_minmax(0,1fr)]">
        <div>
          <h2 class="text-label font-[510] text-text-secondary">Table</h2>
          <p class="mt-1 text-meta text-text-disabled">Dense data display</p>
        </div>
        <div class="divide-y divide-border-subtle overflow-hidden rounded-panel border border-border-subtle sm:hidden">
          {#each tableRows as [name, provider, status, tone]}
            <article class="px-3 py-3">
              <div class="flex min-w-0 items-start justify-between gap-3">
                <p class="min-w-0 text-label font-[510] wrap-anywhere text-text-secondary">{name}</p>
                <IconButton
                  label={`Refresh ${name}`}
                  size="sm"
                >
                  <RefreshCw
                    aria-hidden="true"
                    size={13}
                  />
                </IconButton>
              </div>
              <dl class="mt-2 grid grid-cols-[4.5rem_minmax(0,1fr)] items-center gap-x-3 gap-y-2 text-meta">
                <dt class="text-text-disabled">Provider</dt>
                <dd class="min-w-0 wrap-anywhere text-text-muted">{provider}</dd>
                <dt class="text-text-disabled">Status</dt>
                <dd>
                  <Chip
                    status
                    {tone}
                    >{status}</Chip
                  >
                </dd>
              </dl>
            </article>
          {/each}
        </div>
        <div class="hidden min-w-0 overflow-x-auto rounded-panel border border-border-subtle sm:block">
          <table class="w-full min-w-136 border-collapse text-left">
            <thead class="bg-surface-raised/35 text-meta font-medium text-text-muted">
              <tr class="h-8 border-b border-border-subtle">
                <th class="px-3 font-medium">Connection</th>
                <th class="px-3 font-medium">Provider</th>
                <th class="px-3 font-medium">Status</th>
                <th class="w-20 px-3 text-right font-medium">Actions</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-border-subtle text-label">
              {#each tableRows as [name, provider, status, tone]}
                <tr class="h-10 transition-colors hover:bg-white/3">
                  <td class="px-3 font-[450] text-text-secondary">{name}</td>
                  <td class="px-3 text-text-muted">{provider}</td>
                  <td class="px-3">
                    <Chip
                      status
                      {tone}
                      >{status}</Chip
                    >
                  </td>
                  <td class="px-3 text-right">
                    <IconButton
                      label={`Refresh ${name}`}
                      size="sm"
                    >
                      <RefreshCw
                        aria-hidden="true"
                        size={13}
                      />
                    </IconButton>
                  </td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  </main>
</div>
