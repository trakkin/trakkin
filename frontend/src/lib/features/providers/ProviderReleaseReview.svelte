<script lang="ts">
import { Check, ChevronDown, Copy, ExternalLink, TriangleAlert } from "@lucide/svelte";
import { Chip } from "$lib";
import type { ProviderReleaseInspection } from "$lib/api/providers";

type Props = {
  inspection: ProviderReleaseInspection;
};

let { inspection }: Props = $props();
let copied = $state(false);

let compatibility = $derived(
  inspection.compatibility === "compatible"
    ? {
        label: "Compatible",
        title: "Ready for this host",
        description: `A ${inspection.platform} binary is available and supports this Trakkin version.`,
        tone: "success" as const,
        surfaceClass: "border-success/25 bg-success/6",
        iconClass: "bg-success/12 text-success",
      }
    : inspection.compatibility === "unsupportedPlatform"
      ? {
          label: "Platform unavailable",
          title: "Unavailable on this host",
          description: `This release does not publish a ${inspection.platform} binary.`,
          tone: "warning" as const,
          surfaceClass: "border-warning/25 bg-warning/6",
          iconClass: "bg-warning/12 text-warning",
        }
      : {
          label: "Version incompatible",
          title: "Requires a different Trakkin version",
          description: "The release requirements do not include the version running on this host.",
          tone: "danger" as const,
          surfaceClass: "border-danger/25 bg-danger/6",
          iconClass: "bg-danger/12 text-danger",
        },
);
let requirements = $derived(Object.entries(inspection.requirements));

async function copyDigest() {
  await navigator.clipboard.writeText(inspection.indexDigest);
  copied = true;
  window.setTimeout(() => (copied = false), 1600);
}

function formatTimestamp(value: string | null | undefined): string {
  if (!value) return "Not recorded";
  const timestamp = new Date(value);
  if (!Number.isFinite(timestamp.valueOf())) return "Not recorded";
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(timestamp);
}

function formatBytes(value: number | null | undefined): string {
  if (value == null) return "Not available";
  return new Intl.NumberFormat(undefined, { style: "unit", unit: "megabyte", maximumFractionDigits: 1 }).format(
    value / 1_000_000,
  );
}
</script>

<section
  aria-label={`Release details for ${inspection.providerId} ${inspection.version}`}
  class="grid min-w-0 gap-5"
>
  <div class="flex min-w-0 flex-wrap items-start justify-between gap-3">
    <div class="min-w-0">
      <h2 class="text-heading font-[620] text-text-primary">{inspection.name ?? inspection.providerId}</h2>
      {#if inspection.name && inspection.name !== inspection.providerId}
        <p class="mt-0.5 font-mono text-meta text-text-muted">{inspection.providerId}</p>
      {/if}
      {#if inspection.description}
        <p class="mt-2 max-w-2xl text-label leading-5 text-text-muted">{inspection.description}</p>
      {/if}
    </div>
    <Chip tone="neutral">v{inspection.version}</Chip>
  </div>

  <div class={["flex min-w-0 items-start gap-3 rounded-md border px-3.5 py-3", compatibility.surfaceClass]}>
    <span class={["mt-0.5 flex size-7 shrink-0 items-center justify-center rounded-md", compatibility.iconClass]}>
      {#if inspection.compatibility === "compatible"}
        <Check
          aria-hidden="true"
          size={16}
          strokeWidth={2.4}
        />
      {:else}
        <TriangleAlert
          aria-hidden="true"
          size={15}
        />
      {/if}
    </span>
    <div class="min-w-0 flex-1">
      <div class="flex flex-wrap items-center gap-2">
        <p class="text-label font-[620] text-text-primary">{compatibility.title}</p>
        <Chip tone={compatibility.tone}>{compatibility.label}</Chip>
      </div>
      <p class="mt-0.5 text-meta leading-5 text-text-muted">{compatibility.description}</p>
    </div>
  </div>

  <dl class="grid min-w-0 grid-cols-2 overflow-hidden rounded-md border border-border-subtle text-label">
    <div class="min-w-0 border-b border-r border-border-subtle p-3">
      <dt class="text-meta font-medium text-text-subtle">Published</dt>
      <dd class="mt-1 text-text-secondary">{formatTimestamp(inspection.publishedAt)}</dd>
    </div>
    <div class="min-w-0 border-b border-border-subtle p-3">
      <dt class="text-meta font-medium text-text-subtle">Distributor</dt>
      <dd class="mt-1 wrap-break-word text-text-secondary">
        {inspection.distributor ?? inspection.maintainer?.name ?? "Not recorded"}
      </dd>
    </div>
    <div class="min-w-0 border-r border-border-subtle p-3">
      <dt class="text-meta font-medium text-text-subtle">Binary size</dt>
      <dd class="mt-1 text-text-secondary">{formatBytes(inspection.binarySize)}</dd>
    </div>
    <div class="min-w-0 p-3">
      <dt class="text-meta font-medium text-text-subtle">License</dt>
      <dd class="mt-1 text-text-secondary">{inspection.license ?? "Not recorded"}</dd>
    </div>
  </dl>

  <div class="flex min-w-0 items-center gap-3 rounded-md border border-border-subtle bg-surface-muted/30 px-3 py-2.5">
    <div class="min-w-0 flex-1">
      <p class="text-meta font-medium text-text-subtle">Immutable release digest</p>
      <code
        class="mt-0.5 block min-w-0 overflow-hidden text-ellipsis whitespace-nowrap font-mono text-meta text-text-secondary"
        title={inspection.indexDigest}
      >
        {inspection.indexDigest}
      </code>
    </div>
    <button
      aria-label="Copy release digest"
      class="inline-flex size-8 shrink-0 items-center justify-center rounded-md text-text-muted hover:bg-white/5 hover:text-text-primary focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
      onclick={() => void copyDigest()}
      title={copied ? "Copied" : "Copy release digest"}
      type="button"
    >
      {#if copied}
        <Check
          aria-hidden="true"
          size={14}
        />
      {:else}
        <Copy
          aria-hidden="true"
          size={14}
        />
      {/if}
    </button>
  </div>

  <details class="group min-w-0 overflow-hidden rounded-md border border-border-subtle">
    <summary
      class="flex cursor-pointer list-none items-center justify-between gap-3 px-3 py-2.5 text-label font-[560] text-text-secondary hover:bg-white/3 hover:text-text-primary [&::-webkit-details-marker]:hidden"
    >
      Technical details
      <ChevronDown
        aria-hidden="true"
        class="shrink-0 text-text-muted transition-transform group-open:rotate-180"
        size={15}
      />
    </summary>
    <dl class="grid gap-3 border-t border-border-subtle px-3 py-3 text-label sm:grid-cols-2">
      <div>
        <dt class="text-meta text-text-subtle">Target platform</dt>
        <dd class="mt-0.5 font-mono text-text-secondary">{inspection.platform}</dd>
      </div>
      <div>
        <dt class="text-meta text-text-subtle">Available platforms</dt>
        <dd class="mt-0.5 font-mono text-text-secondary">{inspection.platforms.join(", ")}</dd>
      </div>
      {#if requirements.length > 0}
        <div class="sm:col-span-2">
          <dt class="text-meta text-text-subtle">Runtime requirements</dt>
          <dd class="mt-1 flex flex-wrap gap-x-4 gap-y-1">
            {#each requirements as [name, requirement] (name)}
              <span class="font-mono text-meta text-text-secondary">{name} {requirement}</span>
            {/each}
          </dd>
        </div>
      {/if}
      {#if inspection.revision}
        <div class="sm:col-span-2">
          <dt class="text-meta text-text-subtle">Revision</dt>
          <dd class="mt-0.5 break-all font-mono text-meta text-text-secondary">{inspection.revision}</dd>
        </div>
      {/if}
    </dl>
  </details>

  {#if inspection.repositoryUrl || inspection.homepageUrl || inspection.documentationUrl}
    <div class="flex min-w-0 flex-wrap gap-x-4 gap-y-2 border-t border-border-subtle pt-4 text-label">
      {#if inspection.repositoryUrl}
        <a
          class="inline-flex items-center gap-1 text-accent hover:underline"
          href={inspection.repositoryUrl}
          rel="noreferrer"
          target="_blank"
        >
          Source repository
          <ExternalLink
            aria-hidden="true"
            size={13}
          />
        </a>
      {/if}
      {#if inspection.homepageUrl}
        <a
          class="inline-flex items-center gap-1 text-accent hover:underline"
          href={inspection.homepageUrl}
          rel="noreferrer"
          target="_blank"
        >
          Homepage
          <ExternalLink
            aria-hidden="true"
            size={13}
          />
        </a>
      {/if}
      {#if inspection.documentationUrl}
        <a
          class="inline-flex items-center gap-1 text-accent hover:underline"
          href={inspection.documentationUrl}
          rel="noreferrer"
          target="_blank"
        >
          Documentation
          <ExternalLink
            aria-hidden="true"
            size={13}
          />
        </a>
      {/if}
    </div>
  {/if}
</section>
