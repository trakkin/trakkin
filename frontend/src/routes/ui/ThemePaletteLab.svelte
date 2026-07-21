<script lang="ts">
import { Check, Copy, RotateCcw, Shuffle } from "@lucide/svelte";
import { onMount } from "svelte";
import { Button, IconButton, Tooltip } from "$lib";
import {
  applyThemePalette,
  createThemePalette,
  defaultThemePaletteSettings,
  loadThemePaletteSettings,
  randomThemePaletteSettings,
  saveThemePaletteSettings,
  type ThemePaletteSettings,
  themePaletteCss,
} from "$lib/theme/palette";

let settings = $state<ThemePaletteSettings>({ ...defaultThemePaletteSettings });
let ready = $state(false);
let copied = $state(false);
let copyTimer: ReturnType<typeof setTimeout> | undefined;
let palette = $derived(createThemePalette(settings));
let surfaceSwatches = $derived([
  { label: "Canvas", token: "--surface-canvas", value: palette.surfaceCanvas },
  { label: "Sidebar", token: "--surface-sidebar", value: palette.surfaceSidebar },
  { label: "Panel", token: "--surface-panel", value: palette.surfacePanel },
  { label: "Raised", token: "--surface-raised", value: palette.surfaceRaised },
  { label: "Selected", token: "--surface-selected", value: palette.surfaceSelected },
]);

onMount(() => {
  const storedSettings = loadThemePaletteSettings();
  if (storedSettings) settings = storedSettings;
  ready = true;

  return () => {
    if (copyTimer) clearTimeout(copyTimer);
  };
});

$effect(() => {
  if (!ready) return;
  applyThemePalette(palette);
  saveThemePaletteSettings(settings);
});

function randomize() {
  settings = randomThemePaletteSettings();
}

function reset() {
  settings = { ...defaultThemePaletteSettings };
}

async function copyCss() {
  await navigator.clipboard.writeText(themePaletteCss(palette));
  copied = true;
  if (copyTimer) clearTimeout(copyTimer);
  copyTimer = setTimeout(() => (copied = false), 1600);
}
</script>

<div class="overflow-hidden rounded-panel border border-border-strong bg-surface-sidebar">
  <div class="grid h-18 grid-cols-5">
    {#each surfaceSwatches as swatch}
      <div
        class="relative min-w-0 border-r border-black/15 last:border-r-0"
        style:background-color={swatch.value}
      >
        <span
          class="absolute right-2 bottom-1.5 left-2 truncate font-mono text-[0.625rem] uppercase text-text-secondary mix-blend-plus-lighter"
          >{swatch.value}</span
        >
      </div>
    {/each}
  </div>

  <div class="grid gap-px border-t border-border-subtle bg-border-subtle lg:grid-cols-[minmax(0,1fr)_15rem]">
    <div class="min-w-0 space-y-5 bg-surface-panel p-4 sm:p-5">
      <div class="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p class="text-heading font-[560] text-text-primary">Surface palette</p>
          <p class="mt-1 max-w-xl text-label text-text-muted">
            Surfaces share one restrained hue. The accent stays locked to the Trakkin signal color.
          </p>
        </div>
        <Button
          onclick={randomize}
          size="sm"
          variant="primary"
        >
          <Shuffle
            aria-hidden="true"
            size={13}
          />
          Randomize
        </Button>
      </div>

      <div class="grid gap-4 sm:grid-cols-3">
        <label class="block min-w-0">
          <span class="flex items-center justify-between gap-3 text-meta text-text-muted">
            Surface hue
            <span class="font-mono text-text-secondary">{Math.round(settings.hue)}&deg;</span>
          </span>
          <input
            aria-label="Surface hue"
            class="palette-slider mt-2 w-full"
            max="360"
            min="0"
            step="1"
            type="range"
            bind:value={settings.hue}
          >
        </label>
        <label class="block min-w-0">
          <span class="flex items-center justify-between gap-3 text-meta text-text-muted">
            Brightness
            <span class="font-mono text-text-secondary">{Math.round(settings.lightness * 100)}%</span>
          </span>
          <input
            aria-label="Surface brightness"
            class="palette-slider mt-2 w-full"
            max="0.22"
            min="0.12"
            step="0.001"
            type="range"
            bind:value={settings.lightness}
          >
        </label>
        <label class="block min-w-0">
          <span class="flex items-center justify-between gap-3 text-meta text-text-muted">
            Color presence
            <span class="font-mono text-text-secondary">{settings.chroma.toFixed(3)}</span>
          </span>
          <input
            aria-label="Surface color presence"
            class="palette-slider mt-2 w-full"
            max="0.045"
            min="0"
            step="0.001"
            type="range"
            bind:value={settings.chroma}
          >
        </label>
      </div>
    </div>

    <div class="flex min-w-0 flex-col bg-surface-raised/35 p-4">
      <div class="flex items-center gap-3">
        <span
          aria-hidden="true"
          class="size-8 shrink-0 rounded-control border border-border-strong"
          style:background-color={palette.accent}
        ></span>
        <div class="min-w-0">
          <p class="text-meta text-text-muted">Accent</p>
          <p class="truncate font-mono text-label uppercase text-text-primary">{palette.accent}</p>
        </div>
      </div>

      <dl class="mt-4 space-y-2 border-t border-border-subtle pt-3">
        {#each surfaceSwatches as swatch}
          <div class="flex min-w-0 items-center justify-between gap-3 text-meta">
            <dt class="truncate text-text-muted">{swatch.label}</dt>
            <dd class="shrink-0 font-mono uppercase text-text-secondary">{swatch.value}</dd>
          </div>
        {/each}
      </dl>

      <div class="mt-4 flex items-center justify-end gap-1.5 border-t border-border-subtle pt-3">
        <Tooltip.Root>
          <Tooltip.Trigger>
            {#snippet child({ props })}
              <IconButton
                {...props}
                label="Restore starting palette"
                onclick={reset}
                size="sm"
              >
                <RotateCcw
                  aria-hidden="true"
                  size={13}
                />
              </IconButton>
            {/snippet}
          </Tooltip.Trigger>
          <Tooltip.Content>Restore starting palette</Tooltip.Content>
        </Tooltip.Root>
        <Button
          onclick={copyCss}
          size="sm"
          variant="secondary"
        >
          {#if copied}
            <Check
              aria-hidden="true"
              size={13}
            />
            Copied
          {:else}
            <Copy
              aria-hidden="true"
              size={13}
            />
            Copy CSS
          {/if}
        </Button>
      </div>
      <p
        aria-live="polite"
        class="sr-only"
      >
        {copied ? "Palette CSS copied" : ""}
      </p>
    </div>
  </div>
</div>

<style>
.palette-slider {
  height: 1rem;
  accent-color: var(--accent);
  cursor: pointer;
}

.palette-slider:focus-visible {
  outline: none;
}

.palette-slider:focus-visible::-webkit-slider-thumb {
  outline: 2px solid var(--focus-ring);
  outline-offset: 2px;
}

.palette-slider:focus-visible::-moz-range-thumb {
  outline: 2px solid var(--focus-ring);
  outline-offset: 2px;
}
</style>
