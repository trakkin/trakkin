<script lang="ts">
import "@fontsource-variable/geist/index.css";
import "@fontsource-variable/geist-mono/index.css";
import "./layout.css";
import { browser } from "$app/environment";
import { AppShell, Tooltip } from "$lib";
import { applyThemePalette, createThemePalette, loadThemePaletteSettings } from "$lib/theme/palette";

let { children } = $props();

if (browser) {
  const themePaletteSettings = loadThemePaletteSettings();
  if (themePaletteSettings) applyThemePalette(createThemePalette(themePaletteSettings));
}
</script>

<svelte:head>
  <link
    href="/favicon.svg"
    rel="icon"
    type="image/x-icon"
  >
</svelte:head>
<Tooltip.Provider>
  <AppShell>{@render children()}</AppShell>
</Tooltip.Provider>
