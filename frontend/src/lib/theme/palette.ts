import { defineVersionedLocalStorage } from "$lib/storage/versionedLocalStorage";

export type ThemePaletteSettings = {
  hue: number;
  lightness: number;
  chroma: number;
};

export type ThemePalette = {
  surfaceCanvas: string;
  surfaceSidebar: string;
  surfacePanel: string;
  surfaceRaised: string;
  surfaceSelected: string;
  textPrimary: string;
  textSecondary: string;
  textMuted: string;
  textDisabled: string;
  accent: string;
  accentContrast: string;
};

export const defaultThemePaletteSettings: ThemePaletteSettings = {
  hue: 164,
  lightness: 0.12,
  chroma: 0.007,
};

const themePalettePreference = defineVersionedLocalStorage<ThemePaletteSettings>({
  name: "theme-palette",
  version: 1,
  decode: decodeThemePaletteSettings,
});

export const themePaletteStorageKey = themePalettePreference.key;

const brandAccent = "#b9f45b";

const paletteVariables: Record<keyof ThemePalette, string> = {
  surfaceCanvas: "--surface-canvas",
  surfaceSidebar: "--surface-sidebar",
  surfacePanel: "--surface-panel",
  surfaceRaised: "--surface-raised",
  surfaceSelected: "--surface-selected",
  textPrimary: "--text-primary",
  textSecondary: "--text-secondary",
  textMuted: "--text-muted",
  textDisabled: "--text-disabled",
  accent: "--accent",
  accentContrast: "--accent-contrast",
};

export function createThemePalette(settings: ThemePaletteSettings): ThemePalette {
  const surface = (offset: number, chromaScale = 1) =>
    oklchToHex(settings.lightness + offset, settings.chroma * chromaScale, settings.hue);

  return {
    surfaceCanvas: surface(0, 0.72),
    surfaceSidebar: surface(0.016, 0.86),
    surfacePanel: surface(0.032),
    surfaceRaised: surface(0.062, 1.08),
    surfaceSelected: surface(0.092, 1.14),
    textPrimary: oklchToHex(0.965, 0.006, settings.hue),
    textSecondary: oklchToHex(0.84, 0.012, settings.hue),
    textMuted: oklchToHex(0.66, 0.016, settings.hue),
    textDisabled: oklchToHex(0.5, 0.014, settings.hue),
    accent: brandAccent,
    accentContrast: oklchToHex(0.15, 0.01, settings.hue),
  };
}

export function randomThemePaletteSettings(): ThemePaletteSettings {
  return {
    hue: Math.round(Math.random() * 360),
    lightness: roundTo(0.125 + Math.random() * 0.07, 3),
    chroma: roundTo(0.008 + Math.random() * 0.026, 3),
  };
}

export function applyThemePalette(palette: ThemePalette): void {
  for (const [token, value] of Object.entries(palette) as [keyof ThemePalette, string][]) {
    document.documentElement.style.setProperty(paletteVariables[token], value);
  }
}

export function clearThemePalette(): void {
  for (const variable of Object.values(paletteVariables)) {
    document.documentElement.style.removeProperty(variable);
  }
  themePalettePreference.clear();
}

export function saveThemePaletteSettings(settings: ThemePaletteSettings): void {
  themePalettePreference.write(settings);
}

export function loadThemePaletteSettings(): ThemePaletteSettings | null {
  return themePalettePreference.read() ?? null;
}

export function themePaletteCss(palette: ThemePalette): string {
  const declarations = (Object.entries(palette) as [keyof ThemePalette, string][])
    .map(([token, value]) => `  ${paletteVariables[token]}: ${value};`)
    .join("\n");
  return `:root {\n${declarations}\n}`;
}

function oklchToHex(lightness: number, chroma: number, hue: number): string {
  const hueRadians = (normalizeHue(hue) * Math.PI) / 180;
  const labA = chroma * Math.cos(hueRadians);
  const labB = chroma * Math.sin(hueRadians);

  const lPrime = lightness + 0.3963377774 * labA + 0.2158037573 * labB;
  const mPrime = lightness - 0.1055613458 * labA - 0.0638541728 * labB;
  const sPrime = lightness - 0.0894841775 * labA - 1.291485548 * labB;
  const linearL = lPrime ** 3;
  const linearM = mPrime ** 3;
  const linearS = sPrime ** 3;

  const red = 4.0767416621 * linearL - 3.3077115913 * linearM + 0.2309699292 * linearS;
  const green = -1.2684380046 * linearL + 2.6097574011 * linearM - 0.3413193965 * linearS;
  const blue = -0.0041960863 * linearL - 0.7034186147 * linearM + 1.707614701 * linearS;

  return `#${[red, green, blue].map((channel) => toHex(gammaCorrect(channel))).join("")}`;
}

function gammaCorrect(channel: number): number {
  const corrected = channel <= 0.0031308 ? 12.92 * channel : 1.055 * channel ** (1 / 2.4) - 0.055;
  return clamp(corrected, 0, 1);
}

function toHex(channel: number): string {
  return Math.round(channel * 255)
    .toString(16)
    .padStart(2, "0");
}

function decodeThemePaletteSettings(value: unknown): ThemePaletteSettings | undefined {
  if (!isRecord(value)) return undefined;
  const { hue, lightness, chroma } = value;
  if (![hue, lightness, chroma].every((setting) => typeof setting === "number" && Number.isFinite(setting))) {
    return undefined;
  }
  return {
    hue: normalizeHue(hue as number),
    lightness: clamp(lightness as number, 0.1, 0.24),
    chroma: clamp(chroma as number, 0, 0.05),
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeHue(hue: number): number {
  return ((hue % 360) + 360) % 360;
}

function roundTo(value: number, places: number): number {
  const scale = 10 ** places;
  return Math.round(value * scale) / scale;
}

function clamp(value: number, minimum: number, maximum: number): number {
  return Math.min(Math.max(value, minimum), maximum);
}
