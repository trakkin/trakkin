---
name: tailwind-css-v4
description: 'Implement, debug, migrate, or review Tailwind CSS v4 styling in the Svelte frontend. Use when: working with utilities, variants, theme variables, directives, source detection, responsive styles, or v3-to-v4 differences.'
---

# Tailwind CSS v4

Use Tailwind v4's CSS-first model and the installed frontend toolchain.

## Sources

- Documentation index: `https://raw.githubusercontent.com/tailwindlabs/tailwindcss.com/refs/heads/md-endpoints/llms.txt`
- Frontend configuration: `frontend/package.json` and `frontend/vite.config.ts`
- Frontend stylesheet: `frontend/src/routes/layout.css`

## Reliable Facts

- The installed Tailwind version and generated CSS determine what works in this checkout. The v4 reference supplies current utility, variant, directive, and migration semantics.
- Tailwind v4 is CSS-first. `@theme`, `@source`, `@utility`, `@custom-variant`, and `@reference` solve different configuration and reuse problems without requiring a v3-style JavaScript config.
- Utility candidates must be statically discoverable; constructing partial class names at runtime prevents reliable generation.
- Theme namespaces determine which variables generate utilities, and interactive states still need usable responsive, focus, disabled, and reduced-motion behavior where relevant.

## Working Approach

- Inspect the nearest Svelte and CSS context plus the relevant documentation needed to resolve the task; consult package or plugin setup when behavior appears version-sensitive.
- Choose the v4 mechanism that best expresses the intent rather than applying every directive by recipe. Prefer existing theme values or a genuinely shared token when they fit the local visual language.
- Represent dynamic states with complete class-name alternatives so source detection remains predictable.

## Validation

Use a focused visual or generated-CSS check when possible. `pnpm --dir frontend check` and `pnpm --dir frontend lint` are useful for broader frontend changes, while `pnpm --dir frontend build` is especially relevant when integration, source detection, directives, or theme configuration changes.
