---
name: bits-ui
description: 'Guides Bits UI work in the Svelte 5 frontend. Use when implementing, debugging, migrating, or reviewing primitives, component parts, snippets, state, accessibility, composition, or Bits UI API changes.'
---

# Bits UI

## Overview

Ground Bits UI changes in the installed package and current documentation while preserving Trakkin's component boundaries and interaction accessibility.

## Sources

- Documentation index: `https://bits-ui.com/docs/llms.txt`
- Package manifest: `frontend/package.json`
- Installed package: `frontend/node_modules/bits-ui`

## Workflow

1. Inspect the nearest consumer and any existing wrapper under `frontend/src/lib/components/ui/`.
2. Find the primitive's current documentation. Check installed exports and types when the docs and installed version may differ.
3. Reuse or extend a local wrapper when it represents a shared Trakkin control; consume a primitive directly when no wrapper adds a useful contract.
4. Use documented parts, props, bindings, snippets, events, and `data-*` states. Do not recreate behavior the primitive already owns.
5. Preserve trigger/content relationships, accessible names, keyboard and focus behavior, disabled states, and meaningful loading or validation states.
6. Implement the smallest coherent change, then exercise the interaction and run the narrowest available frontend check.

## Trakkin Conventions

- Shared wrappers compose Bits UI primitives with Svelte 5 snippets and forward supported props through `$props()`.
- Wrapper styling preserves consumer `class` values while applying the local component vocabulary.
- Existing primitives include alert dialogs, buttons, checkboxes, dialogs, dropdown menus, selects, switches, tabs, and tooltips.
- Direct primitive use is acceptable for feature-local composition, such as a popover without a shared wrapper contract.

## Stop and Verify

- Stop if an API detail comes only from memory; verify it against the installed types or documentation.
- Stop if a wrapper drops supported props, snippets, bindings, or the consumer's `class` value.
- Stop if custom composition bypasses the primitive's focus, keyboard, labeling, dismissal, or disabled-state behavior.
- Adapt documentation examples to the installed Svelte and Bits UI versions rather than copying them verbatim.

## Validation

- Exercise the changed interaction with keyboard and pointer input; verify focus entry, movement, dismissal, and return where applicable.
- Frontend diagnostics: `pnpm --dir frontend lint`
- Production integration: `pnpm --dir frontend build`
- The frontend currently has no component test suite; do not claim automated interaction coverage from `pnpm --dir frontend test`.
