---
name: build-component
description: Builds or refactors Vue components in interloper-app with clean UI-focused contracts. Use when creating or updating components, props, emits, v-model contracts, or when simplifying component logic.
---

# Build Component

## Purpose

Create components that are UI-focused, readable, and consistent with project architecture.

## Rules

1. Keep components presentation-first:
   - Rendering, local UI interaction, minor formatting.
   - No direct API calls inside components.
2. Prefer explicit contracts:
   - `defineProps` for inputs.
   - `defineEmits` for intent events (`edit`, `delete`, `submit`, `close`).
   - `defineModel` for two-way page-owned state.
3. Avoid store orchestration in leaf components.
   - If a component needs heavy state/mutations, move orchestration to page/container.
4. Keep script sections compact.
   - Extract repeated non-trivial logic into composables.
5. Preserve behavior unless the user asks for a redesign.

## Build Workflow

1. Define component role:
   - Presentational, form, or graph/table renderer.
2. Draft contract first:
   - Props shape, models, emitted events.
3. Implement UI behavior:
   - Keep side effects out.
4. Verify parent integration:
   - Parent handles store mutations, toasts, confirms.
5. Run lint/typecheck after edits.

## Contract Template

```ts
const props = defineProps<{
  item: Item
  loading?: boolean
}>()

const open = defineModel<boolean>('open', { default: false })

const emit = defineEmits<{
  save: [payload: SavePayload]
  remove: [id: string]
}>()
```

## Checklist

- Component has explicit props/models/emits.
- No direct API/database usage in components.
- No hidden mutation side effects.
- Complex logic extracted when it improves clarity.
- Naming and event payloads match nearby project patterns.
