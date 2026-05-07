---
name: build-page
description: Builds or refactors Nuxt pages in interloper-app as orchestration layers. Use when implementing page state flow, wiring components to stores, handling toasts/confirms, or coordinating panel/sheet navigation.
---

# Build Page

## Purpose

Implement pages as orchestration containers that own screen state and delegate UI rendering to components.

## Rules

1. Pages own orchestration:
   - Selected item IDs.
   - Drawer/modal/panel visibility.
   - Store command calls.
   - Toast/confirm side effects.
2. Pages pass state down:
   - Use props and `v-model:*` on children.
   - Children emit intents; pages execute mutations.
3. Avoid view logic sprawl:
   - If orchestration grows, extract a page-level composable.
4. Keep page behavior explicit:
   - Prefer named handlers over inline complex expressions.

## Workflow

1. Define page state refs/computed.
2. Wire store refs/actions needed by this page.
3. Connect child events to page handlers.
4. Keep child components presentational where possible.
5. Validate behavior paths (create/edit/delete/select/close).

## Handler Pattern

```ts
async function onDelete(item: Item) {
  const confirmed = await confirm({ title: 'Delete item' })
  if (!confirmed) return
  try {
    await store.deleteItem(item.id)
    toast.add({ title: 'Deleted', color: 'success' })
  } catch {
    toast.add({ title: 'Delete failed', color: 'error' })
  }
}
```

## Checklist

- Page controls all side effects for this screen.
- Child components receive explicit state and callbacks via emits.
- Store operations are called from page handlers, not deep leaf components.
- Flow is readable and easy to trace from UI action to store command.
