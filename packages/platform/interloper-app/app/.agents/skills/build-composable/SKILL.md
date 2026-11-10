---
name: build-composable
description: Builds focused Vue composables for reusable logic in interloper-app. Use when extracting repeated or complex non-UI logic from components/pages, especially flow state, projection logic, and realtime/lifecycle helpers.
---

# Build Composable

## Purpose

Extract logic into composables when it improves clarity, reuse, and testability without over-fragmenting code.

## When To Extract

- Logic is duplicated across files.
- A component/page script becomes hard to scan.
- Logic is domain-specific but not tied to rendering.
- Lifecycle/realtime patterns repeat.

Do not extract tiny one-off logic that is clearer inline.

## Design Rules

1. Keep composables single-purpose.
2. Prefer explicit inputs/outputs:
   - pass refs/getters as arguments.
   - return named methods and reactive state.
3. Avoid hidden global side effects.
4. Keep naming domain-oriented:
   - `useSourceSheetSession`, `useGraphConnectionRules`, `useScopedRealtimeSubscription`.

## Pattern

```ts
interface UseFeatureOptions {
  source: Ref<Item[]>
  filter: Ref<string>
}

export function useFeature(options: UseFeatureOptions) {
  const result = computed(() => { ... })
  function runAction(...) { ... }
  return { result, runAction }
}
```

## Realtime Helper Guidance

- Centralize channel lifecycle in composables.
- Expose clear hooks:
  - `onEnter`, `onLeave`
  - `onInsert`, `onUpdate`, `onDelete`
  - optional `shouldHandle` guard

## Checklist

- Input contract is explicit and typed.
- Output API is minimal and stable.
- No UI rendering concerns included.
- Extraction genuinely reduces complexity at call sites.
