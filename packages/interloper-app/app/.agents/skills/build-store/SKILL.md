---
name: build-store
description: Builds and refactors Pinia stores in interloper-app using the project store contract. Use when implementing or updating store state, selectors, queries, commands, and realtime reconciliation behavior.
---

# Build Store

## Purpose

Create robust, consistent stores that own canonical state and persistence concerns.

Follow `STORE_CONTRACT.md`.

## Required Store Shape

1. `state`: refs/computed only
2. internal helpers
3. selectors:
   - `find*ById` nullable
   - `require*ById` throwing
4. queries:
   - `load*`, `reload*`, `fetch*ById`
5. commands:
   - `create*`, `update*`, `delete*`, domain commands

## Rules

1. Store owns API interactions and in-memory reconciliation.
2. No toast/modal/UI concerns in store.
3. Realtime logic should use shared realtime composables.
4. Keep command behavior explicit:
   - throw on failure
   - return created/updated payload when meaningful
5. Keep selectors deterministic and side-effect free.

## Complex Workflow Commands

For multi-step mutations:

- use explicit command names (for example `createSourceWithGraph`)
- isolate sub-steps in internal helpers
- refresh/reconcile canonical state at the end
- keep return shape explicit (`{ id }` or summary object)

## Checklist

- Store API matches contract naming.
- Selector/query/command responsibilities are separated.
- Realtime update logic is consistent and filtered safely.
- Caller-facing API is clear and predictable.
