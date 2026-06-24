/**
 * Catalog-resolution state of a persisted component (source or asset).
 *
 * Mirrors `interloper_db.drift.ComponentStatus` on the backend, derived from
 * the same resolver hydration uses:
 *   - `ok`       — key resolves in the enabled catalog; live and runnable
 *   - `disabled` — key exists in code but is not exposed by this deployment
 *   - `missing`  — key is gone from the code entirely; this is drift
 */
export type ComponentStatus = 'ok' | 'disabled' | 'missing'
