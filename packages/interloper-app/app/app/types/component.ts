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

/** A relation entry embedded on a component (`component.relations[type]`). */
export interface RelationRef {
    dst_id: string
    slot: string
    dst_kind: string
}

/** A standalone relation row from `GET /components/relations`. */
export interface Relation {
    src_id: string
    dst_id: string
    type: string
    slot: string
    dst_kind: string
}

/** Mirrors the API's ComponentResponse — one shape for every kind. */
export interface ComponentRecord {
    id: string
    org_id: string
    kind: string
    key: string
    name: string | null
    status: ComponentStatus
    /** Secret kinds (connection/config/resource): null in list responses, decoded in detail. */
    config: Record<string, any> | null
    /** Runtime state, e.g. job {next_run_at, last_run_at}. */
    state: Record<string, any> | null
    encrypted: boolean
    /** Owning source id for source-owned assets. */
    parent_id: string | null
    relations: Record<string, RelationRef[]>
    /** Sources: their assets (one level deep). */
    children: ComponentRecord[]
    created_at: string | null
    updated_at: string | null
}

/** Relation entry in create/update payloads. */
export interface RelationInput {
    dst_id: string
    slot?: string
}

/** Create/update payload. Each relations type listed is fully replaced. */
export interface ComponentInput {
    kind?: string
    key?: string
    name?: string
    config?: Record<string, any>
    encrypted?: boolean
    /** Sources: child asset keys to keep (omit = all catalog assets). */
    children?: string[]
    relations?: Record<string, RelationInput[]>
}

// ─── Helpers ─────────────────────────────────────────────────────────

/** Whether an asset materializes (clients own `config.materializable`). */
export function materializable(c: ComponentRecord): boolean {
    return c.config?.materializable ?? true
}

/** Relation refs of a given type, e.g. `relationRefs(c, 'destination')`. */
export function relationRefs(c: ComponentRecord, type: string): RelationRef[] {
    return c.relations?.[type] ?? []
}

/** Destination ids of a given relation type. */
export function relationIds(c: ComponentRecord, type: string): string[] {
    return relationRefs(c, type).map(r => r.dst_id)
}

/** Resource relations as a {slot: dst_id} map (the old `resources` field). */
export function resourceMap(c: ComponentRecord): Record<string, string> {
    const map: Record<string, string> = {}
    for (const ref of relationRefs(c, 'resource')) map[ref.slot] = ref.dst_id
    return map
}

// ─── Job accessors (config/state fields) ─────────────────────────────

export function jobCron(c: ComponentRecord): string {
    return c.config?.cron ?? ''
}

export function jobTags(c: ComponentRecord): string[] {
    return c.config?.tags ?? []
}

export function jobEnabled(c: ComponentRecord): boolean {
    return c.config?.enabled ?? true
}

export function jobPartitioned(c: ComponentRecord): boolean {
    return c.config?.partitioned ?? false
}

export function jobBackfillDays(c: ComponentRecord): number | null {
    return c.config?.backfill_days ?? null
}

export function jobNextRunAt(c: ComponentRecord): string | null {
    return c.state?.next_run_at ?? null
}

export function jobLastRunAt(c: ComponentRecord): string | null {
    return c.state?.last_run_at ?? null
}

/** A job's target ids of a given kind ('source' | 'asset'). */
export function jobTargetIds(c: ComponentRecord, kind: string): string[] {
    return relationRefs(c, 'target')
        .filter(r => r.dst_kind === kind)
        .map(r => r.dst_id)
}

// ─── Hook accessors (config fields) ──────────────────────────────────

export function hookEvents(c: ComponentRecord): string[] {
    return c.config?.events ?? []
}

export function hookEnabled(c: ComponentRecord): boolean {
    return c.config?.enabled ?? true
}
