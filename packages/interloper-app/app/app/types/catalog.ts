/** One relation a component kind may declare toward other components, named by its record key. */
export interface RelationDefinition {
    /** Allowed dst component kind(s). */
    kind: string | string[]
    /** Allowed dst component key(s). '' accepts any component of the relation's kind(s). */
    key: string | string[]
    /** Whether the relation binds several components (a fan-in). */
    many: boolean
    /** Whether the relation may stay unbound. */
    optional: boolean
    /**
     * What deleting the relation's destination does to the referrer, and the
     * only thing that decides it: `block` refuses the deletion, `detach`
     * drops the relation and lets it through (job targets, hook watches).
     * `optional` has no say here, it governs unbinding.
     */
    on_delete: 'block' | 'detach'
    name: string
}

export interface ComponentDefinition {
    kind: string
    key: string
    path: string
    name: string
    icon: string
    description: string
    tags: string[]
    config_schema: Record<string, unknown>
    /** JSON Schema of the kind's machine-owned state (`{}` = stateless). */
    state_schema: Record<string, unknown>
    /** Relation vocabulary: name → allowed dst kind(s), key(s) and cardinality. */
    relations: Record<string, RelationDefinition>
    provider?: string
    /** Whether the type implements a live connection check (resources only). */
    checkable?: boolean
    /** Whether the type can renew its stored credential (connections only). */
    renewable?: boolean
}

export interface AssetDefinition extends ComponentDefinition {
    source_key: string
    partitioning: Record<string, unknown> | null
    asset_schema: JsonSchema | null
}

export interface SourceDefinition extends ComponentDefinition {
    assets: AssetDefinition[]
}

export type DestinationDefinition = ComponentDefinition

export type Catalog = Record<string, ComponentDefinition>

// ─── Relation helpers ────────────────────────────────────────────────

/** Resource kinds: connection/config/resource relations carry secrets or settings, not data. */
export const RESOURCE_KINDS = ['connection', 'config', 'resource'] as const

/** Source part of a relation key that accepts a match from any source (`*.campaigns`). */
export const ANY_SOURCE = '*'

/** A relation's dst kind(s) as an array, whether declared singular or plural. */
export function kindsOf(r: RelationDefinition): string[] {
    return Array.isArray(r.kind) ? r.kind : [r.kind]
}

/** A relation's dst key(s) as an array, `''` (any key) filtered out. */
export function keysOf(r: RelationDefinition): string[] {
    const keys = Array.isArray(r.key) ? r.key : [r.key]
    return keys.filter(k => k !== '')
}

/** Relations whose kind is a resource kind (connection/config/resource), keyed by name. */
export function resourceRelations(defn: ComponentDefinition): Record<string, RelationDefinition> {
    return Object.fromEntries(
        Object.entries(defn.relations ?? {})
            .filter(([, r]) => kindsOf(r).some(k => (RESOURCE_KINDS as readonly string[]).includes(k))),
    )
}

/**
 * An asset's declared upstreams: its relations whose kind includes 'asset',
 * keyed by name. Guarded on the definition's own kind, since a job's
 * `targets` and a hook's `watches` accept assets too without being upstreams.
 */
export function upstreamRelations(defn: ComponentDefinition): Record<string, RelationDefinition> {
    if (defn.kind !== 'asset') return {}
    return Object.fromEntries(
        Object.entries(defn.relations ?? {}).filter(([, r]) => kindsOf(r).includes('asset')),
    )
}

/** Required (non-optional) upstream relations → upstream asset key (first key, bare or qualified). */
export function requiredUpstreams(defn: ComponentDefinition): Record<string, string> {
    return Object.fromEntries(
        Object.entries(upstreamRelations(defn))
            .filter(([, r]) => !r.optional)
            .map(([name, r]) => [name, keysOf(r)[0] ?? '']),
    )
}

/** Compatible destination keys from the `destinations` relation. Empty = all compatible. */
export function allowedDestinationKeys(defn: ComponentDefinition): string[] {
    const destinations = defn.relations?.destinations
    return destinations ? keysOf(destinations) : []
}

// ─── State schema ────────────────────────────────────────────────────

/** A displayable column derived from a definition's `state_schema`. */
export interface StateColumn {
    key: string
    label: string
    format: 'datetime' | 'text'
}

/** Humanize a snake_case key: `last_run_at` → "Last Run At". */
function humanizeKey(key: string): string {
    return key.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ')
}

/** Columns for a definition's `state_schema` properties. `_at` keys are datetimes; `x-hidden` fields are plumbing. */
export function stateColumns(defn: ComponentDefinition): StateColumn[] {
    const properties = (defn.state_schema?.properties ?? {}) as Record<string, JsonSchemaProperty>
    return Object.entries(properties)
        .filter(([, prop]) => !(prop as Record<string, unknown>)['x-hidden'])
        .map(([key, prop]) => ({
            key,
            label: prop.title ?? humanizeKey(key),
            format: key.endsWith('_at') ? 'datetime' as const : 'text' as const,
        }))
}

// ─── Qualified keys ──────────────────────────────────────────────────

/**
 * Parse a qualified key into source_key and asset_key.
 * "facebook_ads.campaigns" → { sourceKey: "facebook_ads", assetKey: "campaigns" }
 * "campaigns" → { sourceKey: "", assetKey: "campaigns" }
 */
export function parseQualifiedKey(qk: string): { sourceKey: string; assetKey: string } {
    const dot = qk.indexOf('.')
    if (dot === -1) return { sourceKey: '', assetKey: qk }
    return { sourceKey: qk.substring(0, dot), assetKey: qk.substring(dot + 1) }
}

/**
 * Build a qualified key from source and asset keys.
 */
export function qualifiedKey(sourceKey: string, assetKey: string): string {
    return sourceKey ? `${sourceKey}.${assetKey}` : assetKey
}

// ─── JSON Schema ─────────────────────────────────────────────────────

/** Minimal JSON Schema representation for asset output schemas. */
export interface JsonSchema {
    type?: string
    title?: string
    description?: string
    properties?: Record<string, JsonSchemaProperty>
    required?: string[]
    [key: string]: unknown
}

export interface JsonSchemaProperty {
    type?: string | string[]
    title?: string
    description?: string
    format?: string
    anyOf?: Array<{ type?: string; format?: string }>
    [key: string]: unknown
}
