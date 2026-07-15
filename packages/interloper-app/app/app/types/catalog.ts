/** A declared slot on a slotted relation type. */
export interface RelationSlot {
    /** Expected dst component key. `''` accepts any component of the relation's kinds. */
    key: string
    required: boolean
}

/** One relation type a component kind may declare toward other components. */
export interface RelationDefinition {
    /** Allowed dst component kinds. */
    kinds: string[]
    slotted: boolean
    /** Allowed dst component keys. Empty = any of `kinds`. Set for `destination`. */
    keys: string[]
    /**
     * Declared slots. Set for `resource` (slot → resource key) and
     * `dependency` (param → upstream asset key, possibly qualified
     * `source_key.asset_key`; `required` flag meaningful).
     */
    slots: Record<string, RelationSlot>
    /**
     * What deleting the relation's destination does to the referrer:
     * `block` refuses the deletion, `detach` cascades the relation away
     * (job targets, hook watches). Optional slots detach regardless.
     */
    on_delete: 'block' | 'detach'
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
    /** Relation vocabulary: type → allowed dst kinds, keys and slots. */
    relations: Record<string, RelationDefinition>
    provider?: string
    /** Whether the type implements a live connection check (resources only). */
    checkable?: boolean
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

/** Slot name → resource catalog key, from the definition's `resource` relation. */
export function resourceSlots(defn: ComponentDefinition): Record<string, string> {
    const slots = defn.relations?.resource?.slots ?? {}
    return Object.fromEntries(Object.entries(slots).map(([slot, s]) => [slot, s.key]))
}

/** Dependency slots: param name → upstream asset key + required flag. */
export function dependencySlots(defn: ComponentDefinition): Record<string, RelationSlot> {
    return defn.relations?.dependency?.slots ?? {}
}

/** Required dependency params → upstream asset key (bare or qualified). */
export function requiredDependencies(defn: ComponentDefinition): Record<string, string> {
    return Object.fromEntries(
        Object.entries(dependencySlots(defn))
            .filter(([, s]) => s.required)
            .map(([param, s]) => [param, s.key]),
    )
}

/** Compatible destination keys from the `destination` relation. Empty = all compatible. */
export function allowedDestinationKeys(defn: ComponentDefinition): string[] {
    return defn.relations?.destination?.keys ?? []
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

/** Columns for a definition's `state_schema` properties. `_at` keys are datetimes. */
export function stateColumns(defn: ComponentDefinition): StateColumn[] {
    const properties = (defn.state_schema?.properties ?? {}) as Record<string, JsonSchemaProperty>
    return Object.entries(properties).map(([key, prop]) => ({
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
