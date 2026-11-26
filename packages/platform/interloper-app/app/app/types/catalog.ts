import type { OAuthFieldMeta } from '~/types/oauth'

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

export type Maturity = 'deprecated' | 'alpha' | 'beta' | 'stable'

/** Whether a maturity is flagged with a badge; stable, the default, shows nothing. */
export function showsMaturity(maturity?: Maturity | null): maturity is Exclude<Maturity, 'stable'> {
    return maturity === 'deprecated' || maturity === 'alpha' || maturity === 'beta'
}

export interface ComponentDefinition {
    kind: string
    key: string
    path: string
    name: string
    icon: string
    description: string
    tags: string[]
    maturity: Maturity
    /** Key of the definition this one is shown under: its own, unless it declares another (a star schema its platform). */
    group: string
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
    /** An editable reference (`il.reference(editable=True)`): the app may rewrite its rows whole. */
    editable: boolean
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

/** The config field marked `x-discriminator`, which tells a definition's instances apart; null when none is. */
export function discriminatorField(defn: ComponentDefinition | undefined): string | null {
    const properties = (defn?.config_schema?.properties ?? {}) as Record<string, Record<string, unknown>>
    return Object.entries(properties).find(([, property]) => property['x-discriminator'])?.[0] ?? null
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

/** A JSON Schema as Pydantic's `model_json_schema()` writes it: a component's config, an asset's rows. */
export interface JsonSchema {
    type?: string
    title?: string
    description?: string
    properties?: Record<string, JsonSchemaProperty>
    required?: string[]
    $defs?: Record<string, JsonSchemaProperty>
    /** Sign-in through an OAuth provider fills the connection's credential fields. */
    'x-oauth'?: OAuthFieldMeta
    [key: string]: unknown
}

/** A field whose options the server resolves: `/catalog/{key}/resolve` calls `<relation>.<method>`. */
export interface FetchFieldMeta {
    provider: string
    label_key: string
    value_key: string
}

/** One property of a JSON Schema, with the `x-*` hints the app's forms read. */
export interface JsonSchemaProperty {
    type?: string | string[]
    format?: string
    title?: string
    description?: string
    default?: unknown
    enum?: unknown[]
    $ref?: string
    allOf?: JsonSchemaProperty[]
    anyOf?: JsonSchemaProperty[]
    items?: JsonSchemaProperty
    properties?: Record<string, JsonSchemaProperty>
    required?: string[]
    /** Set by `resolveSchemaProperty` on an `anyOf` with a `null` branch (Pydantic's `Optional`). */
    nullable?: boolean
    minItems?: number
    minimum?: number
    maximum?: number
    minLength?: number
    maxLength?: number
    'x-widget'?: string
    'x-options'?: { label: string, value: string }[]
    'x-options-from'?: string
    'x-fetch'?: FetchFieldMeta
    'x-discriminator'?: boolean
    'x-info'?: string
    'x-section'?: string
    [key: string]: unknown
}

/**
 * A property with its `$ref`, `allOf` and `anyOf` inlined against the schema's
 * `$defs`, so an enum (a ref into `$defs`) or an optional field (an `anyOf`
 * with a `null` branch) reads like a plain property. Sibling keys (default,
 * description, x-*) win over the referenced definition's; `anyOf` resolves to
 * its first non-null branch and sets `nullable`.
 */
export function resolveSchemaProperty(property: JsonSchemaProperty, defs: Record<string, JsonSchemaProperty> = {}): JsonSchemaProperty {
    const deref = (part: JsonSchemaProperty): JsonSchemaProperty => {
        if (!part.$ref) return part
        const { $ref, ...siblings } = part
        // Drop the definition's own title/description: they describe the
        // type (e.g. an enum's class name and docstring), not the field.
        const { title: _title, description: _description, ...target } = defs[$ref.split('/').pop() ?? ''] ?? {}
        return { ...target, ...siblings }
    }
    let resolved = deref(property)
    if (resolved.allOf) {
        const { allOf, ...siblings } = resolved
        resolved = { ...allOf.map(deref).reduce((merged, part) => ({ ...merged, ...part }), {}), ...siblings }
    }
    if (resolved.anyOf) {
        const { anyOf, ...siblings } = resolved
        const branch = anyOf.find(candidate => candidate.type !== 'null') ?? anyOf[0] ?? {}
        resolved = { ...deref(branch), ...siblings, nullable: anyOf.some(candidate => candidate.type === 'null') }
    }
    if (resolved.items) {
        resolved = { ...resolved, items: resolveSchemaProperty(resolved.items, defs) }
    }
    return resolved
}

/**
 * The widget a resolved property edits with: its `x-widget` hint, else one
 * inferred from its type: `select` for an enum or options, `object` for a
 * nested model, `date` and `datetime` for the string formats, `switch`,
 * `number`, `array`, and `text` for everything else.
 */
export function schemaWidget(property: JsonSchemaProperty): string {
    if (property['x-widget']) return property['x-widget']
    if (property['x-options'] || property['x-options-from'] || property.enum) return 'select'
    if (property.type === 'object' && property.properties) return 'object'
    if (property.format === 'date') return 'date'
    if (property.format === 'date-time') return 'datetime'
    switch (property.type) {
        case 'boolean': return 'switch'
        case 'integer':
        case 'number': return 'number'
        case 'array': return 'array'
        default: return 'text'
    }
}

/** A column of an asset's rows as its schema declares it, and the widget a cell edits it with. */
export interface SchemaColumn {
    key: string
    /** The column's property, resolved. */
    property: JsonSchemaProperty
    widget: string
    /** A row must hold a value: the schema requires the column and does not accept null. */
    required: boolean
}

/** The columns an asset's schema declares, in its order; none without a schema. */
export function schemaColumns(schema: JsonSchema | null | undefined): SchemaColumn[] {
    const required = new Set(schema?.required ?? [])
    return Object.entries(schema?.properties ?? {}).map(([key, declared]) => {
        const property = resolveSchemaProperty(declared, schema?.$defs)
        return { key, property, widget: schemaWidget(property), required: required.has(key) && !property.nullable }
    })
}
