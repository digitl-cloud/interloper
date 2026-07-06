export interface ComponentDefinition {
    kind: string
    key: string
    path: string
    name: string
    icon: string
    description: string
    config_schema?: Record<string, unknown>
    /** Relation vocabulary: type → allowed dst kinds + whether slotted. */
    relations?: Record<string, { kinds: string[]; slotted: boolean }>
    provider?: string
}

export interface AssetDefinition extends ComponentDefinition {
    source_key: string
    tags: string[]
    /** Slot name → resource catalog key. */
    resources: Record<string, string>
    destinations: string[]
    /** param_name → qualified asset key (e.g. "facebook_ads.campaigns") */
    requires: Record<string, string>
    /** param_name → qualified asset key (optional cross-source deps) */
    optional_requires: Record<string, string>
    partitioning: Record<string, unknown> | null
    asset_schema: JsonSchema | null
}

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

export interface SourceDefinition extends ComponentDefinition {
    tags: string[]
    /** Slot name → resource catalog key. */
    resources: Record<string, string>
    config_schema: Record<string, unknown>
    /** Compatible destination keys. Empty = all destinations compatible. */
    destinations: string[]
    assets: AssetDefinition[]
}

export interface DestinationDefinition extends ComponentDefinition {
    tags: string[]
    /** Slot name → resource catalog key. */
    resources: Record<string, string>
}

export type Catalog = Record<string, ComponentDefinition>
