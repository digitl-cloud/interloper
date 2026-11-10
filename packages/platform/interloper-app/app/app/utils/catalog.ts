/**
 * Display name of a component's type: the catalog definition behind its key.
 * Source-owned assets live under their source's definition, so the kind picks
 * the lookup. Falls back to the key when the catalog lacks the definition.
 */
export function componentTypeName(kind: string, key: string): string {
    const catalogStore = useCatalogStore()
    const defn = kind === 'asset' ? catalogStore.getAssetDefinition(key) : catalogStore.catalog[key]
    return defn?.name ?? key
}
