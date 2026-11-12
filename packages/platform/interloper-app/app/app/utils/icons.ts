/**
 * Look up the icon for a component by its catalog key.
 *
 * Reads the `icon` field from the component definition in the specs store.
 * Falls back to `fallback`, or the asset's concept icon, when the key is not
 * in the catalog or has no icon set.
 *
 * @param key - The component key from the catalog (e.g. `adup_connection`).
 * @param fallback - Icon to use when no icon is found, usually the kind's concept icon.
 */
export function componentIcon(key: string, fallback?: string): string {
    const catalogStore = useCatalogStore()
    return catalogStore.catalog[key]?.icon || fallback || useAppConfig().icons.asset
}
