const DEFAULT_ICON = 'i-lucide-box'

/**
 * Look up the icon for a component by its catalog key.
 *
 * Reads the `icon` field from the component definition in the specs store.
 * Falls back to a generic icon if the key is not in the catalog or has no icon set.
 *
 * @param key - The component key from the catalog (e.g. `adup_connection`).
 * @param fallback - Icon to use when no icon is found.
 */
export function componentIcon(key: string, fallback = DEFAULT_ICON): string {
    const catalogStore = useCatalogStore()
    return catalogStore.catalog[key]?.icon || fallback
}
