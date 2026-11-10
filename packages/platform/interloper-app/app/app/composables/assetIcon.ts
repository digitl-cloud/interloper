/** asset_id → catalog icon name (e.g. "i-lucide-database"). */
export function useAssetIcon() {
    const componentsStore = useComponentsStore()
    const catalogStore = useCatalogStore()

    const assetIcon = computed(() => {
        const map = new Map<string, string>()
        for (const source of componentsStore.byKind('source')) {
            const sourceDefn = catalogStore.getSourceDefinition(source.key)
            for (const asset of source.children) {
                const assetDefn = sourceDefn?.assets?.find(a => a.key === asset.key)
                if (assetDefn?.icon) map.set(asset.id, assetDefn.icon)
            }
        }
        return map
    })

    return assetIcon
}
