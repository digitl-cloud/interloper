/** asset_id → "Source Name → Asset Name" */
export function useAssetDisplayName() {
    const componentsStore = useComponentsStore()
    const catalogStore = useCatalogStore()

    const assetDisplayName = computed(() => {
        const map = new Map<string, string>()
        for (const source of componentsStore.byKind('source')) {
            const sourceDefn = catalogStore.getSourceDefinition(source.key)
            const sourceName = source.name ?? sourceDefn?.name ?? source.key
            for (const asset of source.children) {
                const assetDefn = sourceDefn?.assets?.find(a => a.key === asset.key)
                const assetName = assetDefn?.name ?? asset.key
                map.set(asset.id, `${sourceName} → ${assetName}`)
            }
        }
        return map
    })

    return assetDisplayName
}
