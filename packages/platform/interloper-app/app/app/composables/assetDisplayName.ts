export interface AssetNames {
    sourceName: string
    sourceIcon: string
    assetName: string
    /** Combined "Source Name → Asset Name" label. */
    label: string
}

/** asset_id → display names of the asset and its source */
export function useAssetDisplayName() {
    const componentsStore = useComponentsStore()
    const catalogStore = useCatalogStore()

    const assetDisplayName = computed(() => {
        const map = new Map<string, AssetNames>()
        for (const source of componentsStore.byKind('source')) {
            const sourceDefn = catalogStore.getSourceDefinition(source.key)
            const sourceName = source.name ?? sourceDefn?.name ?? source.key
            for (const asset of source.children) {
                const assetDefn = sourceDefn?.assets?.find(a => a.key === asset.key)
                const assetName = assetDefn?.name ?? asset.key
                map.set(asset.id, {
                    sourceName,
                    sourceIcon: componentIcon(source.key),
                    assetName,
                    label: `${sourceName} → ${assetName}`,
                })
            }
        }
        return map
    })

    return assetDisplayName
}
