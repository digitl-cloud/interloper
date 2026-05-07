import type { Asset, AssetCreateInput, AssetDependency, AssetUpdateInput } from '~/types/asset'

export const useAssetsStore = defineStore('assets', () => {
    const { apiFetch } = useApi()

    /**********************
     * State
     **********************/
    const assets = ref<Asset[]>([])
    const dependencies = ref<AssetDependency[]>([])
    const loading = ref(false)
    const error = ref<Error | null>(null)

    /**********************
     * Getters
     **********************/
    const standalone = computed(() => assets.value.filter(a => a.source_id === null))

    /**********************
     * Internals
     **********************/
    function _upsert(asset: Asset) {
        const idx = assets.value.findIndex(a => a.id === asset.id)
        if (idx >= 0) assets.value[idx] = { ...assets.value[idx], ...asset }
        else assets.value.push(asset)
    }

    function _remove(id: string) {
        assets.value = assets.value.filter(a => a.id !== id)
    }

    /**********************
     * Actions
     **********************/
    async function fetch() {
        loading.value = true
        error.value = null
        try {
            const [assetList, deps] = await Promise.all([
                apiFetch<Asset[]>('/assets'),
                apiFetch<AssetDependency[]>('/assets/dependencies'),
            ])
            assets.value = assetList
            dependencies.value = deps
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            loading.value = false
        }
    }

    async function fetchOne(id: string): Promise<Asset> {
        return apiFetch<Asset>(`/assets/${id}`)
    }

    async function create(input: AssetCreateInput): Promise<Asset> {
        const asset = await apiFetch<Asset>('/assets', {
            method: 'POST',
            body: input,
        })
        _upsert(asset)
        return asset
    }

    async function update(id: string, input: AssetUpdateInput): Promise<Asset> {
        const asset = await apiFetch<Asset>(`/assets/${id}`, {
            method: 'PUT',
            body: input,
        })
        _upsert(asset)
        return asset
    }

    async function remove(ids: string | string[]) {
        const list = Array.isArray(ids) ? ids : [ids]
        await Promise.all(list.map(id => apiFetch(`/assets/${id}`, { method: 'DELETE' })))
        list.forEach(_remove)
    }

    async function addDependency(assetId: string, upstreamAssetId: string, paramName: string) {
        await apiFetch(`/assets/${assetId}/dependencies`, {
            method: 'POST',
            body: { upstream_asset_id: upstreamAssetId, param_name: paramName },
        })
        dependencies.value.push({ asset_id: assetId, upstream_asset_id: upstreamAssetId })
    }

    async function removeDependency(assetId: string, upstreamAssetId: string) {
        await apiFetch(`/assets/${assetId}/dependencies/${upstreamAssetId}`, {
            method: 'DELETE',
        })
        dependencies.value = dependencies.value.filter(
            d => !(d.asset_id === assetId && d.upstream_asset_id === upstreamAssetId),
        )
    }

    /**********************
     * Lookups
     **********************/
    function findById(id: string): Asset | undefined {
        return assets.value.find(a => a.id === id)
    }

    function bySourceId(sourceId: string): Asset[] {
        return assets.value.filter(a => a.source_id === sourceId)
    }

    function upstreamOf(assetId: string): string[] {
        return dependencies.value
            .filter(d => d.asset_id === assetId)
            .map(d => d.upstream_asset_id)
    }

    function downstreamOf(assetId: string): string[] {
        return dependencies.value
            .filter(d => d.upstream_asset_id === assetId)
            .map(d => d.asset_id)
    }

    function $reset() {
        assets.value = []
        dependencies.value = []
        loading.value = false
        error.value = null
    }

    return {
        assets,
        standalone,
        dependencies,
        loading,
        error,
        fetch,
        fetchOne,
        create,
        update,
        remove,
        addDependency,
        removeDependency,
        findById,
        bySourceId,
        upstreamOf,
        downstreamOf,
        _upsert,
        _remove,
        $reset,
    }
})
