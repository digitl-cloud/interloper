import type { AssetRows } from '~/types/rows'

/**
 * An asset's rows, through its own destinations: `read` pages them (`limit`
 * 1 to 10,000, the most any read takes, 100 by default), keeping, given a `search`, the rows with a text
 * cell that contains it, case ignored; `write` rewrites an editable asset's
 * rows whole, naming the version they were read at (a 409 says the table
 * changed since); `run` queues a run of a component (a source, an asset, a
 * job) and returns its id, through the runs store, so the run shows up live
 * across the app.
 */
export function useAssetRows() {
    const { apiFetch } = useOrgApi()
    const runsStore = useRunsStore()

    function read(assetId: string, options: { search?: string, limit?: number, offset?: number } = {}): Promise<AssetRows> {
        const { search, limit, offset } = options
        return apiFetch<AssetRows>(`/assets/${assetId}/rows`, {
            query: { limit, offset, search: search || undefined },
        })
    }

    function write(assetId: string, rows: Record<string, unknown>[], version: string): Promise<AssetRows> {
        return apiFetch<AssetRows>(`/assets/${assetId}/rows`, { method: 'PUT', body: { rows, version } })
    }

    function run(componentId: string): Promise<string> {
        return runsStore.createRun(componentId)
    }

    return { read, write, run }
}
