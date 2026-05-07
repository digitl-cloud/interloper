import type { AssetExecution } from '~/types/asset_execution'

export const useAssetExecutionsStore = defineStore('assetExecutions', () => {
    const { apiFetch } = useApi()
    const orgStore = useOrganisationStore()

    /**********************
     * State
     **********************/
    const runId = ref<string | null>(null)
    const assetExecutions = ref<AssetExecution[]>([])
    const loading = ref(false)
    const error = ref<Error | null>(null)

    /**********************
     * Realtime
     **********************/
    // Re-fetch asset executions when new events arrive for the current run.
    // The asset_executions view aggregates events, so a full re-fetch is
    // the simplest way to stay in sync.
    useRealtimeSubscription({
        table: 'events',
        scope: () => runId.value ? orgStore.organisation?.id : null,
        shouldHandle: (record: Record<string, any>) => record.run_id === runId.value,
        onInsert: () => {
            if (runId.value) _refetch(runId.value)
        },
    })

    /**********************
     * Internals
     **********************/
    async function _refetch(id: string) {
        try {
            assetExecutions.value = await apiFetch<AssetExecution[]>(`/runs/${id}/asset-executions`)
        }
        catch {
            // Silently ignore — the initial fetch already set the error state
        }
    }

    /**********************
     * Actions
     **********************/
    async function fetchForRun(id: string) {
        runId.value = id
        loading.value = true
        error.value = null
        try {
            assetExecutions.value = await apiFetch<AssetExecution[]>(`/runs/${id}/asset-executions`)
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            loading.value = false
        }
    }

    function $reset() {
        runId.value = null
        assetExecutions.value = []
        loading.value = false
        error.value = null
    }

    return {
        runId,
        assetExecutions,
        loading,
        error,
        fetchForRun,
        $reset,
    }
})
