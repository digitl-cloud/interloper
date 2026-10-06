import type { Execution } from '~/types/execution'

export const useExecutionsStore = defineStore('executions', () => {
    const { fetchAll } = useApi()
    const orgStore = useOrganisationStore()

    /**********************
     * State
     **********************/
    const runId = ref<string | null>(null)
    const executions = ref<Execution[]>([])
    const loading = ref(false)
    const error = ref<Error | null>(null)
    /** Latest execution per asset across the organisation, once `fetchLatest` has run. */
    const latest = ref<Execution[]>([])
    const latestLoaded = ref(false)
    const latestByAssetId = computed(() => {
        const map = new Map<string, Execution>()
        for (const execution of latest.value) {
            if (execution.component_id) map.set(execution.component_id, execution)
        }
        return map
    })

    /**********************
     * Realtime
     **********************/
    // The executions table changes as events fold into it, so a pushed row is the row a refetch would read.
    useRealtimeSubscription({
        table: 'executions',
        scope: () => runId.value || latestLoaded.value ? orgStore.organisation?.id : null,
        onInsert: _apply,
        onUpdate: _apply,
    })

    /**********************
     * Internals
     **********************/
    function _apply(record: Record<string, any>) {
        const execution = record as Execution
        if (execution.run_id === runId.value) {
            executions.value = _upsert(executions.value, execution, row => row.component_id === execution.component_id)
        }
        if (latestLoaded.value && execution.component_id) {
            const current = latestByAssetId.value.get(execution.component_id)
            if (!current || current.run_id === execution.run_id || _newer(execution, current)) {
                latest.value = _upsert(latest.value, execution, row => row.component_id === execution.component_id)
            }
        }
    }

    function _upsert(rows: Execution[], row: Execution, matches: (row: Execution) => boolean) {
        const index = rows.findIndex(matches)
        return index === -1 ? [...rows, row] : rows.map((existing, i) => (i === index ? row : existing))
    }

    function _newer(candidate: Execution, current: Execution) {
        return (Date.parse(candidate.created_at ?? '') || 0) >= (Date.parse(current.created_at ?? '') || 0)
    }

    /**********************
     * Actions
     **********************/
    async function fetchForRun(id: string) {
        runId.value = id
        loading.value = true
        error.value = null
        try {
            executions.value = await fetchAll<Execution>(`/runs/${id}/executions`)
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            loading.value = false
        }
    }

    async function fetchLatest() {
        try {
            latest.value = await fetchAll<Execution>('/executions', { latest: 'true' })
            latestLoaded.value = true
        }
        catch (e) {
            error.value = e as Error
        }
    }

    function $reset() {
        runId.value = null
        executions.value = []
        loading.value = false
        error.value = null
        latest.value = []
        latestLoaded.value = false
    }

    return {
        runId,
        executions,
        loading,
        error,
        latest,
        latestByAssetId,
        fetchForRun,
        fetchLatest,
        $reset,
    }
})
