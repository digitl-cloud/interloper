import type { Execution } from '~/types/execution'

export const useExecutionsStore = defineStore('executions', () => {
    const { apiFetch } = useApi()
    const orgStore = useOrganisationStore()

    /**********************
     * State
     **********************/
    const runId = ref<string | null>(null)
    const executions = ref<Execution[]>([])
    const loading = ref(false)
    const error = ref<Error | null>(null)

    /**********************
     * Realtime
     **********************/
    // Re-fetch executions when new events arrive for the current run.
    // The executions view aggregates events, so a full re-fetch is the
    // simplest way to stay in sync.
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
            executions.value = await apiFetch<Execution[]>(`/runs/${id}/executions`)
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
            executions.value = await apiFetch<Execution[]>(`/runs/${id}/executions`)
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
        executions.value = []
        loading.value = false
        error.value = null
    }

    return {
        runId,
        executions,
        loading,
        error,
        fetchForRun,
        $reset,
    }
})
