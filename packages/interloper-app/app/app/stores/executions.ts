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

    // A run streams many events; one refetch per burst is enough for the dots.
    let latestTimer: ReturnType<typeof setTimeout> | undefined
    useRealtimeSubscription({
        table: 'events',
        scope: () => latestLoaded.value ? orgStore.organisation?.id : null,
        onInsert: () => {
            clearTimeout(latestTimer)
            latestTimer = setTimeout(() => { fetchLatest() }, 1000)
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

    async function fetchLatest() {
        try {
            latest.value = await apiFetch<Execution[]>('/runs/executions/latest')
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
