import type { EventType } from '~/types/event'

export interface RunEvent {
    id: string
    org_id: string
    run_id: string | null
    event_type: EventType
    asset_id: string | null
    asset_key: string | null
    partition_or_window: string | null
    error: string | null
    traceback: string | null
    message: string | null
    timestamp: string
}

export const useEventsStore = defineStore('events', () => {
    const { apiFetch } = useApi()
    const orgStore = useOrganisationStore()

    /**********************
     * State
     **********************/
    const runId = ref<string | null>(null)
    const events = ref<RunEvent[]>([])
    const loading = ref(false)
    const error = ref<Error | null>(null)

    /**********************
     * Internals
     **********************/
    function _upsert(event: RunEvent) {
        const idx = events.value.findIndex(e => e.id === event.id)
        if (idx >= 0) {
            events.value[idx] = event
        }
        else {
            events.value.push(event)
            events.value.sort((a, b) => a.timestamp.localeCompare(b.timestamp))
        }
    }

    /**********************
     * Realtime
     **********************/
    useRealtimeSubscription({
        table: 'events',
        scope: () => runId.value ? orgStore.organisation?.id : null,
        shouldHandle: (record: Record<string, any>) => record.run_id === runId.value,
        onInsert: (record: Record<string, any>) => _upsert(record as RunEvent),
    })

    /**********************
     * Actions
     **********************/
    async function fetchForRun(id: string) {
        runId.value = id
        loading.value = true
        error.value = null
        try {
            events.value = await apiFetch<RunEvent[]>(`/runs/${id}/events`)
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            loading.value = false
        }
    }

    /**********************
     * Lookups
     **********************/
    function findById(id: string): RunEvent | undefined {
        return events.value.find(e => e.id === id)
    }

    function byAssetKey(key: string): RunEvent[] {
        return events.value.filter(e => e.asset_key === key)
    }

    function $reset() {
        runId.value = null
        events.value = []
        loading.value = false
        error.value = null
    }

    return {
        runId,
        events,
        loading,
        error,
        fetchForRun,
        findById,
        byAssetKey,
        _upsert,
        $reset,
    }
})
