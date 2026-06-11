import type { Backfill } from '~/types/backfill'

export const useBackfillsStore = defineStore('backfills', () => {
    const { apiFetch } = useApi()
    const orgStore = useOrganisationStore()

    /**********************
     * State
     **********************/
    const backfills = ref<Backfill[]>([])
    const loading = ref(false)
    const error = ref<Error | null>(null)

    /**********************
     * Internals
     **********************/
    function _upsert(backfill: Backfill) {
        const idx = backfills.value.findIndex(b => b.id === backfill.id)
        if (idx >= 0) backfills.value[idx] = { ...backfills.value[idx], ...backfill }
        else backfills.value.push(backfill)
    }

    function _remove(id: string) {
        backfills.value = backfills.value.filter(b => b.id !== id)
    }

    /**********************
     * Realtime
     **********************/
    useRealtimeSubscription({
        table: 'backfills',
        scope: () => orgStore.organisation?.id,
        onInsert: (record: Record<string, any>) => _upsert(record as Backfill),
        onUpdate: (record: Record<string, any>) => _upsert(record as Backfill),
        onDelete: (record: Record<string, any>) => _remove(record.id),
    })

    /**********************
     * Actions
     **********************/
    async function fetch(activeOnly = false) {
        loading.value = true
        error.value = null
        try {
            const query = activeOnly ? '?active_only=true' : ''
            backfills.value = await apiFetch<Backfill[]>(`/backfills${query}`)
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            loading.value = false
        }
    }

    async function fetchOne(id: string): Promise<Backfill> {
        return apiFetch<Backfill>(`/backfills/${id}`)
    }

    /**********************
     * Lookups
     **********************/
    function findById(id: string): Backfill | undefined {
        return backfills.value.find(b => b.id === id)
    }

    function $reset() {
        backfills.value = []
        loading.value = false
        error.value = null
    }

    useOrgScopedRefetch(() => fetch(), $reset)

    return {
        backfills,
        loading,
        error,
        fetch,
        fetchOne,
        findById,
        _upsert,
        _remove,
        $reset,
    }
})
