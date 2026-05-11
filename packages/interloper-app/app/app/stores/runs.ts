import type { Run } from '~/types/run'

export const useRunsStore = defineStore('runs', () => {
    const { apiFetch } = useApi()
    const orgStore = useOrganisationStore()

    /**********************
     * State
     **********************/
    const runs = ref<Run[]>([])
    const total = ref(0)
    const pageSize = ref(50)
    const pageIndex = ref(0)
    const loading = ref(false)
    const error = ref<Error | null>(null)

    /**********************
     * Getters
     **********************/
    const totalPages = computed(() => Math.ceil(total.value / pageSize.value))

    /**********************
     * Internals
     **********************/
    function _upsert(run: Partial<Run> & { id: string }) {
        const idx = runs.value.findIndex(r => r.id === run.id)
        const existing = runs.value[idx]
        if (idx >= 0 && existing) {
            // Strip undefined values so realtime partials don't overwrite richer API data
            const clean = Object.fromEntries(Object.entries(run).filter(([, v]) => v !== undefined)) as Partial<Run>
            runs.value[idx] = { ...existing, ...clean }
        }
        else {
            runs.value.unshift(run as Run)
            total.value++
        }
    }

    function _remove(id: string) {
        const existed = runs.value.some(r => r.id === id)
        runs.value = runs.value.filter(r => r.id !== id)
        if (existed) total.value = Math.max(0, total.value - 1)
    }

    /**********************
     * Realtime
     **********************/
    useRealtimeSubscription({
        table: 'runs',
        scope: () => orgStore.organisation?.id,
        onInsert: (record: Record<string, any>) => _upsert(record as Run),
        onUpdate: (record: Record<string, any>) => _upsert(record as Run),
        onDelete: (record: Record<string, any>) => _remove(record.id),
    })

    /**********************
     * Actions
     **********************/
    async function fetch(filters?: { jobId?: string; backfillId?: string; status?: string }) {
        loading.value = true
        error.value = null
        try {
            const params = new URLSearchParams()
            params.set('limit', String(pageSize.value))
            params.set('offset', String(pageIndex.value * pageSize.value))
            if (filters?.jobId) params.set('job_id', filters.jobId)
            if (filters?.backfillId) params.set('backfill_id', filters.backfillId)
            if (filters?.status) params.set('status', filters.status)
            runs.value = await apiFetch<Run[]>(`/runs?${params}`)
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            loading.value = false
        }
    }

    async function fetchOne(id: string): Promise<Run> {
        return apiFetch<Run>(`/runs/${id}`)
    }

    async function goToPage(page: number) {
        pageIndex.value = page
        await fetch()
    }

    /**********************
     * Lookups
     **********************/
    function findById(id: string): Run | undefined {
        return runs.value.find(r => r.id === id)
    }

    function $reset() {
        runs.value = []
        total.value = 0
        pageIndex.value = 0
        loading.value = false
        error.value = null
    }

    return {
        runs,
        total,
        pageSize,
        pageIndex,
        totalPages,
        loading,
        error,
        fetch,
        fetchOne,
        goToPage,
        findById,
        _upsert,
        _remove,
        $reset,
    }
})
