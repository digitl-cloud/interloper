import type { Run } from '~/types/run'

/** Target-side narrowing of the runs list; empty/null means no filter. */
export interface RunFilters {
    /** Case-insensitive match on the target's name or key. */
    q: string
    /** Target component kind. */
    kind: string | null
    /** Target type (catalog key). */
    key: string | null
}

const NO_FILTERS: RunFilters = { q: '', kind: null, key: null }

export const useRunsStore = defineStore('runs', () => {
    const { apiFetch, apiFetchRaw } = useApi()
    const orgStore = useOrganisationStore()

    /**********************
     * State
     **********************/
    const runs = ref<Run[]>([])
    const total = ref(0)
    const pageSize = ref(50)
    const pageIndex = ref(0)
    const filters = ref<RunFilters>({ ...NO_FILTERS })
    const loading = ref(false)
    const error = ref<Error | null>(null)

    /**********************
     * Getters
     **********************/
    const totalPages = computed(() => Math.ceil(total.value / pageSize.value))
    const filtered = computed(() => filters.value.q !== '' || filters.value.kind !== null || filters.value.key !== null)

    /**********************
     * Internals
     **********************/
    /**
     * A listing holds one row per stack, its latest attempt. So a run arriving
     * over realtime either updates its own row, supersedes the earlier attempt
     * of the stack it belongs to, or is new work.
     */
    function _upsert(run: Run) {
        const idx = runs.value.findIndex(r => r.id === run.id)
        if (idx >= 0) {
            runs.value[idx] = { ...runs.value[idx], ...run }
            return
        }
        const predecessor = run.root_run_id
            ? runs.value.findIndex(r => (r.root_run_id ?? r.id) === run.root_run_id)
            : -1
        if (predecessor >= 0) {
            runs.value[predecessor] = run
            return
        }
        runs.value.unshift(run)
        total.value++
    }

    function _remove(id: string) {
        const existed = runs.value.some(r => r.id === id)
        runs.value = runs.value.filter(r => r.id !== id)
        if (existed) total.value = Math.max(0, total.value - 1)
    }

    /** The client-side reading of the active filters, for records arriving over realtime. */
    function _matches(run: Run): boolean {
        const { q, kind, key } = filters.value
        if (kind && run.component_kind !== kind) return false
        if (key && run.component_key !== key) return false
        if (!q) return true
        const needle = q.toLowerCase()
        return [run.component_name, run.component_key].some(text => text?.toLowerCase().includes(needle))
    }

    /** A realtime record enters the list only if it matches the filters; one already listed is always refreshed. */
    function _onRealtime(record: Run) {
        if (findById(record.id) || _matches(record)) _upsert(record)
    }

    /**********************
     * Realtime
     **********************/
    useRealtimeSubscription({
        table: 'runs',
        scope: () => orgStore.organisation?.id,
        onInsert: (record: Record<string, any>) => _onRealtime(record as Run),
        onUpdate: (record: Record<string, any>) => _onRealtime(record as Run),
        onDelete: (record: Record<string, any>) => _remove(record.id),
    })

    /**********************
     * Actions
     **********************/
    async function fetch() {
        loading.value = true
        error.value = null
        try {
            const params = new URLSearchParams()
            params.set('limit', String(pageSize.value))
            params.set('offset', String(pageIndex.value * pageSize.value))
            const { q, kind, key } = filters.value
            if (q) params.set('q', q)
            if (kind) params.set('component_kind', kind)
            if (key) params.set('component_key', key)
            const res = await apiFetchRaw<Run[]>(`/runs?${params}`)
            runs.value = res._data ?? []
            total.value = Number(res.headers.get('X-Total-Count') ?? runs.value.length)
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

    /** Every attempt of one stack, newest first. A listing only ever carries the latest. */
    async function fetchStack(rootRunId: string): Promise<Run[]> {
        const params = new URLSearchParams({ root_run_id: rootRunId })
        return apiFetch<Run[]>(`/runs?${params}`)
    }

    /** Queue a manual run for a runnable component (job, source, or asset). Returns the created run's id. */
    async function createRun(componentId: string, partitionKey?: string): Promise<string> {
        const run = await apiFetch<Run>('/runs', {
            method: 'POST',
            body: { component_id: componentId, partition_key: partitionKey },
        })
        return run.id
    }

    async function retryRun(id: string, scope: 'all' | 'failed'): Promise<string> {
        const res = await apiFetch<{ run_id: string }>(`/runs/${id}/retry`, {
            method: 'POST',
            body: { scope },
        })
        return res.run_id
    }

    async function goToPage(page: number) {
        pageIndex.value = page
        await fetch()
    }

    /** Narrow the list; the page restarts at the first, since the old offset means nothing under new filters. */
    async function setFilters(next: Partial<RunFilters>) {
        filters.value = { ...filters.value, ...next }
        pageIndex.value = 0
        await fetch()
    }

    /**
     * Drop the filters without refetching. The runs table calls this when it
     * leaves, so the pages sharing this list (collection, sources) fetch it
     * unfiltered on their own mount.
     */
    function clearFilters() {
        filters.value = { ...NO_FILTERS }
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
        clearFilters()
        loading.value = false
        error.value = null
    }

    useOrgScopedRefetch(() => fetch(), $reset)

    return {
        runs,
        total,
        pageSize,
        pageIndex,
        filters,
        filtered,
        totalPages,
        loading,
        error,
        fetch,
        fetchOne,
        createRun,
        fetchStack,
        retryRun,
        goToPage,
        setFilters,
        clearFilters,
        findById,
        _upsert,
        _remove,
        $reset,
    }
})
