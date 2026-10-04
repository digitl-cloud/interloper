import type { Page } from '~/composables/api'
import type { Run } from '~/types/run'

/** Target-side narrowing of the runs list; empty/null means no filter. */
export interface RunFilters {
    /** Case-insensitive match on the target's name or key. */
    q: string
    /** Target component kind. */
    kind: string | null
    /** Target type (catalog key). */
    key: string | null
    /** Run status, read off the stack's latest attempt. */
    status: string | null
}

const NO_FILTERS: RunFilters = { q: '', kind: null, key: null, status: null }

export const useRunsStore = defineStore('runs', () => {
    const { apiFetch, fetchAll } = useApi()
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
    const stacks = ref<Record<string, Run[]>>({})

    /**********************
     * Getters
     **********************/
    const totalPages = computed(() => Math.ceil(total.value / pageSize.value))
    const filtered = computed(() => Object.entries(filters.value).some(([name, value]) => value !== NO_FILTERS[name as keyof RunFilters]))

    /**********************
     * Internals
     **********************/
    /**
     * A listing holds one row per stack, its latest attempt. So a run arriving
     * over realtime either updates its own row, supersedes the earlier attempt
     * of the stack it belongs to, or is new work.
     */
    function _cacheAttempt(run: Run) {
        const root = run.root_run_id
        const cached = root ? stacks.value[root] : undefined
        if (!root || !cached) return
        const merged = { ...cached.find(r => r.id === run.id), ...run }
        const attempts = [merged, ...cached.filter(r => r.id !== run.id)].sort((a, b) => b.attempt - a.attempt)
        stacks.value = { ...stacks.value, [root]: attempts }
    }

    function _upsert(run: Run) {
        _cacheAttempt(run)
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
        const { q, kind, key, status } = filters.value
        if (kind && run.component_kind !== kind) return false
        if (key && run.component_key !== key) return false
        if (status && run.status !== status) return false
        if (!q) return true
        const needle = q.toLowerCase()
        return [run.component_name, run.component_key].some(text => text?.toLowerCase().includes(needle))
    }

    function _isShown(id: string): boolean {
        return !!findById(id) || Object.values(stacks.value).some(attempts => attempts.some(r => r.id === id))
    }

    /** Swap in a refetched run's fields wherever it is shown, without the list semantics of `_upsert`. */
    function _refresh(run: Run) {
        const idx = runs.value.findIndex(r => r.id === run.id)
        if (idx >= 0) runs.value[idx] = { ...runs.value[idx], ...run }
        _cacheAttempt(run)
    }

    // Execution counts move with events, which never touch the runs table: the
    // runs a burst of events concerns are refetched together, at most once a second.
    const staleCounts = new Set<string>()
    let countsTimer: ReturnType<typeof setTimeout> | undefined

    async function _refreshCounts() {
        countsTimer = undefined
        const ids = [...staleCounts]
        staleCounts.clear()
        const fresh = await Promise.all(ids.map(id => fetchOne(id).catch(() => null)))
        for (const run of fresh) if (run) _refresh(run)
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

    useRealtimeSubscription({
        table: 'events',
        scope: () => runs.value.length ? orgStore.organisation?.id : null,
        shouldHandle: (record: Record<string, any>) => !!record.run_id && _isShown(record.run_id),
        onInsert: (record: Record<string, any>) => {
            staleCounts.add(record.run_id)
            countsTimer ??= setTimeout(_refreshCounts, 1000)
        },
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
            const { q, kind, key, status } = filters.value
            if (q) params.set('q', q)
            if (kind) params.set('component_kind', kind)
            if (key) params.set('component_key', key)
            if (status) params.set('status', status)
            const page = await apiFetch<Page<Run>>(`/runs?${params}`)
            runs.value = page.items
            total.value = page.total
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
        return fetchAll<Run>('/runs', { root_run_id: rootRunId })
    }

    /** Load a stack's attempts into `stacks` unless they are already there. */
    async function loadStack(rootRunId: string) {
        if (stacks.value[rootRunId]) return
        stacks.value = { ...stacks.value, [rootRunId]: await fetchStack(rootRunId) }
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
        const run = await apiFetch<Run>(`/runs/${id}/retry`, {
            method: 'POST',
            body: { scope },
        })
        return run.id
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
        stacks.value = {}
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
        stacks,
        loadStack,
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
