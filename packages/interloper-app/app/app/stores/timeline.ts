import type { Run } from '~/types/run'

/**
 * Runs of one wall-clock window, for the Timeline page.
 *
 * Separate from the runs store on purpose: that one pages the runs *table*
 * (50 newest, offset-paged), while a timeline asks a different question — every
 * run that executed inside a period, however many pages of history back that
 * reaches.
 */

/** Selectable window lengths, in ms. */
export const TIMELINE_SPANS = [
    { value: 3_600_000, label: '1h' },
    { value: 6 * 3_600_000, label: '6h' },
    { value: 12 * 3_600_000, label: '12h' },
    { value: 24 * 3_600_000, label: '24h' },
    { value: 7 * 24 * 3_600_000, label: '7d' },
    { value: 30 * 24 * 3_600_000, label: '30d' },
]

/** Cap on one window's runs; anything beyond is reported, never silently dropped. */
const MAX_RUNS = 1000

export const useTimelineStore = defineStore('timeline', () => {
    const { apiFetchRaw } = useApi()
    const orgStore = useOrganisationStore()

    /**********************
     * State
     **********************/
    const runs = ref<Run[]>([])
    const span = ref(24 * 3_600_000)
    /** Window bounds in epoch ms, anchored at the last fetch. */
    const rangeStart = ref(Date.now() - span.value)
    const rangeEnd = ref(Date.now())
    const total = ref(0)
    const loading = ref(false)
    const error = ref<Error | null>(null)

    /**********************
     * Getters
     **********************/
    /** The window holds more runs than were loaded — the view is partial. */
    const truncated = computed(() => total.value > runs.value.length)

    /**********************
     * Internals
     **********************/
    /**
     * Whether a run belongs to the current window. Only the left edge is
     * enforced: a run that started after the window was anchored is the live
     * edge of the timeline, which grows to meet it.
     */
    function _inWindow(run: Run): boolean {
        if (!run.started_at) return false
        if (!run.completed_at) return true
        return new Date(run.completed_at).getTime() >= rangeStart.value
    }

    function _upsert(run: Partial<Run> & { id: string }) {
        const idx = runs.value.findIndex(r => r.id === run.id)
        const existing = runs.value[idx]
        if (idx >= 0 && existing) {
            // Strip undefined values so realtime partials don't overwrite richer API data
            const clean = Object.fromEntries(Object.entries(run).filter(([, v]) => v !== undefined)) as Partial<Run>
            const merged = { ...existing, ...clean }
            if (_inWindow(merged)) runs.value[idx] = merged
            else runs.value.splice(idx, 1)
        }
        else if (_inWindow(run as Run)) {
            runs.value.push(run as Run)
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
    /** Re-anchor the window to now and load the runs that executed inside it. */
    async function fetch() {
        loading.value = true
        error.value = null
        rangeEnd.value = Date.now()
        rangeStart.value = rangeEnd.value - span.value
        try {
            const params = new URLSearchParams({
                after: new Date(rangeStart.value).toISOString(),
                before: new Date(rangeEnd.value).toISOString(),
                limit: String(MAX_RUNS),
            })
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

    async function setSpan(ms: number) {
        span.value = ms
        await fetch()
    }

    function $reset() {
        runs.value = []
        total.value = 0
        loading.value = false
        error.value = null
    }

    useOrgScopedRefetch(() => fetch(), $reset)

    return {
        runs,
        span,
        rangeStart,
        rangeEnd,
        total,
        truncated,
        loading,
        error,
        fetch,
        setSpan,
        $reset,
    }
})
