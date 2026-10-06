import { MAX_PAGE_SIZE, type Page } from '~/composables/api'
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
    const { apiFetch } = useApi()
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
    /** Whether a fetch has completed since the last reset, so a view can tell its first load from a refresh. */
    const loaded = ref(false)
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

    function _upsert(run: Run) {
        const idx = runs.value.findIndex(r => r.id === run.id)
        if (idx >= 0) {
            const merged = { ...runs.value[idx], ...run }
            if (_inWindow(merged)) runs.value[idx] = merged
            else runs.value.splice(idx, 1)
        }
        else if (_inWindow(run)) {
            runs.value.push(run)
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
    /**
     * Re-anchor the window to now and load the runs that executed inside it.
     * `futureRatio` is the share of the window that lies after now; the
     * default 0 ends the window at the fetch.
     */
    async function fetch(options: { futureRatio?: number } = {}) {
        loading.value = true
        error.value = null
        rangeEnd.value = Date.now() + span.value * (options.futureRatio ?? 0)
        rangeStart.value = rangeEnd.value - span.value
        try {
            const params = new URLSearchParams({
                after: new Date(rangeStart.value).toISOString(),
                before: new Date(Math.min(rangeEnd.value, Date.now())).toISOString(),
                limit: String(MAX_PAGE_SIZE),
            })
            const loaded: Run[] = []
            let page: Page<Run>
            do {
                params.set('offset', String(loaded.length))
                page = await apiFetch<Page<Run>>(`/runs?${params}`)
                loaded.push(...page.items)
            } while (page.items.length && loaded.length < Math.min(page.total, MAX_RUNS))
            runs.value = loaded
            total.value = page.total
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            loading.value = false
            loaded.value = true
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
        loaded.value = false
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
        loaded,
        error,
        fetch,
        setSpan,
        $reset,
    }
})
