import type { Coverage, CoverageMonths, Overview } from '~/types/overview'

/** Both reads aggregate the whole organisation, so a busy one refreshes at most this often. */
const REFRESH_THROTTLE = 15_000
/** Only a run reaching one of these can change a coverage day. */
const TERMINAL_STATUSES = new Set(['success', 'failed', 'canceled'])

/** ISO date (YYYY-MM-DD) of a UTC instant. */
function isoDate(date: Date): string {
    return date.toISOString().slice(0, 10)
}

export const useOverviewStore = defineStore('overview', () => {
    const { apiFetch } = useApi()
    const orgStore = useOrganisationStore()

    const overview = ref<Overview | null>(null)
    const coverage = ref<Coverage | null>(null)
    const coverageMonths = ref<CoverageMonths>(6)
    const loading = ref(false)
    const coverageLoading = ref(false)
    const error = ref<Error | null>(null)
    const coverageError = ref<Error | null>(null)
    const active = ref(false)

    let overviewSeq = 0
    let coverageSeq = 0

    async function fetchOverview() {
        const seq = ++overviewSeq
        active.value = true
        loading.value = true
        error.value = null
        try {
            const result = await apiFetch<Overview>('/overview')
            if (seq === overviewSeq) overview.value = result
        }
        catch (e) {
            if (seq === overviewSeq) error.value = e as Error
        }
        finally {
            if (seq === overviewSeq) loading.value = false
        }
    }

    async function fetchCoverage() {
        const seq = ++coverageSeq
        coverageLoading.value = true
        coverageError.value = null
        try {
            const until = new Date()
            const since = new Date(until)
            // Day 1 first: subtracting months from a month end would overflow into the next month.
            since.setUTCDate(1)
            since.setUTCMonth(since.getUTCMonth() - coverageMonths.value)
            const params = new URLSearchParams({ since: isoDate(since), until: isoDate(until) })
            const result = await apiFetch<Coverage>(`/overview/coverage?${params}`)
            if (seq === coverageSeq) coverage.value = result
        }
        catch (e) {
            if (seq === coverageSeq) coverageError.value = e as Error
        }
        finally {
            if (seq === coverageSeq) coverageLoading.value = false
        }
    }

    async function setCoverageMonths(months: CoverageMonths) {
        coverageMonths.value = months
        await fetchCoverage()
    }

    let refreshTimer: ReturnType<typeof setTimeout> | undefined
    let coverageStale = false
    /** The first event starts the timer and later ones ride it; the calendar refetches only when marked stale. */
    function scheduleRefresh(staleCoverage = false) {
        coverageStale ||= staleCoverage
        if (refreshTimer) return
        refreshTimer = setTimeout(() => {
            refreshTimer = undefined
            fetchOverview()
            if (coverageStale) {
                coverageStale = false
                fetchCoverage()
            }
        }, REFRESH_THROTTLE)
    }
    const scope = () => overview.value ? orgStore.organisation?.id : null
    useRealtimeSubscription({
        table: 'runs',
        scope,
        onInsert: () => scheduleRefresh(),
        onUpdate: record => scheduleRefresh(TERMINAL_STATUSES.has(record.status)),
        onDelete: () => scheduleRefresh(),
    })
    useRealtimeSubscription({
        table: 'backfills',
        scope,
        onInsert: () => scheduleRefresh(),
        onUpdate: () => scheduleRefresh(),
        onDelete: () => scheduleRefresh(),
    })

    function clear() {
        clearTimeout(refreshTimer)
        refreshTimer = undefined
        coverageStale = false
        overviewSeq++
        coverageSeq++
        overview.value = null
        coverage.value = null
        loading.value = false
        coverageLoading.value = false
        error.value = null
        coverageError.value = null
    }

    function $reset() {
        clear()
        active.value = false
    }

    useOrgScopedRefetch(() => {
        if (active.value) {
            fetchOverview()
            fetchCoverage()
        }
    }, clear)

    return {
        overview,
        coverage,
        coverageMonths,
        loading,
        coverageLoading,
        error,
        coverageError,
        fetchOverview,
        fetchCoverage,
        setCoverageMonths,
        $reset,
    }
})
