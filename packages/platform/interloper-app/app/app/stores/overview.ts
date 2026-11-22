import type { Coverage, CoverageMonths, CoverageSources, Overview, TypeSources } from '~/types/overview'

/** Both reads aggregate the whole organisation, so a busy one refreshes at most this often. */
const REFRESH_THROTTLE = 15_000
/** Only a run reaching one of these can change a coverage day. */
const TERMINAL_STATUSES = new Set(['success', 'failed', 'canceled'])

/** ISO date (YYYY-MM-DD) of a UTC instant. */
function isoDate(date: Date): string {
    return date.toISOString().slice(0, 10)
}

export const useOverviewStore = defineStore('overview', () => {
    const { apiFetch } = useOrgApi()
    const orgStore = useOrganisationStore()

    const overview = ref<Overview | null>(null)
    /** The calendar's types over the window; a type's sources are read only once it is expanded. */
    const coverage = ref<Coverage | null>(null)
    const coverageMonths = ref<CoverageMonths>(3)
    /** The window the latest coverage read asked for, known before its answer. */
    const coverageWindow = ref<{ since: string, until: string } | null>(null)
    /** The types expanded in the calendar's list, by key. */
    const expandedTypes = ref(new Set<string>())
    /** The sources read for each type over `coverageWindow`, by key. */
    const typeSources = ref<Record<string, TypeSources>>({})
    const loading = ref(false)
    const coverageLoading = ref(false)
    const error = ref<Error | null>(null)
    const coverageError = ref<Error | null>(null)

    let overviewSeq = 0
    let coverageSeq = 0
    let sourcesSeq = 0
    /** The latest sources read of each type; an answer to an earlier one, or to a dropped one, is ignored. */
    const sourcesReads = new Map<string, number>()

    async function fetchOverview() {
        const seq = ++overviewSeq
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

    /**
     * Read the types of the window, and the sources of the expanded types. A refresh keeps the expanded types'
     * sources on screen until their answers replace them; any other read drops every type's sources, which belong
     * to the window read before. Either way a collapsed type's sources are dropped, to be read again on expanding.
     */
    async function fetchCoverage({ refresh = false } = {}) {
        const seq = ++coverageSeq
        coverageLoading.value = true
        coverageError.value = null
        const until = new Date()
        const since = new Date(until)
        // Day 1 first: subtracting months from a month end would overflow into the next month.
        since.setUTCDate(1)
        since.setUTCMonth(since.getUTCMonth() - coverageMonths.value)
        coverageWindow.value = { since: isoDate(since), until: isoDate(until) }
        typeSources.value = refresh
            ? Object.fromEntries(Object.entries(typeSources.value).filter(([key]) => expandedTypes.value.has(key)))
            : {}
        sourcesReads.clear()
        for (const key of expandedTypes.value) fetchTypeSources(key)
        try {
            const params = new URLSearchParams(coverageWindow.value)
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

    /** Merge a state into what is held for one type's sources. */
    function setTypeSources(key: string, state: Partial<TypeSources>) {
        const held = typeSources.value[key] ?? { sources: null, loading: false, error: null }
        typeSources.value = { ...typeSources.value, [key]: { ...held, ...state } }
    }

    /** Read one type's sources over the window, keeping those it holds on screen until the answer replaces them. */
    async function fetchTypeSources(key: string) {
        const window = coverageWindow.value
        if (!window) return
        const seq = ++sourcesSeq
        sourcesReads.set(key, seq)
        setTypeSources(key, { loading: true, error: null })
        try {
            const params = new URLSearchParams(window)
            const result = await apiFetch<CoverageSources>(`/overview/coverage/${encodeURIComponent(key)}?${params}`)
            if (sourcesReads.get(key) === seq) setTypeSources(key, { sources: result.sources, loading: false })
        }
        catch (e) {
            if (sourcesReads.get(key) === seq) setTypeSources(key, { error: e as Error, loading: false })
        }
    }

    /** Expand a type, reading its sources unless they are held or on their way, or collapse it, keeping them. */
    function toggleType(key: string) {
        const next = new Set(expandedTypes.value)
        if (!next.delete(key)) next.add(key)
        expandedTypes.value = next
        const held = typeSources.value[key]
        if (next.has(key) && !held?.sources && !held?.loading) fetchTypeSources(key)
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
                fetchCoverage({ refresh: true })
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

    function $reset() {
        clearTimeout(refreshTimer)
        refreshTimer = undefined
        coverageStale = false
        overviewSeq++
        coverageSeq++
        sourcesReads.clear()
        overview.value = null
        coverage.value = null
        coverageWindow.value = null
        expandedTypes.value = new Set()
        typeSources.value = {}
        loading.value = false
        coverageLoading.value = false
        error.value = null
        coverageError.value = null
    }

    watch(() => orgStore.organisation?.id, $reset)

    return {
        overview,
        coverage,
        coverageMonths,
        coverageWindow,
        expandedTypes,
        typeSources,
        loading,
        coverageLoading,
        error,
        coverageError,
        fetchOverview,
        fetchCoverage,
        fetchTypeSources,
        toggleType,
        setCoverageMonths,
        $reset,
    }
})
