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
    level: string | null
    timestamp: string
}

/** How many events to request per page while infinite-scrolling a run. */
const EVENTS_PAGE_SIZE = 100

export const useEventsStore = defineStore('events', () => {
    const { apiFetchRaw } = useApi()
    const orgStore = useOrganisationStore()

    /**********************
     * State
     **********************/
    const runId = ref<string | null>(null)
    // Server-side asset filter. Events are paged from the server, so filtering
    // must happen there too — filtering only the loaded pages client-side would
    // hide every matching event that hasn't been scrolled into view yet.
    const assetId = ref<string | null>(null)
    const events = ref<RunEvent[]>([])
    const total = ref(0)
    const loading = ref(false) // initial page load
    const loadingMore = ref(false) // subsequent pages (infinite scroll)
    const error = ref<Error | null>(null)

    // Bumped on every reset (run change, filter change, $reset) so a page
    // response that was in flight across the reset is dropped instead of
    // merging stale rows and clobbering total/nextOffset.
    let fetchEpoch = 0

    // Offset of the next page to request. Tracks rows pulled from the server
    // only — realtime tail-inserts append to `events` but never advance this.
    // Must be a ref: `hasMore` reads it inside a computed, so a plain variable
    // would leave `hasMore` stale once `total` stops changing (the last page
    // sets `total` to its existing value, firing no recompute) — which makes
    // infinite scroll re-fire loadMore forever at the end of the list.
    const nextOffset = ref(0)

    /**********************
     * Getters
     **********************/
    /** Whether more events remain on the server beyond what's been fetched. */
    const hasMore = computed(() => nextOffset.value < total.value)

    /**********************
     * Internals
     **********************/
    function _sort() {
        events.value.sort((a, b) => a.timestamp.localeCompare(b.timestamp) || a.id.localeCompare(b.id))
    }

    function _upsert(event: RunEvent) {
        const idx = events.value.findIndex(e => e.id === event.id)
        if (idx >= 0) {
            events.value[idx] = event
        }
        else {
            events.value.push(event)
            _sort()
            total.value++
        }
    }

    /** Append a freshly fetched page, skipping rows already present (e.g. via realtime). */
    function _mergePage(rows: RunEvent[]) {
        const seen = new Set(events.value.map(e => e.id))
        const fresh = rows.filter(r => !seen.has(r.id))
        if (!fresh.length) return
        events.value.push(...fresh)
        _sort()
    }

    /** Fetch the page at `nextOffset` and merge it in. Caller owns loading flags. */
    async function _loadPage() {
        const id = runId.value
        if (!id) return
        const epoch = fetchEpoch
        const params = new URLSearchParams({
            limit: String(EVENTS_PAGE_SIZE),
            offset: String(nextOffset.value),
        })
        if (assetId.value) params.set('asset_id', assetId.value)
        const res = await apiFetchRaw<RunEvent[]>(`/runs/${id}/events?${params}`)
        if (epoch !== fetchEpoch) return // state was reset while in flight
        const page = res._data ?? []
        total.value = Number(res.headers.get('X-Total-Count') ?? nextOffset.value + page.length)
        // A short/empty page means the server has nothing more; pin the offset
        // to the total so `hasMore` settles false and we never loop forever.
        nextOffset.value = page.length < EVENTS_PAGE_SIZE ? total.value : nextOffset.value + page.length
        _mergePage(page)
    }

    /**********************
     * Realtime
     **********************/
    useRealtimeSubscription({
        table: 'events',
        scope: () => runId.value ? orgStore.organisation?.id : null,
        shouldHandle: (record: Record<string, any>) =>
            record.run_id === runId.value && (!assetId.value || record.asset_id === assetId.value),
        onInsert: (record: Record<string, any>) => _upsert(record as RunEvent),
    })

    /**********************
     * Actions
     **********************/
    /**
     * Load the first page of events for a run, optionally filtered to one asset.
     *
     * Events are ordered oldest-first, so the terminal/outcome events
     * (`asset_completed`, `asset_failed`, `run_failed`, …) live at the end.
     * The table reaches them by infinite-scrolling — see `loadMore` — rather
     * than loading the whole history up front.
     */
    async function fetchForRun(id: string, asset: string | null = null) {
        runId.value = id
        assetId.value = asset
        fetchEpoch++
        events.value = []
        total.value = 0
        nextOffset.value = 0
        error.value = null
        loading.value = true
        try {
            await _loadPage()
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            loading.value = false
        }
    }

    /** Re-page from the start with an asset filter (`null` clears it). */
    async function filterByAsset(asset: string | null) {
        if (!runId.value || asset === assetId.value) return
        await fetchForRun(runId.value, asset)
    }

    /** Load the next page of events. Safe to call repeatedly (infinite scroll). */
    async function loadMore() {
        if (loading.value || loadingMore.value || !hasMore.value) return
        loadingMore.value = true
        try {
            await _loadPage()
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            loadingMore.value = false
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
        assetId.value = null
        fetchEpoch++
        events.value = []
        total.value = 0
        nextOffset.value = 0
        loading.value = false
        loadingMore.value = false
        error.value = null
    }

    return {
        runId,
        assetId,
        events,
        total,
        loading,
        loadingMore,
        hasMore,
        error,
        fetchForRun,
        filterByAsset,
        loadMore,
        findById,
        byAssetKey,
        _upsert,
        $reset,
    }
})
