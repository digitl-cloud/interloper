/** One page of a listing: the slice requested and the size of the whole set. */
export interface Page<T> {
    items: T[]
    total: number
}

/** The largest page the API serves. */
export const MAX_PAGE_SIZE = 500

type FetchOptions = Parameters<typeof $fetch>[1]
type Fetcher = <T>(path: string, options?: FetchOptions) => Promise<T>

/** Every item of a listing, walking its pages, for views that need the whole set. */
async function walkPages<T>(fetcher: Fetcher, path: string, params?: URLSearchParams | Record<string, string>): Promise<T[]> {
    const query = new URLSearchParams(params)
    query.set('limit', String(MAX_PAGE_SIZE))
    const items: T[] = []
    for (;;) {
        query.set('offset', String(items.length))
        const page = await fetcher<Page<T>>(`${path}?${query}`)
        items.push(...page.items)
        if (!page.items.length || items.length >= page.total) return items
    }
}

export function useApi() {
    async function apiFetch<T>(path: string, options?: FetchOptions): Promise<T> {
        return $fetch(`/api${path}`, {
            credentials: 'include',
            ...options,
        }) as Promise<T>
    }

    function fetchAll<T>(path: string, params?: URLSearchParams | Record<string, string>): Promise<T[]> {
        return walkPages<T>(apiFetch, path, params)
    }

    return { apiFetch, fetchAll }
}

/**
 * `useApi` under the active organisation: `apiFetch('/runs')` reaches
 * `/api/organisations/<id>/runs`, the organisation read at call time.
 *
 * A call belongs to the organisation it was made in. Once the route leaves
 * that organisation the request is aborted and its promise never settles: no
 * late response fills the next organisation's stores, and no caller reports
 * the abandoned call as a failure. Neither does its `finally` run, so state a
 * caller sets around a call (a `loading` or `creating` flag) must be reset on
 * the switch by whoever outlives it: a store in its `$reset`, a component kept
 * across organisations (the layout's agent panel) in its own switch watcher.
 * Pages under `/<key>/` remount on a switch and need nothing.
 */
export function useOrgApi() {
    const { apiFetch: baseFetch } = useApi()
    const organisationStore = useOrganisationStore()

    async function apiFetch<T>(path: string, options?: FetchOptions): Promise<T> {
        const { id } = organisationStore.requireOrganisation()
        const signal = organisationStore.signal()
        const abandoned = () => new Promise<never>(() => {})
        try {
            const result = await baseFetch<T>(`/organisations/${id}${path}`, { ...options, signal })
            return signal.aborted ? abandoned() : result
        }
        catch (e) {
            if (signal.aborted) return abandoned()
            throw e
        }
    }

    function fetchAll<T>(path: string, params?: URLSearchParams | Record<string, string>): Promise<T[]> {
        return walkPages<T>(apiFetch, path, params)
    }

    return { apiFetch, fetchAll }
}
