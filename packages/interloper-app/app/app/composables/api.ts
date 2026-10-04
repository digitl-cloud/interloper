/** One page of a listing: the slice requested and the size of the whole set. */
export interface Page<T> {
    items: T[]
    total: number
}

/** The largest page the API serves. */
export const MAX_PAGE_SIZE = 500

export function useApi() {
    async function apiFetch<T>(path: string, options?: Parameters<typeof $fetch>[1]): Promise<T> {
        return $fetch(`/api${path}`, {
            credentials: 'include',
            ...options,
        }) as Promise<T>
    }

    /** Every item of a listing, walking its pages, for views that need the whole set. */
    async function fetchAll<T>(path: string, params?: URLSearchParams | Record<string, string>): Promise<T[]> {
        const query = new URLSearchParams(params)
        query.set('limit', String(MAX_PAGE_SIZE))
        const items: T[] = []
        for (;;) {
            query.set('offset', String(items.length))
            const page = await apiFetch<Page<T>>(`${path}?${query}`)
            items.push(...page.items)
            if (!page.items.length || items.length >= page.total) return items
        }
    }

    return { apiFetch, fetchAll }
}
