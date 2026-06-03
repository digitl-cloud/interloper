export function useApi() {
    async function apiFetch<T>(path: string, options?: Parameters<typeof $fetch>[1]): Promise<T> {
        return $fetch(`/api${path}`, {
            credentials: 'include',
            ...options,
        }) as Promise<T>
    }

    /** Like `apiFetch` but returns the full response so callers can read headers (e.g. pagination totals). */
    async function apiFetchRaw<T>(path: string, options?: Parameters<typeof $fetch.raw>[1]) {
        return $fetch.raw<T>(`/api${path}`, {
            credentials: 'include',
            ...options,
        })
    }

    return { apiFetch, apiFetchRaw }
}
