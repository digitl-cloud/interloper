export function useApi() {
    async function apiFetch<T>(path: string, options?: Parameters<typeof $fetch>[1]): Promise<T> {
        return $fetch(`/api${path}`, {
            credentials: 'include',
            ...options,
        }) as Promise<T>
    }

    return { apiFetch }
}
