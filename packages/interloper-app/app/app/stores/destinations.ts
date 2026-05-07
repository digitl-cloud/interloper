import type { Destination, DestinationInput } from '~/types/destination'

export const useDestinationsStore = defineStore('destinations', () => {
    const { apiFetch } = useApi()

    /**********************
     * State
     **********************/
    const destinations = ref<Destination[]>([])
    const loading = ref(false)
    const error = ref<Error | null>(null)

    /**********************
     * Internals
     **********************/
    function _upsert(dest: Destination) {
        const idx = destinations.value.findIndex(d => d.id === dest.id)
        if (idx >= 0) destinations.value[idx] = { ...destinations.value[idx], ...dest }
        else destinations.value.push(dest)
    }

    function _remove(id: string) {
        destinations.value = destinations.value.filter(d => d.id !== id)
    }

    /**********************
     * Actions
     **********************/
    async function fetch() {
        loading.value = true
        error.value = null
        try {
            destinations.value = await apiFetch<Destination[]>('/destinations')
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            loading.value = false
        }
    }

    async function create(input: DestinationInput): Promise<Destination> {
        const dest = await apiFetch<Destination>('/destinations', {
            method: 'POST',
            body: input,
        })
        _upsert(dest)
        return dest
    }

    async function update(id: string, input: DestinationInput): Promise<Destination> {
        const dest = await apiFetch<Destination>(`/destinations/${id}`, {
            method: 'PUT',
            body: input,
        })
        _upsert(dest)
        return dest
    }

    async function remove(ids: string | string[]) {
        const list = Array.isArray(ids) ? ids : [ids]
        await Promise.all(list.map(id => apiFetch(`/destinations/${id}`, { method: 'DELETE' })))
        list.forEach(_remove)
    }

    /**********************
     * Lookups
     **********************/
    function findById(id: string): Destination | undefined {
        return destinations.value.find(d => d.id === id)
    }

    function search(query: string): Destination[] {
        if (!query) return destinations.value
        const q = query.toLowerCase()
        return destinations.value.filter(d =>
            (d.name?.toLowerCase().includes(q))
            || d.key.toLowerCase().includes(q),
        )
    }

    function $reset() {
        destinations.value = []
        loading.value = false
        error.value = null
    }

    return {
        destinations,
        loading,
        error,
        fetch,
        create,
        update,
        remove,
        findById,
        search,
        _upsert,
        _remove,
        $reset,
    }
})
