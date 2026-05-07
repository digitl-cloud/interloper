import type { Source, SourceInput } from '~/types/source'

export const useSourcesStore = defineStore('sources', () => {
    const { apiFetch } = useApi()

    /**********************
     * State
     **********************/
    const sources = ref<Source[]>([])
    const loading = ref(false)
    const error = ref<Error | null>(null)

    /**********************
     * Internals
     **********************/
    function _upsert(source: Source) {
        const idx = sources.value.findIndex(s => s.id === source.id)
        if (idx >= 0) sources.value[idx] = { ...sources.value[idx], ...source }
        else sources.value.push(source)
    }

    function _remove(id: string) {
        sources.value = sources.value.filter(s => s.id !== id)
    }

    /**********************
     * Actions
     **********************/
    async function fetch() {
        loading.value = true
        error.value = null
        try {
            sources.value = await apiFetch<Source[]>('/sources')
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            loading.value = false
        }
    }

    async function create(input: SourceInput): Promise<Source> {
        const source = await apiFetch<Source>('/sources', {
            method: 'POST',
            body: input,
        })
        _upsert(source)
        return source
    }

    async function update(id: string, input: SourceInput): Promise<Source> {
        const source = await apiFetch<Source>(`/sources/${id}`, {
            method: 'PUT',
            body: input,
        })
        _upsert(source)
        return source
    }

    async function remove(ids: string | string[]) {
        const list = Array.isArray(ids) ? ids : [ids]
        await Promise.all(list.map(id => apiFetch(`/sources/${id}`, { method: 'DELETE' })))
        list.forEach(_remove)
    }

    /**********************
     * Lookups
     **********************/
    function findById(id: string): Source | undefined {
        return sources.value.find(s => s.id === id)
    }

    function search(query: string): Source[] {
        if (!query) return sources.value
        const q = query.toLowerCase()
        return sources.value.filter(s =>
            s.name.toLowerCase().includes(q)
            || s.key.toLowerCase().includes(q),
        )
    }

    function $reset() {
        sources.value = []
        loading.value = false
        error.value = null
    }

    return {
        sources,
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
