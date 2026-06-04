import type { Resource, ResourceDetail, ResourceInput } from '~/types/resource'

export const useResourcesStore = defineStore('resources', () => {
    const { apiFetch } = useApi()

    /**********************
     * State
     **********************/
    const resources = ref<Resource[]>([])
    const loading = ref(false)
    const error = ref<Error | null>(null)

    /**********************
     * Internals
     **********************/
    function _upsert(resource: Resource) {
        const idx = resources.value.findIndex(r => r.id === resource.id)
        if (idx >= 0) resources.value[idx] = { ...resources.value[idx], ...resource }
        else resources.value.push(resource)
    }

    function _remove(id: string) {
        resources.value = resources.value.filter(r => r.id !== id)
    }

    /**********************
     * Actions
     **********************/
    async function fetch(kind?: string) {
        loading.value = true
        error.value = null
        try {
            const query = kind ? `?kind=${kind}` : ''
            resources.value = await apiFetch<Resource[]>(`/resources${query}`)
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            loading.value = false
        }
    }

    async function fetchOne(id: string): Promise<ResourceDetail> {
        return apiFetch<ResourceDetail>(`/resources/${id}`)
    }

    async function create(input: ResourceInput): Promise<Resource> {
        const resource = await apiFetch<Resource>('/resources', {
            method: 'POST',
            body: input,
        })
        _upsert(resource)
        return resource
    }

    async function update(id: string, input: ResourceInput): Promise<Resource> {
        const resource = await apiFetch<Resource>(`/resources/${id}`, {
            method: 'PUT',
            body: input,
        })
        _upsert(resource)
        return resource
    }

    async function remove(ids: string | string[]) {
        const list = Array.isArray(ids) ? ids : [ids]
        await Promise.all(list.map(id => apiFetch(`/resources/${id}`, { method: 'DELETE' })))
        list.forEach(_remove)
    }

    /**********************
     * Lookups
     **********************/
    function findById(id: string): Resource | undefined {
        return resources.value.find(r => r.id === id)
    }

    function byKind(kind: string): Resource[] {
        return resources.value.filter(r => r.kind === kind)
    }

    function search(query: string, kind?: string): Resource[] {
        const base = kind ? byKind(kind) : resources.value
        if (!query) return base
        const q = query.toLowerCase()
        return base.filter(r =>
            r.name.toLowerCase().includes(q)
            || r.key.toLowerCase().includes(q),
        )
    }

    function $reset() {
        resources.value = []
        loading.value = false
        error.value = null
    }

    useOrgScopedRefetch(() => fetch(), $reset)

    return {
        resources,
        loading,
        error,
        fetch,
        fetchOne,
        create,
        update,
        remove,
        findById,
        byKind,
        search,
        _upsert,
        _remove,
        $reset,
    }
})
