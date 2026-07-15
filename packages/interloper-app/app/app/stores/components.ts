import type { ComponentRecord, ComponentInput, Relation, RelationInput } from '~/types/component'

export const useComponentsStore = defineStore('components', () => {
    const { apiFetch } = useApi()

    /**********************
     * State
     **********************/
    const components = ref<ComponentRecord[]>([])
    const relations = ref<Relation[]>([])
    const loading = ref(false)
    const error = ref<Error | null>(null)

    /**********************
     * Getters
     **********************/
    /** All relations of type `dependency` (asset → upstream asset edges). */
    const dependencies = computed(() => relations.value.filter(r => r.type === 'dependency'))

    /**********************
     * Internals
     **********************/
    function _upsert(component: ComponentRecord) {
        const idx = components.value.findIndex(c => c.id === component.id)
        if (idx >= 0) components.value[idx] = { ...components.value[idx], ...component }
        else components.value.push(component)
    }

    function _remove(id: string) {
        // Deleting a source cascades its assets — drop children too.
        components.value = components.value.filter(c => c.id !== id && c.parent_id !== id)
    }

    /**********************
     * Actions
     **********************/
    /**
     * Fetch components, optionally narrowed to `kinds`. An unfiltered fetch
     * replaces the whole list; a filtered one replaces only entries of the
     * fetched kinds so pages loading different kinds don't clobber each other.
     */
    async function fetchAll(kinds?: string[]) {
        loading.value = true
        error.value = null
        try {
            const params = new URLSearchParams()
            for (const kind of kinds ?? []) params.append('kind', kind)
            const fetched = await apiFetch<ComponentRecord[]>(`/components${kinds?.length ? `?${params}` : ''}`)
            if (kinds?.length) {
                const kindSet = new Set(kinds)
                components.value = [...components.value.filter(c => !kindSet.has(c.kind)), ...fetched]
            }
            else {
                components.value = fetched
            }
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            loading.value = false
        }
    }

    /** Fetch one component (secret kinds come back with decoded `config`). */
    async function fetchOne(id: string): Promise<ComponentRecord> {
        const component = await apiFetch<ComponentRecord>(`/components/${id}`)
        _upsert(component)
        return component
    }

    async function create(input: ComponentInput): Promise<ComponentRecord> {
        const component = await apiFetch<ComponentRecord>('/components', {
            method: 'POST',
            body: input,
        })
        _upsert(component)
        return component
    }

    async function update(id: string, input: ComponentInput): Promise<ComponentRecord> {
        const component = await apiFetch<ComponentRecord>(`/components/${id}`, {
            method: 'PUT',
            body: input,
        })
        _upsert(component)
        return component
    }

    async function remove(ids: string | string[]) {
        const list = Array.isArray(ids) ? ids : [ids]
        const results = await Promise.allSettled(list.map(id => apiFetch(`/components/${id}`, { method: 'DELETE' })))
        list.forEach((id, i) => {
            if (results[i]!.status === 'fulfilled') _remove(id)
        })
        const failed = results.find((r): r is PromiseRejectedResult => r.status === 'rejected')
        if (failed) throw failed.reason
    }

    /** Fetch relations, optionally narrowed to a type (replaces that type only). */
    async function fetchRelations(type?: string) {
        const query = type ? `?type=${type}` : ''
        const fetched = await apiFetch<Relation[]>(`/components/relations${query}`)
        if (type) relations.value = [...relations.value.filter(r => r.type !== type), ...fetched]
        else relations.value = fetched
    }

    async function addRelation(id: string, input: { type: string } & RelationInput): Promise<Relation> {
        const relation = await apiFetch<Relation>(`/components/${id}/relations`, {
            method: 'POST',
            body: input,
        })
        relations.value = [
            ...relations.value.filter(r => !(r.src_id === id && r.type === input.type && r.dst_id === input.dst_id)),
            relation,
        ]
        return relation
    }

    async function removeRelation(id: string, type: string, dstId: string) {
        await apiFetch(`/components/${id}/relations/${type}/${dstId}`, { method: 'DELETE' })
        relations.value = relations.value.filter(
            r => !(r.src_id === id && r.type === type && r.dst_id === dstId),
        )
    }

    async function fetchPartitionRowCounts(id: string) {
        return apiFetch<{
            asset_key: string
            partition_column: string
            counts: Array<{ partition: string; row_count: number }>
        }>(`/components/${id}/partition-row-counts`)
    }

    /**********************
     * Lookups
     **********************/
    function byKind(kind: string): ComponentRecord[] {
        return components.value.filter(c => c.kind === kind)
    }

    function byId(id: string): ComponentRecord | undefined {
        return components.value.find(c => c.id === id)
    }

    /**
     * Components outside `ids` (and their children) that hold relations into
     * them — the client-side mirror of the API's delete guard, used to preview
     * "used by" before a delete is attempted. Referrers that are source-owned
     * assets resolve to their parent source, the unit the user can act on.
     */
    function usedBy(ids: string | string[]): ComponentRecord[] {
        const subtree = new Set(Array.isArray(ids) ? ids : [ids])
        for (const id of [...subtree]) {
            for (const child of byId(id)?.children ?? []) subtree.add(child.id)
        }
        const referrers = new Map<string, ComponentRecord>()
        for (const r of relations.value) {
            if (!subtree.has(r.dst_id) || subtree.has(r.src_id)) continue
            let src = byId(r.src_id)
            if (src?.parent_id && !subtree.has(src.parent_id)) src = byId(src.parent_id) ?? src
            if (src && !subtree.has(src.id)) referrers.set(src.id, src)
        }
        return [...referrers.values()]
    }

    function search(query: string, kind?: string): ComponentRecord[] {
        const base = kind ? byKind(kind) : components.value
        if (!query) return base
        const q = query.toLowerCase()
        return base.filter(c =>
            (c.name?.toLowerCase().includes(q) ?? false)
            || c.key.toLowerCase().includes(q),
        )
    }

    function $reset() {
        components.value = []
        relations.value = []
        loading.value = false
        error.value = null
    }

    useOrgScopedRefetch(async () => {
        await Promise.all([fetchAll(), fetchRelations()])
    }, $reset)

    /**********************
     * Realtime
     **********************/
    const orgStore = useOrganisationStore()

    // Component notifications are slim ({id, kind, parent_id}) — the payload
    // channel never carries configs — so changes refetch through the API.
    // A parent refetch keeps sources' embedded children fresh.
    function _refetchChanged(record: Record<string, any>) {
        fetchOne(record.id).catch(() => {})
        if (record.parent_id) fetchOne(record.parent_id).catch(() => {})
    }

    useRealtimeSubscription({
        table: 'components',
        scope: () => orgStore.organisation?.id,
        onInsert: _refetchChanged,
        onUpdate: _refetchChanged,
        onDelete: (record: Record<string, any>) => {
            _remove(record.id)
            relations.value = relations.value.filter(r => r.src_id !== record.id && r.dst_id !== record.id)
            if (record.parent_id) fetchOne(record.parent_id).catch(() => {})
        },
    })

    // Relation rows are small and arrive whole — mirror them locally.
    useRealtimeSubscription({
        table: 'component_relations',
        scope: () => orgStore.organisation?.id,
        onInsert: (record: Record<string, any>) => {
            const key = (r: Relation) => `${r.src_id}|${r.type}|${r.slot}|${r.dst_id}`
            const incoming = record as Relation
            if (!relations.value.some(r => key(r) === key(incoming))) relations.value.push(incoming)
        },
        onUpdate: () => {},
        onDelete: (record: Record<string, any>) => {
            relations.value = relations.value.filter(
                r => !(r.src_id === record.src_id && r.type === record.type
                    && r.slot === record.slot && r.dst_id === record.dst_id),
            )
        },
    })

    return {
        components,
        relations,
        dependencies,
        loading,
        error,
        fetchAll,
        fetchOne,
        create,
        update,
        remove,
        fetchRelations,
        addRelation,
        removeRelation,
        fetchPartitionRowCounts,
        byKind,
        byId,
        usedBy,
        search,
        _upsert,
        _remove,
        $reset,
    }
})
