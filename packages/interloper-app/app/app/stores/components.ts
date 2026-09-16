import type { ComponentRecord, ComponentInput, DeleteImpact, Relation, RelationInput } from '~/types/component'

export const useComponentsStore = defineStore('components', () => {
    const { apiFetch } = useApi()
    const toast = useToast()

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
    /** All asset-to-asset relations (an asset's declared upstreams). */
    const upstreams = computed(() => relations.value.filter(r => r.src_kind === 'asset' && r.dst_kind === 'asset'))

    /**********************
     * Internals
     **********************/
    /** Place a fetched record: inside its owner's children when it has one, among the roots otherwise. */
    function _upsert(component: ComponentRecord) {
        const siblings = component.parent_id ? byId(component.parent_id)?.children : components.value
        if (!siblings) return
        const idx = siblings.findIndex(c => c.id === component.id)
        if (idx >= 0) siblings[idx] = { ...siblings[idx], ...component }
        else siblings.push(component)
    }

    /**
     * Record a failed fetch and say so out loud.
     *
     * A swallowed failure is indistinguishable from an empty organisation:
     * the pages render their "nothing here yet" states over a server that
     * refused the request. `error` drives the tables' error state; the toast
     * covers the pages that read the store without one (graph, timeline).
     */
    function _reportFetchFailure(e: unknown, what: string) {
        error.value = e as Error
        toast.add(errorToast(e, `Failed to load ${what}`))
    }

    /** Drop a record wherever it sits: a root (its owned components with it) or one owner's child. */
    function _remove(id: string) {
        components.value = components.value.filter(c => c.id !== id)
        for (const root of components.value) {
            if (root.children?.some(c => c.id === id)) root.children = root.children.filter(c => c.id !== id)
        }
    }

    /**********************
     * Actions
     **********************/
    /**
     * Fetch root components, optionally narrowed to `kinds`. An unfiltered
     * fetch replaces the whole list; a filtered one replaces only entries of
     * the fetched kinds so pages loading different kinds don't clobber each
     * other. Owned components (a source's assets) arrive under their owner's
     * `children`, never as entries of their own.
     */
    async function fetchAll(kinds?: string[]) {
        loading.value = true
        error.value = null
        try {
            const params = new URLSearchParams()
            for (const kind of kinds ?? []) params.append('kind', kind)
            const fetched = await apiFetch<ComponentRecord[]>(`/components/${kinds?.length ? `?${params}` : ''}`)
            if (kinds?.length) {
                const kindSet = new Set(kinds)
                components.value = [...components.value.filter(c => !kindSet.has(c.kind)), ...fetched]
            }
            else {
                components.value = fetched
            }
        }
        catch (e) {
            _reportFetchFailure(e, 'components')
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
        const component = await apiFetch<ComponentRecord>('/components/', {
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

    /**
     * Fetch relations, optionally narrowed to a name (replaces that name only).
     * Reported rather than thrown: pages fire it without awaiting, so a
     * rejection would otherwise surface nowhere but the console.
     */
    async function fetchRelations(name?: string) {
        const query = name ? `?name=${name}` : ''
        try {
            const fetched = await apiFetch<Relation[]>(`/components/relations${query}`)
            if (name) relations.value = [...relations.value.filter(r => r.name !== name), ...fetched]
            else relations.value = fetched
        }
        catch (e) {
            _reportFetchFailure(e, 'component relations')
        }
    }

    /**
     * Add one relation binding (`{ name, dst_id }`).
     *
     * Pass `many: false` when the name is single-valued: the API repoints it,
     * dropping the binding it held, so the mirror has to drop that row too
     * or the old binding lingers. Otherwise only the identical binding is
     * replaced, since a `many` name accumulates.
     */
    async function addRelation(
        id: string,
        input: { name: string } & RelationInput,
        options?: { many: boolean },
    ): Promise<Relation> {
        const relation = await apiFetch<Relation>(`/components/${id}/relations`, {
            method: 'POST',
            body: input,
        })
        const repoints = options?.many === false
        relations.value = [
            ...relations.value.filter(
                r => !(r.src_id === id && r.name === input.name && (repoints || r.dst_id === input.dst_id)),
            ),
            relation,
        ]
        return relation
    }

    /** Remove one relation binding by name and destination id. */
    async function removeRelation(id: string, name: string, dstId: string) {
        await apiFetch(`/components/${id}/relations/${name}/${dstId}`, { method: 'DELETE' })
        relations.value = relations.value.filter(
            r => !(r.src_id === id && r.name === name && r.dst_id === dstId),
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
    /** Every component by id, owned ones included: the roots' trees flattened. */
    const index = computed(() => {
        const map = new Map<string, ComponentRecord>()
        const visit = (record: ComponentRecord) => {
            map.set(record.id, record)
            for (const child of record.children ?? []) visit(child)
        }
        for (const root of components.value) visit(root)
        return map
    })

    /** Every component, owned ones included: the flat view of the collection. */
    const all = computed(() => [...index.value.values()])

    function byKind(kind: string): ComponentRecord[] {
        return all.value.filter(c => c.kind === kind)
    }

    function byId(id: string): ComponentRecord | undefined {
        return index.value.get(id)
    }

    /**
     * The server's delete preview for `ids`: the same rule its guard enforces,
     * evaluated before anything is deleted. `blocking` referrers make the
     * backend refuse with 409; `detaching` ones just lose the relation.
     */
    async function deleteImpact(ids: string | string[]): Promise<DeleteImpact> {
        const params = new URLSearchParams()
        for (const id of Array.isArray(ids) ? ids : [ids]) params.append('id', id)
        return apiFetch<DeleteImpact>(`/components/delete-impact?${params}`)
    }

    function search(query: string, kind?: string): ComponentRecord[] {
        const base = kind ? byKind(kind) : all.value
        if (!query) return base
        const q = query.toLowerCase()
        return base.filter(c =>
            (c.name?.toLowerCase().includes(q) ?? false)
            || c.key.toLowerCase().includes(q),
        )
    }

    /** Refetch everything the collection pages read: the retry their tables offer. */
    async function reload() {
        await Promise.all([fetchAll(), fetchRelations()])
    }

    function $reset() {
        components.value = []
        relations.value = []
        loading.value = false
        error.value = null
    }

    useOrgScopedRefetch(reload, $reset)

    /**********************
     * Realtime
     **********************/
    const orgStore = useOrganisationStore()

    // Component notifications are slim ({id, kind, parent_id}) — the payload
    // channel never carries configs — so changes refetch through the API. The
    // owner is the unit: an owned component's change refetches its owner,
    // whose detail carries the children.
    function _refetchChanged(record: Record<string, any>) {
        fetchOne(record.parent_id ?? record.id).catch(() => {})
    }

    useRealtimeSubscription({
        table: 'components',
        scope: () => orgStore.organisation?.id,
        onInsert: _refetchChanged,
        onUpdate: _refetchChanged,
        onDelete: (record: Record<string, any>) => {
            _remove(record.id)
            relations.value = relations.value.filter(r => r.src_id !== record.id && r.dst_id !== record.id)
        },
    })

    // Relation rows are small and arrive whole — mirror them locally.
    useRealtimeSubscription({
        table: 'component_relations',
        scope: () => orgStore.organisation?.id,
        onInsert: (record: Record<string, any>) => {
            const key = (r: Relation) => `${r.src_id}|${r.name}|${r.dst_id}`
            const incoming = record as Relation
            if (!relations.value.some(r => key(r) === key(incoming))) relations.value.push(incoming)
        },
        onUpdate: () => {},
        onDelete: (record: Record<string, any>) => {
            relations.value = relations.value.filter(
                r => !(r.src_id === record.src_id && r.name === record.name && r.dst_id === record.dst_id),
            )
        },
    })

    return {
        components,
        all,
        relations,
        upstreams,
        loading,
        error,
        fetchAll,
        fetchOne,
        create,
        update,
        remove,
        fetchRelations,
        reload,
        addRelation,
        removeRelation,
        fetchPartitionRowCounts,
        byKind,
        byId,
        deleteImpact,
        search,
        _upsert,
        _remove,
        $reset,
    }
})
