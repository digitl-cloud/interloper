import type { ComponentRecord, ComponentInput, DeleteImpact, Relation, RelationInput } from '~/types/component'

/** The key an unfiltered listing is held under, beside the kinds. */
const ALL_KINDS = '*'

export const useComponentsStore = defineStore('components', () => {
    const { apiFetch, fetchAll: fetchPages } = useOrgApi()
    const toast = useToast()

    /**********************
     * State
     **********************/
    const components = ref<ComponentRecord[]>([])
    const relations = ref<Relation[]>([])
    const pending = ref(0)
    const error = ref<Error | null>(null)

    /**
     * The listings asked for in the organisation, by kind (`ALL_KINDS` for an
     * unfiltered one), each with the request that brought or is bringing it:
     * once held, the realtime handlers keep it current and nothing fetches it
     * again. `null` marks a request that failed, so the next ask fetches it
     * again and a reload knows to.
     */
    const listings = new Map<string, Promise<void> | null>()
    /** The relations listing, the same way; `undefined` until asked for. */
    let relationsListing: Promise<void> | null | undefined

    /**********************
     * Getters
     **********************/
    /** All asset-to-asset relations (an asset's declared upstreams). */
    const upstreams = computed(() => relations.value.filter(r => r.src_kind === 'asset' && r.dst_kind === 'asset'))
    const loading = computed(() => pending.value > 0)

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
        _relabel(component)
    }

    /**
     * Carry a component's name onto the embedded relations that point at it:
     * a rename writes no other row, so no message names its referrers.
     */
    function _relabel(component: ComponentRecord) {
        for (const record of all.value) {
            for (const refs of Object.values(record.relations ?? {})) {
                for (const ref of refs) {
                    if (ref.dst_id === component.id) ref.dst_name = component.name
                }
            }
        }
    }

    /**
     * Mirror a relation row onto its source's embedded `relations`, which a
     * relation message leaves alone otherwise. A new binding is labelled from
     * the destination the store holds; one it does not hold refetches the
     * source, whose detail names it.
     */
    function _embed(relation: Relation) {
        const src = byId(relation.src_id)
        if (!src) return
        const refs = (src.relations ??= {})[relation.name] ??= []
        if (refs.some(ref => ref.dst_id === relation.dst_id)) return
        const dst = byId(relation.dst_id)
        if (dst) refs.push({ dst_id: dst.id, dst_kind: dst.kind, dst_key: dst.key, dst_name: dst.name })
        else _refetchChanged(src)
    }

    function _unembed(relation: Relation) {
        const src = byId(relation.src_id)
        const refs = src?.relations?.[relation.name]
        if (src && refs) src.relations[relation.name] = refs.filter(ref => ref.dst_id !== relation.dst_id)
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

    /**
     * Fetch one listing, recorded under each of its keys until it fails.
     * The failure is matched on the request itself, so a failed request a
     * reload has since replaced does not unmark the one that replaced it.
     */
    function _requestListing(kinds?: string[]): Promise<void> {
        const keys = kinds ?? [ALL_KINDS]
        const request = _fetchListing(kinds).then((ok) => {
            if (ok) return
            for (const key of keys) {
                if (listings.get(key) === request) listings.set(key, null)
            }
        })
        for (const key of keys) listings.set(key, request)
        return request
    }

    /**
     * Fetch root components, optionally narrowed to `kinds`, and say whether
     * they landed. An unfiltered fetch replaces the whole list; a filtered
     * one replaces only entries of the fetched kinds so pages loading
     * different kinds don't clobber each other.
     */
    async function _fetchListing(kinds?: string[]): Promise<boolean> {
        pending.value++
        error.value = null
        try {
            const params = new URLSearchParams()
            for (const kind of kinds ?? []) params.append('kind', kind)
            const fetched = await fetchPages<ComponentRecord>('/components', params)
            if (kinds) {
                const kindSet = new Set(kinds)
                components.value = [...components.value.filter(c => !kindSet.has(c.kind)), ...fetched]
            }
            else {
                components.value = fetched
            }
            return true
        }
        catch (e) {
            _reportFetchFailure(e, 'components')
            return false
        }
        finally {
            pending.value--
        }
    }

    async function _fetchRelations(): Promise<boolean> {
        try {
            relations.value = await fetchPages<Relation>('/relations')
            return true
        }
        catch (e) {
            _reportFetchFailure(e, 'component relations')
            return false
        }
    }

    /**********************
     * Actions
     **********************/
    /**
     * Hold the root components of `kinds`, or of every kind when none are
     * given, and resolve once they are held. Only what the store neither
     * holds nor is fetching for the organisation is fetched: the realtime
     * handlers keep what it holds current. The API has no "every kind but
     * these", so the first unfiltered call fetches every kind, those held
     * included.
     *
     * Owned components (a source's assets) arrive under their owner's
     * `children`, never as entries of their own: `source` brings the
     * sources' assets with them, `asset` the standalone assets alone.
     */
    function fetchAll(kinds?: string[]): Promise<void> {
        const everything = listings.get(ALL_KINDS)
        if (everything) return everything
        if (!kinds?.length) return _requestListing()
        const missing = kinds.filter(kind => !listings.get(kind))
        if (missing.length) _requestListing(missing)
        return Promise.all(kinds.map(kind => listings.get(kind))).then(() => {})
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

    /**
     * Hold every relation of the organisation, fetched once like the
     * listings and kept current by the relation messages. A failure is
     * reported rather than thrown: pages fire it without awaiting, so a
     * rejection would otherwise surface nowhere but the console.
     */
    function fetchRelations(): Promise<void> {
        if (relationsListing) return relationsListing
        const request = _fetchRelations().then((ok) => {
            if (!ok && relationsListing === request) relationsListing = null
        })
        relationsListing = request
        return request
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

    /**
     * Fetch again everything asked for in the organisation, failed fetches
     * included: the retry an error state offers, and the catch-up after the
     * realtime socket reconnects, since what changed while it was down never
     * reached the store.
     */
    async function reload() {
        const kinds = [...listings.keys()]
        const withRelations = relationsListing !== undefined
        listings.clear()
        relationsListing = undefined
        await Promise.all([
            kinds.includes(ALL_KINDS) ? fetchAll() : kinds.length ? fetchAll(kinds) : undefined,
            withRelations ? fetchRelations() : undefined,
        ])
    }

    function $reset() {
        components.value = []
        relations.value = []
        error.value = null
        listings.clear()
        relationsListing = undefined
    }

    /**********************
     * Realtime
     **********************/
    const orgStore = useOrganisationStore()
    watch(() => orgStore.organisation?.id, $reset)

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
        // Relations included: the one reload covers both subscriptions.
        onReconnect: reload,
    })

    // Relation rows are small and arrive whole — mirror them locally.
    useRealtimeSubscription({
        table: 'component_relations',
        scope: () => orgStore.organisation?.id,
        onInsert: (record: Record<string, any>) => {
            const key = (r: Relation) => `${r.src_id}|${r.name}|${r.dst_id}`
            const incoming = record as Relation
            if (!relations.value.some(r => key(r) === key(incoming))) relations.value.push(incoming)
            _embed(incoming)
        },
        onUpdate: () => {},
        onDelete: (record: Record<string, any>) => {
            relations.value = relations.value.filter(
                r => !(r.src_id === record.src_id && r.name === record.name && r.dst_id === record.dst_id),
            )
            _unembed(record as Relation)
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
