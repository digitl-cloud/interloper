/**
 * The upstream sources a source or an asset may bind. The framework gives the `upstream` relation to every source
 * and asset whose relations read another source's assets; binding sources there narrows them to those sources.
 */
import type { AssetDefinition, Catalog, ComponentDefinition, SourceDefinition } from '~/types/catalog'
import { ANY_SOURCE, keysOf, upstreamRelations } from '~/types/catalog'
import type { ComponentRecord } from '~/types/component'

/** The relation upstream sources are bound under. */
export const UPSTREAM = 'upstream'

/** Sources that fill the same relations, and the names of those relations. */
export interface UpstreamGroup {
    relations: string[]
    sources: ComponentRecord[]
}

/** Whether a definition carries the `upstream` relation, so its upstream sources can be chosen. */
export function carriesUpstream(definition: ComponentDefinition | undefined): boolean {
    return !!definition?.relations?.[UPSTREAM]
}

/** "fact_campaign_performance, matches" → "Fact campaign performance, matches". */
export function upstreamGroupLabel(group: UpstreamGroup): string {
    const label = group.relations.join(', ').replaceAll('_', ' ')
    return label.charAt(0).toUpperCase() + label.slice(1)
}

/**
 * The organisation's sources whose definition holds an asset filling one of the owner's relations that read
 * another source, grouped by the relations they fill, each group sorted by name. Relations filled by the same
 * sources share a group, so a star schema over one platform shows one group. The owner itself is never offered.
 */
export function upstreamCandidates(
    definition: ComponentDefinition,
    ownSourceKey: string | null,
    sources: ComponentRecord[],
    catalog: Catalog,
    ownId?: string,
): UpstreamGroup[] {
    const readers = definition.kind === 'source' ? (definition as SourceDefinition).assets : [definition]
    const groups = new Map<string, UpstreamGroup>()
    for (const reader of readers) {
        for (const [name, relation] of Object.entries(upstreamRelations(reader as AssetDefinition))) {
            const fillers = sources
                .filter(source => source.id !== ownId && keysOf(relation).some(key => fills(key, source, ownSourceKey, catalog)))
                .sort((a, b) => (a.name ?? a.key).localeCompare(b.name ?? b.key))
            if (!fillers.length) continue
            const handle = fillers.map(source => source.id).join()
            const group = groups.get(handle)
            if (group && !group.relations.includes(name)) group.relations.push(name)
            else if (!group) groups.set(handle, { relations: [name], sources: fillers })
        }
    }
    return [...groups.values()]
}

/** Whether `source` holds an asset a declared key names outside the owner's own source. */
function fills(declared: string, source: ComponentRecord, ownSourceKey: string | null, catalog: Catalog): boolean {
    const dot = declared.lastIndexOf('.')
    if (dot < 0) return false
    const sourceKey = declared.slice(0, dot)
    const assetKey = declared.slice(dot + 1)
    if (sourceKey === ownSourceKey || (sourceKey !== ANY_SOURCE && sourceKey !== source.key)) return false
    const definition = catalog[source.key] as SourceDefinition | undefined
    return definition?.assets?.some(asset => asset.key === assetKey) ?? false
}
