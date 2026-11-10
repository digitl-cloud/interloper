import type { ComponentRecord } from '~/types/component'
import { relationRefs } from '~/types/component'

/** What the badge needs of a destination: its catalog key and display name. */
export interface DestinationLike {
    key: string
    name: string | null
}

export interface DestinationBadge {
    icon: string
    label: string
    count: number
    isMulti: boolean
}

export function useDestinationBadge() {
    const componentsStore = useComponentsStore()
    const catalogStore = useCatalogStore()

    function getBadgeForDestinations(destinations: DestinationLike[]): DestinationBadge | null {
        if (destinations.length === 0) return null

        if (destinations.length > 1) {
            return {
                icon: 'i-lucide-database',
                label: `${destinations.length} destinations`,
                count: destinations.length,
                isMulti: true,
            }
        }

        const destination = destinations[0]!
        const defn = catalogStore.getDestinationDefinition(destination.key)

        return {
            icon: defn?.icon ?? 'i-lucide-hard-drive',
            label: destination.name ?? defn?.name ?? destination.key,
            count: 1,
            isMulti: false,
        }
    }

    function getBadgeForSource(source: ComponentRecord): DestinationBadge | null {
        return getBadgeForDestinations(
            relationRefs(source, 'destinations').map(ref => ({ key: ref.dst_key, name: ref.dst_name })),
        )
    }

    function getBadgeForAssetId(assetId: string): DestinationBadge | null {
        const source = componentsStore.byKind('source').find(s => s.children.some(a => a.id === assetId))
        if (!source) return null
        return getBadgeForSource(source)
    }

    return { getBadgeForDestinations, getBadgeForSource, getBadgeForAssetId }
}
