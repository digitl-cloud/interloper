import type { Source } from '~/types/source'
import type { Destination } from '~/types/destination'

export interface DestinationBadge {
    icon: string
    label: string
    count: number
    isMulti: boolean
}

export function useDestinationBadge() {
    const sourcesStore = useSourcesStore()
    const catalogStore = useCatalogStore()

    function getBadgeForDestinations(destinations: Destination[]): DestinationBadge | null {
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
            label: defn?.name ?? destination.name ?? destination.key,
            count: 1,
            isMulti: false,
        }
    }

    function getBadgeForSource(source: Source): DestinationBadge | null {
        return getBadgeForDestinations(source.destinations)
    }

    function getBadgeForAssetId(assetId: string): DestinationBadge | null {
        const source = sourcesStore.sources.find(s => s.assets.some(a => a.id === assetId))
        if (!source) return null
        return getBadgeForSource(source)
    }

    return { getBadgeForDestinations, getBadgeForSource, getBadgeForAssetId }
}
