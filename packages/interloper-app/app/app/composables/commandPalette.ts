import type { CommandPaletteGroup } from '@nuxt/ui'

/** Capitalize and pluralize a kind string: "connection" → "Connections". */
function kindLabel(kind: string): string {
    return kind.charAt(0).toUpperCase() + kind.slice(1) + 's'
}

const RESOURCE_KIND_ICONS: Record<string, string> = {
    connection: 'i-lucide-key-round',
    config: 'i-lucide-settings',
}

export function useCommandPalette() {
    const catalogStore = useCatalogStore()
    const open = ref(false)

    const groups = computed<CommandPaletteGroup[]>(() => {
        const resourceItems = catalogStore.resourceKinds.map(kind => ({
            label: kindLabel(kind),
            icon: RESOURCE_KIND_ICONS[kind] ?? 'i-lucide-box',
            to: `/resources/${kind}`,
        }))

        return [
            {
                id: 'pages',
                label: 'Pages',
                items: [
                    { label: 'Sources', icon: 'i-lucide-plug', to: '/sources' },
                    { label: 'Destinations', icon: 'i-lucide-database', to: '/destinations' },
                    ...resourceItems,
                    { label: 'Jobs', icon: 'i-lucide-calendar-clock', to: '/jobs' },
                ],
            },
        ]
    })

    return { open, groups }
}
