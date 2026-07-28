export interface NavPage {
    label: string
    icon: string
    to: string
}

export interface NavSection {
    label: string
    pages: NavPage[]
}

/** Icons for resource kinds — fallback to generic box. */
export const RESOURCE_KIND_ICONS: Record<string, string> = {
    connection: 'i-lucide-key-round',
    config: 'i-lucide-settings',
}

/** Capitalize and pluralize a kind string: "connection" → "Connections". */
export function kindLabel(kind: string): string {
    return kind.charAt(0).toUpperCase() + kind.slice(1) + 's'
}

/**
 * Single source of truth for the app's main pages: both the sidebar nav and
 * the command palette derive from it, so they can't drift apart.
 */
export function useNavSections() {
    const catalogStore = useCatalogStore()

    return computed<NavSection[]>(() => [
        {
            label: 'Overview',
            pages: [
                { label: 'Graph', icon: 'i-lucide-workflow', to: '/graph' },
                { label: 'Collection', icon: 'i-lucide-library', to: '/collection' },
            ],
        },
        {
            label: 'Entities',
            pages: [
                { label: 'Sources', icon: 'i-lucide-plug', to: '/sources' },
                { label: 'Destinations', icon: 'i-lucide-database', to: '/destinations' },
                ...catalogStore.resourceKinds.map(kind => ({
                    label: kindLabel(kind),
                    icon: RESOURCE_KIND_ICONS[kind] ?? 'i-lucide-box',
                    to: `/resources/${kind}`,
                })),
            ],
        },
        {
            label: 'Scheduling',
            pages: [
                { label: 'Jobs', icon: 'i-lucide-calendar-clock', to: '/jobs' },
                { label: 'Hooks', icon: 'i-carbon-lightning', to: '/hooks' },
                { label: 'Executions', icon: 'i-lucide-activity', to: '/executions' },
            ],
        },
    ])
}
