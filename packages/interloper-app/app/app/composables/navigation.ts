export interface NavPage {
    label: string
    icon: string
    to: string
    /** Aliases the command palette also matches on (e.g. "DAG" → Graph). Not rendered. */
    keywords?: string[]
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
                { label: 'Graph', icon: 'i-lucide-workflow', to: '/graph', keywords: ['dag', 'pipeline', 'lineage'] },
                { label: 'Collection', icon: 'i-lucide-library', to: '/collection', keywords: ['catalog', 'library'] },
            ],
        },
        {
            label: 'Entities',
            pages: [
                { label: 'Sources', icon: 'i-lucide-plug', to: '/sources', keywords: ['connectors', 'integrations'] },
                { label: 'Destinations', icon: 'i-lucide-database', to: '/destinations', keywords: ['warehouse', 'export'] },
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
                { label: 'Jobs', icon: 'i-lucide-calendar-clock', to: '/jobs', keywords: ['cron', 'schedule'] },
                { label: 'Hooks', icon: 'i-carbon-lightning', to: '/hooks', keywords: ['triggers', 'automation'] },
                { label: 'Executions', icon: 'i-lucide-activity', to: '/executions', keywords: ['runs', 'backfills', 'history'] },
            ],
        },
    ])
}
