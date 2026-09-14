export interface NavPage {
    label: string
    icon: string
    to: string
    /** Aliases the command palette also matches on (e.g. "DAG" → Graph). Not rendered. */
    keywords?: string[]
}

/** A sidebar destination; a hub also carries its routed views, shown as its strip and as its submenu. */
export interface NavDestination extends NavPage {
    views?: NavPage[]
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

/** Route slug of a kind, its plural ("connection" → "connections"). */
export function kindSlug(kind: string): string {
    return `${kind}s`
}

/** Inverse of :func:`kindSlug`: the kind behind a slug ("connections" → "connection"). */
export function kindFromSlug(slug: string): string {
    return slug.slice(0, -1)
}

/** List view of a kind inside the Components hub ("connection" → "/components/connections"). */
export function kindPath(kind: string): string {
    return `/components/${kindSlug(kind)}`
}

/** Views of the Executions hub, in strip order. */
export const EXECUTION_VIEWS: NavPage[] = [
    { label: 'Runs', icon: 'i-lucide-activity', to: '/executions/runs', keywords: ['executions', 'history'] },
    { label: 'Backfills', icon: 'i-lucide-history', to: '/executions/backfills', keywords: ['executions'] },
]

/** Views of the Components hub, one per kind, in lifecycle order. */
export function useComponentViews() {
    const catalogStore = useCatalogStore()

    return computed<NavPage[]>(() => [
        { label: 'Sources', icon: 'i-lucide-plug', to: kindPath('source'), keywords: ['components', 'connectors', 'integrations'] },
        { label: 'Destinations', icon: 'i-lucide-database', to: kindPath('destination'), keywords: ['warehouse', 'export'] },
        ...catalogStore.resourceKinds.map(kind => ({
            label: kindLabel(kind),
            icon: RESOURCE_KIND_ICONS[kind] ?? 'i-lucide-box',
            to: kindPath(kind),
        })),
        { label: 'Jobs', icon: 'i-lucide-calendar-clock', to: kindPath('job'), keywords: ['cron', 'schedule'] },
        { label: 'Hooks', icon: 'i-carbon-lightning', to: kindPath('hook'), keywords: ['triggers', 'automation'] },
    ])
}

/**
 * Single source of truth for the app's main pages: the sidebar renders the
 * destinations with each hub's views as a submenu, each hub renders those
 * views as a strip, and the command palette flattens both — so none of
 * them can drift apart.
 */
export function useNavDestinations() {
    const componentViews = useComponentViews()

    return computed<NavDestination[]>(() => [
        { label: 'Timeline', icon: 'i-lucide-gantt-chart', to: '/timeline', keywords: ['gantt', 'schedule', 'runs', 'history'] },
        { label: 'Graph', icon: 'i-lucide-workflow', to: '/graph', keywords: ['dag', 'pipeline', 'lineage'] },
        { label: 'Collection', icon: 'i-lucide-library', to: '/collection', keywords: ['catalog', 'library'] },
        { label: 'Components', icon: 'i-lucide-boxes', to: kindPath('source'), views: componentViews.value },
        { label: 'Executions', icon: 'i-lucide-activity', to: '/executions/runs', views: EXECUTION_VIEWS },
    ])
}

/** Whether a destination owns a route: itself, one of its views, or anything beneath either. */
export function isNavActive(destination: NavDestination, path: string): boolean {
    return [destination, ...(destination.views ?? [])].some(page => path === page.to || path.startsWith(`${page.to}/`))
}
