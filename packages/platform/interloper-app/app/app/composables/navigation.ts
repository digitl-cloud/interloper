import type { RouteLocationRaw } from 'vue-router'

export interface NavPage {
    label: string
    icon: string
    to: string
    /** Aliases the command palette also matches on (e.g. "DAG" → Graph). Not rendered. */
    keywords?: string[]
    /** Owns its own route only, not the routes beneath it: the organisation's landing page. */
    exact?: boolean
}

/** A view in a page's tab group: a route, or a `?tab=` query with its own `active` flag. */
export interface PageTab {
    label: string
    to: RouteLocationRaw
    active?: boolean
    badge?: string | number
}

/** A sidebar destination; a hub also carries its routed views, shown in its page toolbar and as its sidebar submenu. */
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

/** List view of a kind inside the Components hub, within the organisation ("connection" → "/components/connections"). */
export function kindPath(kind: string): string {
    return `/components/${kindSlug(kind)}`
}

/** Views of the Executions hub, in toolbar order. */
export function useExecutionViews() {
    const organisationStore = useOrganisationStore()

    return computed<NavPage[]>(() => [
        { label: 'Runs', icon: 'i-lucide-activity', to: organisationStore.path('/executions/runs'), keywords: ['executions', 'history'] },
        { label: 'Backfills', icon: 'i-lucide-history', to: organisationStore.path('/executions/backfills'), keywords: ['executions'] },
    ])
}

/** Views of the Organization hub; its settings only for the organisation's admins. */
export function useOrganizationViews() {
    const organisationStore = useOrganisationStore()

    return computed<NavPage[]>(() => [
        { label: 'Members', icon: 'i-lucide-users', to: organisationStore.path('/organization/members'), keywords: ['team', 'invite'] },
        ...(organisationStore.role === 'admin'
            ? [{ label: 'Settings', icon: 'i-lucide-sliders-horizontal', to: organisationStore.path('/organization/settings'), keywords: ['name', 'key'] }]
            : []),
    ])
}

/** Views of the Components hub, one per kind, in lifecycle order. */
export function useComponentViews() {
    const catalogStore = useCatalogStore()
    const organisationStore = useOrganisationStore()

    return computed<NavPage[]>(() => [
        { label: 'Sources', icon: 'i-lucide-plug', to: organisationStore.path(kindPath('source')), keywords: ['components', 'connectors', 'integrations'] },
        { label: 'Destinations', icon: 'i-lucide-database', to: organisationStore.path(kindPath('destination')), keywords: ['warehouse', 'export'] },
        ...catalogStore.resourceKinds.map(kind => ({
            label: kindLabel(kind),
            icon: RESOURCE_KIND_ICONS[kind] ?? 'i-lucide-box',
            to: organisationStore.path(kindPath(kind)),
        })),
        { label: 'Jobs', icon: 'i-lucide-calendar-clock', to: organisationStore.path(kindPath('job')), keywords: ['cron', 'schedule'] },
        { label: 'Hooks', icon: 'i-carbon-lightning', to: organisationStore.path(kindPath('hook')), keywords: ['triggers', 'automation'] },
    ])
}

/**
 * Single source of truth for the app's main pages: the sidebar renders the
 * destinations with each hub's views as a submenu, each hub page renders those
 * views in its toolbar, and the command palette flattens both — so none of
 * them can drift apart.
 */
export function useNavDestinations() {
    const organisationStore = useOrganisationStore()
    const componentViews = useComponentViews()
    const executionViews = useExecutionViews()

    return computed<NavDestination[]>(() => [
        { label: 'Overview', icon: 'i-lucide-layout-dashboard', to: organisationStore.path('/'), exact: true, keywords: ['home', 'dashboard', 'health'] },
        { label: 'Timeline', icon: 'i-lucide-gantt-chart', to: organisationStore.path('/timeline'), keywords: ['gantt', 'schedule', 'runs', 'history'] },
        { label: 'Graph', icon: 'i-lucide-workflow', to: organisationStore.path('/graph'), keywords: ['dag', 'pipeline', 'lineage'] },
        { label: 'Collection', icon: 'i-lucide-library', to: organisationStore.path('/collection'), keywords: ['catalog', 'library'] },
        { label: 'Components', icon: 'i-lucide-boxes', to: organisationStore.path(kindPath('source')), views: componentViews.value },
        { label: 'Executions', icon: 'i-lucide-activity', to: organisationStore.path('/executions/runs'), views: executionViews.value },
    ])
}

/** Whether a destination owns a route: itself, one of its views, or anything beneath either unless `exact`. */
export function isNavActive(destination: NavDestination, path: string): boolean {
    return [destination, ...(destination.views ?? [])].some(page => path === page.to || (!page.exact && path.startsWith(`${page.to}/`)))
}
