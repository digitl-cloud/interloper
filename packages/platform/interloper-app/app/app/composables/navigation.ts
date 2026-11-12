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
    icon?: string
    active?: boolean
    badge?: string | number
}

/** A sidebar destination; a hub also carries its routed views, shown in its page toolbar and as its sidebar submenu. */
export interface NavDestination extends NavPage {
    views?: NavPage[]
}

/** The icon of a resource kind, from the app's concept icons; an unknown kind is a generic asset box. */
export function resourceKindIcon(kind: string): string {
    const { icons } = useAppConfig()
    return (icons as Record<string, string>)[kind] ?? icons.asset
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
    const { icons } = useAppConfig()
    const organisationStore = useOrganisationStore()

    return computed<NavPage[]>(() => [
        { label: 'Runs', icon: icons.run, to: organisationStore.path('/executions/runs'), keywords: ['executions', 'history'] },
        { label: 'Backfills', icon: icons.backfill, to: organisationStore.path('/executions/backfills'), keywords: ['executions'] },
    ])
}

/** Views of the Organisation hub; its settings only for the organisation's admins. */
export function useOrganizationViews() {
    const { icons } = useAppConfig()
    const organisationStore = useOrganisationStore()

    return computed<NavPage[]>(() => [
        { label: 'Members', icon: icons.members, to: organisationStore.path('/organization/members'), keywords: ['team', 'invite'] },
        ...(organisationStore.role === 'admin'
            ? [{ label: 'Settings', icon: icons.settings, to: organisationStore.path('/organization/settings'), keywords: ['name', 'key'] }]
            : []),
    ])
}

/** Views of the Components hub, one per kind, in lifecycle order. */
export function useComponentViews() {
    const { icons } = useAppConfig()
    const catalogStore = useCatalogStore()
    const organisationStore = useOrganisationStore()

    return computed<NavPage[]>(() => [
        { label: 'Sources', icon: icons.source, to: organisationStore.path(kindPath('source')), keywords: ['components', 'connectors', 'integrations'] },
        { label: 'Destinations', icon: icons.destination, to: organisationStore.path(kindPath('destination')), keywords: ['warehouse', 'export'] },
        ...catalogStore.resourceKinds.map(kind => ({
            label: kindLabel(kind),
            icon: resourceKindIcon(kind),
            to: organisationStore.path(kindPath(kind)),
        })),
        { label: 'Jobs', icon: icons.job, to: organisationStore.path(kindPath('job')), keywords: ['cron', 'schedule'] },
        { label: 'Hooks', icon: icons.hook, to: organisationStore.path(kindPath('hook')), keywords: ['triggers', 'automation'] },
    ])
}

/**
 * Single source of truth for the app's main pages: the sidebar renders the
 * destinations with each hub's views as a submenu, each hub page renders those
 * views in its toolbar, and the command palette flattens both — so none of
 * them can drift apart.
 */
export function useNavDestinations() {
    const { icons } = useAppConfig()
    const organisationStore = useOrganisationStore()
    const componentViews = useComponentViews()
    const executionViews = useExecutionViews()

    return computed<NavDestination[]>(() => [
        { label: 'Overview', icon: icons.overview, to: organisationStore.path('/'), exact: true, keywords: ['home', 'dashboard', 'health'] },
        { label: 'Timeline', icon: icons.timeline, to: organisationStore.path('/timeline'), keywords: ['gantt', 'schedule', 'runs', 'history'] },
        { label: 'Graph', icon: icons.graph, to: organisationStore.path('/graph'), keywords: ['dag', 'pipeline', 'lineage'] },
        { label: 'Collection', icon: icons.collection, to: organisationStore.path('/collection'), keywords: ['catalog', 'library'] },
        { label: 'Components', icon: icons.components, to: organisationStore.path(kindPath('source')), views: componentViews.value },
        { label: 'Executions', icon: icons.executions, to: organisationStore.path('/executions/runs'), views: executionViews.value },
    ])
}

/** Whether a destination owns a route: itself, one of its views, or anything beneath either unless `exact`. */
export function isNavActive(destination: NavDestination, path: string): boolean {
    return [destination, ...(destination.views ?? [])].some(page => path === page.to || (!page.exact && path.startsWith(`${page.to}/`)))
}
