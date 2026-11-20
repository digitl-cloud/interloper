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

/** A view in a page's tab group: a route, or a `?view=` query with its own `active` flag. */
export interface PageTab {
    label: string
    to: RouteLocationRaw
    icon?: string
    active?: boolean
    badge?: string | number
}

/**
 * A sidebar destination. Its `views` are the other addresses it answers for:
 * they light it in the sidebar and stand in its place in the command palette
 * (an extension's sub-pages, a page's views).
 */
export interface NavDestination extends NavPage {
    views?: NavPage[]
}

/** A captioned group of sidebar destinations; the first, without a caption, holds the landing page. */
export interface NavSection {
    label?: string
    destinations: NavDestination[]
}

/** One way a page shows its records, chosen by `?view=`; a page's first view is its default. */
export interface PageView {
    value: string
    label: string
    icon: string
    /** Aliases the command palette matches this view on. */
    keywords?: string[]
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

/** List page of a kind, within the organisation ("connection" → "/components/connections"). */
export function kindPath(kind: string): string {
    return `/components/${kindSlug(kind)}`
}

/** The Assets page's views: its dependency graph, and its table. */
export function useAssetViews(): PageView[] {
    const { icons } = useAppConfig()
    return [
        { value: 'graph', label: 'Graph', icon: icons.graph },
        { value: 'table', label: 'Table', icon: icons.table, keywords: ['collection', 'catalog', 'library'] },
    ]
}

/** The Runs page's views: its table, and its runs on a wall-clock timeline. */
export function useRunViews(): PageView[] {
    const { icons } = useAppConfig()
    return [
        { value: 'table', label: 'Table', icon: icons.table },
        { value: 'timeline', label: 'Timeline', icon: icons.timeline, keywords: ['gantt', 'schedule'] },
    ]
}

/**
 * The view a page shows, from `?view=` (its first view when absent), and its
 * views as the tabs of its toolbar.
 *
 * @param views The page's views, its default first.
 * @returns The view shown, and the tabs that switch it.
 */
export function usePageView(views: PageView[]) {
    const values = views.map(view => view.value)
    const fallback = values[0]!
    const view = useQueryParam('view', fallback, oneOfQuery(values))
    const tabs = computed<PageTab[]>(() => views.map(option => ({
        label: option.label,
        to: { query: { view: option.value === fallback ? undefined : option.value } },
        active: view.value === option.value,
    })))
    return { view, tabs }
}

/** A page with views as a destination: its default view is the page itself, each other one opens as "<Page> <view>". */
function withViews(page: NavPage, views: PageView[]): NavDestination {
    return {
        ...page,
        views: views.map((view, index) => index === 0
            ? page
            : { label: `${page.label} ${view.label.toLowerCase()}`, icon: view.icon, to: `${page.to}?view=${view.value}`, keywords: view.keywords }),
    }
}

/** The organisation's own pages, in its menu at the sidebar's foot; its settings only for its admins. */
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

/**
 * Single source of truth for the app's main pages: the sidebar renders the
 * sections and the command palette lists their destinations, so neither can
 * drift from the other. The extensions' destinations get a section of their own.
 */
export function useNavSections() {
    const { icons } = useAppConfig()
    const catalogStore = useCatalogStore()
    const organisationStore = useOrganisationStore()
    const extensionDestinations = useExtensionDestinations()
    const assetViews = useAssetViews()
    const runViews = useRunViews()

    return computed<NavSection[]>(() => [
        {
            destinations: [
                { label: 'Overview', icon: icons.overview, to: organisationStore.path('/'), exact: true, keywords: ['home', 'dashboard', 'health'] },
            ],
        },
        {
            label: 'Data',
            destinations: [
                withViews({ label: 'Assets', icon: icons.asset, to: organisationStore.path('/assets'), keywords: ['graph', 'dag', 'pipeline', 'lineage'] }, assetViews),
                { label: 'Sources', icon: icons.source, to: organisationStore.path(kindPath('source')), keywords: ['components', 'connectors', 'integrations'] },
                { label: 'Destinations', icon: icons.destination, to: organisationStore.path(kindPath('destination')), keywords: ['warehouse', 'export'] },
                ...catalogStore.resourceKinds.map(kind => ({
                    label: kindLabel(kind),
                    icon: resourceKindIcon(kind),
                    to: organisationStore.path(kindPath(kind)),
                })),
            ],
        },
        {
            label: 'Automation',
            destinations: [
                { label: 'Jobs', icon: icons.job, to: organisationStore.path(kindPath('job')), keywords: ['cron', 'schedule'] },
                { label: 'Hooks', icon: icons.hook, to: organisationStore.path(kindPath('hook')), keywords: ['triggers', 'automation'] },
            ],
        },
        {
            label: 'Activity',
            destinations: [
                withViews({ label: 'Runs', icon: icons.run, to: organisationStore.path('/executions/runs'), keywords: ['executions', 'history'] }, runViews),
                { label: 'Backfills', icon: icons.backfill, to: organisationStore.path('/executions/backfills'), keywords: ['executions'] },
            ],
        },
        ...(extensionDestinations.value.length ? [{ label: 'Extensions', destinations: extensionDestinations.value }] : []),
    ])
}

/** Whether a destination owns a route: itself, one of its views, or anything beneath either unless `exact`. */
export function isNavActive(destination: NavDestination, path: string): boolean {
    return [destination, ...(destination.views ?? [])].some(page => path === page.to || (!page.exact && path.startsWith(`${page.to}/`)))
}
