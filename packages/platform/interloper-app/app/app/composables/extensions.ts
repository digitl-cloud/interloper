import type { RouteComponent, RouteMeta, Router } from 'vue-router'

/** The organisation's route, which extension pages are added beneath so its middleware runs before them. */
export const ORGANISATION_ROUTE = 'organisation'

declare module 'vue-router' {
    interface RouteMeta {
        /** The key of the extension that added the page. */
        extension?: string
    }
}

/** A page an extension adds, under `/<organisation>/ext/<key>/`. */
export interface ExtensionRoute {
    /** Path below the extension's root: `''` for the root itself; params allowed (`buckets/:id`). */
    path: string
    component: RouteComponent | (() => Promise<RouteComponent>)
    /** Unique within the extension; the route is named `ext-<key>-<name>`. */
    name?: string
    meta?: RouteMeta
}

/** A page an extension's sidebar entry leads to, its path below the extension's root. */
export interface ExtensionNavPage {
    label: string
    icon: string
    path: string
    /** Aliases the command palette also matches on. */
    keywords?: string[]
    /** Owns its own route only, not the routes beneath it. */
    exact?: boolean
}

/** A sidebar and command-palette entry, in the Extensions section; its `views` light it in the sidebar and stand in its place in the palette. */
export interface ExtensionNavDestination extends ExtensionNavPage {
    views?: ExtensionNavPage[]
}

/** What an extension's `setup` receives: its key and the ways it plugs into the app. */
export interface ExtensionHost {
    readonly key: string
    addRoute: (route: ExtensionRoute) => void
    addNavDestination: (destination: ExtensionNavDestination) => void
}

/** An extension's UI: the default export of its entry module. */
export interface Extension {
    setup: (host: ExtensionHost) => void | Promise<void>
}

/** Declare an extension's UI; it is returned as is, typed. */
export function defineExtension(extension: Extension): Extension {
    return extension
}

/** Where an extension's page lives, from the organisation's root: `/ext/<key>/<path>`. */
function extensionPath(key: string, path: string): string {
    return path ? `/ext/${key}/${path}` : `/ext/${key}`
}

const destinations = shallowRef<{ key: string, destination: ExtensionNavDestination }[]>([])

/** The host an extension's `setup` gets: its pages go under the organisation's route, its entries into the sidebar. */
export function createExtensionHost(key: string, router: Router): ExtensionHost {
    return {
        key,
        addRoute(route) {
            router.addRoute(ORGANISATION_ROUTE, {
                path: extensionPath(key, route.path).slice(1),
                name: route.name === undefined ? undefined : `ext-${key}-${route.name}`,
                component: route.component,
                meta: { ...route.meta, extension: key },
            })
        },
        addNavDestination(destination) {
            destinations.value = [...destinations.value, { key, destination }]
        },
    }
}

/**
 * The extension whose page is rendering, so the page never repeats its own key:
 * `apiFetch('/buckets')` reaches its route `/api/organisations/<id>/ext/<key>/buckets`
 * as `useOrgApi` would, and `path('buckets/<id>')` is its page `/<organisation>/ext/<key>/buckets/<id>`,
 * the path written as `addRoute` takes it.
 */
export function useExtension() {
    const key = useRoute().meta.extension
    if (!key) throw new Error('useExtension() is called outside the pages an extension added')
    const { apiFetch: orgFetch, fetchAll: orgFetchAll } = useOrgApi()
    const organisationStore = useOrganisationStore()

    return {
        key,
        apiFetch: <T>(path: string, options?: Parameters<typeof orgFetch>[1]) => orgFetch<T>(`/ext/${key}${path}`, options),
        fetchAll: <T>(path: string, params?: Parameters<typeof orgFetchAll>[1]) => orgFetchAll<T>(`/ext/${key}${path}`, params),
        path: (path = '') => organisationStore.path(extensionPath(key, path)),
    }
}

/** The sidebar destinations the extensions added, addressed in the active organisation. */
export function useExtensionDestinations() {
    const organisationStore = useOrganisationStore()

    return computed<NavDestination[]>(() => destinations.value.map(({ key, destination }) => {
        const toPage = (page: ExtensionNavPage): NavPage => ({
            label: page.label,
            icon: page.icon,
            to: organisationStore.path(extensionPath(key, page.path)),
            keywords: page.keywords,
            exact: page.exact,
        })
        return { ...toPage(destination), views: destination.views?.map(toPage) }
    }))
}
