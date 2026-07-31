export default defineNuxtRouteMiddleware(async (to) => {
    // Proxies may add a trailing slash (nginx directory redirects); canonicalize
    // so /login/ is /login everywhere, keeping the query intact.
    const path = to.path.length > 1 ? to.path.replace(/\/+$/, '') : to.path
    if (path !== to.path)
        return navigateTo({ path, query: to.query, hash: to.hash }, { replace: true })

    if (path.startsWith('/auth/'))
        return

    const userStore = useUserStore()
    const organisationStore = useOrganisationStore()
    const catalogStore = useCatalogStore()

    if (!userStore.authenticated) {
        await userStore.fetchMe()
    }

    // Redirect to home if already authenticated and visiting /login
    if (path === '/login') {
        if (userStore.authenticated)
            return navigateTo('/')
        return
    }

    if (!userStore.authenticated) {
        const redirect = to.fullPath === '/' ? undefined : to.fullPath
        return navigateTo(redirect ? `/login?redirect=${encodeURIComponent(redirect)}` : '/login')
    }

    // Ensure organisation is resolved after authentication
    if (!organisationStore.organisation) {
        await organisationStore.loadOrganisation()
    }

    // Authenticated but no organisation:
    // - /invite/* and /welcome: allow through
    // - /admin/*: allow super-admins through (they may belong to no org)
    // - All other routes: redirect to /welcome so user can create one
    if (!organisationStore.organisation) {
        if (path.startsWith('/invite/') || path === '/welcome')
            return
        if (path.startsWith('/admin') && userStore.isSuperAdmin)
            return
        return navigateTo('/welcome')
    }

    // Load catalog once after auth + org are ready
    if (!catalogStore.loaded) {
        await catalogStore.fetchCatalog()
    }
})
