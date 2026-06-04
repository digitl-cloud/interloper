export default defineNuxtRouteMiddleware(async (to) => {
    if (to.path.startsWith('/auth/'))
        return

    const userStore = useUserStore()
    const organisationStore = useOrganisationStore()
    const catalogStore = useCatalogStore()

    if (!userStore.authenticated) {
        await userStore.fetchMe()
    }

    // Redirect to home if already authenticated and visiting /login
    if (to.path === '/login') {
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
        if (to.path.startsWith('/invite/') || to.path === '/welcome')
            return
        if (to.path.startsWith('/admin') && userStore.isSuperAdmin)
            return
        return navigateTo('/welcome')
    }

    // Load catalog once after auth + org are ready
    if (!catalogStore.loaded) {
        await catalogStore.fetchCatalog()
    }
})
