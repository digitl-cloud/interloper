export default defineNuxtRouteMiddleware(async (to) => {
    // Proxies may add a trailing slash (nginx directory redirects); canonicalize
    // so /login/ is /login everywhere, keeping the query intact.
    const path = to.path.length > 1 ? to.path.replace(/\/+$/, '') : to.path
    if (path !== to.path)
        return navigateTo({ path, query: to.query, hash: to.hash }, { replace: true })

    if (path.startsWith('/auth/'))
        return

    const userStore = useUserStore()

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
})
