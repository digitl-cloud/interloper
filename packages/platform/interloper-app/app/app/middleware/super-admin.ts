export default defineNuxtRouteMiddleware(async () => {
    const userStore = useUserStore()

    if (!userStore.authenticated) {
        await userStore.fetchMe()
    }

    if (!userStore.isSuperAdmin) {
        return navigateTo('/')
    }
})
