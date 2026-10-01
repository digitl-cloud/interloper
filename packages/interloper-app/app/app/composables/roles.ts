/** Whether the signed-in user may act on the organisation's components: an editor or an admin. */
export function useCanEdit() {
    const userStore = useUserStore()
    return computed(() => userStore.user?.role === 'editor' || userStore.user?.role === 'admin')
}
