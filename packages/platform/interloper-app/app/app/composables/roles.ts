/** Whether the signed-in user may run, retry, cancel and backfill: an editor or an admin. */
export function useCanRun() {
    const organisationStore = useOrganisationStore()
    return computed(() => organisationStore.role === 'editor' || organisationStore.role === 'admin')
}

/**
 * Whether the signed-in user may change the organisation's components: one
 * who may run, while the collection is not served from files (`interloper dev
 * -f`), which only those files change.
 */
export function useCanEdit() {
    const canRun = useCanRun()
    const componentsStore = useComponentsStore()
    void componentsStore.fetchServedFrom()
    return computed(() => canRun.value && componentsStore.servedFrom === null)
}
