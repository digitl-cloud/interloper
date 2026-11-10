/** Whether the signed-in user may act on the organisation's components: an editor or an admin. */
export function useCanEdit() {
    const organisationStore = useOrganisationStore()
    return computed(() => organisationStore.role === 'editor' || organisationStore.role === 'admin')
}
