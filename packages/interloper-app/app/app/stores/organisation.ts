import type { Organisation } from '~/types/organisation'

export const useOrganisationStore = defineStore('organisation', () => {
    const { apiFetch } = useApi()
    const userStore = useUserStore()

    /**********************
     * State
     **********************/
    const loading = ref(false)
    const error = ref<Error | null>(null)
    const organisation = ref<Organisation | null>(null)

    /**********************
     * Actions
     **********************/
    async function fetchOrganisations(): Promise<Organisation[]> {
        return apiFetch<Organisation[]>('/organisations')
    }

    async function loadOrganisation() {
        // If session already has an org (from /auth/me), use it
        const sessionOrg = userStore.user?.organisation
        if (sessionOrg) {
            organisation.value = sessionOrg
            return
        }

        // Session has no org — fetch user's memberships and pick the best one
        const orgs = await fetchOrganisations()
        if (orgs.length === 0) return

        // Prefer the user's last-used org if still a member, otherwise first
        const lastOrgId = userStore.user?.last_organisation_id
        const preferred = lastOrgId ? orgs.find(o => o.id === lastOrgId) : null
        await switchOrg((preferred ?? orgs[0]!).id)
    }

    async function createOrganisation(name: string) {
        const created = await apiFetch<Organisation>('/organisations', {
            method: 'POST',
            body: { name },
        })
        await userStore.fetchMe()
        organisation.value = created
        return created
    }

    async function switchOrg(orgId: string) {
        await apiFetch('/auth/switch-org', {
            method: 'POST',
            body: { organisation_id: orgId },
        })
        await userStore.fetchMe()
        organisation.value = userStore.user?.organisation ?? null
    }

    function findOrganisation(): Organisation | null {
        return organisation.value
    }

    function requireOrganisation(): Organisation {
        if (!organisation.value)
            throw new Error('Organisation not loaded')
        return organisation.value
    }

    return {
        organisation,
        loading,
        error,
        findOrganisation,
        fetchOrganisations,
        loadOrganisation,
        createOrganisation,
        switchOrg,
        requireOrganisation,
    }
})
