import type { Invitation, Member, Organisation, OrgMember } from '~/types/organisation'

export const useOrganisationStore = defineStore('organisation', () => {
    const { apiFetch, fetchAll } = useApi()
    const userStore = useUserStore()

    /**********************
     * State
     **********************/
    const loading = ref(false)
    const error = ref<Error | null>(null)
    const organisation = ref<Organisation | null>(null)

    /**********************
     * Cross-tab sync
     **********************/
    // The active org lives in the shared session cookie, so a switch in one
    // tab silently changes what every other tab's API calls return. Broadcast
    // switches so other tabs re-sync their state; org-scoped stores and
    // OrganizationGate react from there.
    const channel = typeof BroadcastChannel !== 'undefined' ? new BroadcastChannel('interloper-org') : null
    channel?.addEventListener('message', async (event: MessageEvent) => {
        const orgId: unknown = event.data?.orgId
        if (typeof orgId === 'string' && orgId !== organisation.value?.id) {
            await userStore.fetchMe()
            organisation.value = userStore.user?.organisation ?? null
        }
    })

    /**********************
     * Actions
     **********************/
    async function fetchOrganisations(): Promise<Organisation[]> {
        return fetchAll<Organisation>('/organisations')
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
        channel?.postMessage({ orgId: created.id })
        return created
    }

    async function switchOrg(orgId: string) {
        await apiFetch('/auth/switch-org', {
            method: 'POST',
            body: { organisation_id: orgId },
        })
        await userStore.fetchMe()
        organisation.value = userStore.user?.organisation ?? null
        channel?.postMessage({ orgId })
    }

    async function renameOrganisation(orgId: string, name: string): Promise<Organisation> {
        const renamed = await apiFetch<Organisation>(`/organisations/${orgId}`, {
            method: 'PATCH',
            body: { name },
        })
        if (organisation.value?.id === orgId) organisation.value = renamed
        return renamed
    }

    /** Deletes the organisation and all its data; `name` must repeat the exact name. */
    async function deleteOrganisation(orgId: string, name: string) {
        await apiFetch(`/organisations/${orgId}`, {
            method: 'DELETE',
            body: { name },
        })
    }

    /** An organisation's members, then its pending invitations when `withInvitations`, as table rows. */
    async function fetchMemberRows(orgId: string, withInvitations: boolean): Promise<OrgMember[]> {
        const [members, invitations] = await Promise.all([
            fetchAll<Member>(`/organisations/${orgId}/members`),
            withInvitations ? fetchAll<Invitation>(`/organisations/${orgId}/invitations`) : [],
        ])
        return [
            ...members.map(m => ({ ...m, status: 'active' as const })),
            ...invitations.map(i => ({ id: i.id, email: i.email, name: null, avatar_url: null, role: i.role, status: 'invited' as const })),
        ]
    }

    /** Join an organisation directly (super-admins only). */
    async function joinOrganisation(orgId: string, role: string = 'admin'): Promise<Member> {
        return apiFetch<Member>(`/organisations/${orgId}/members`, {
            method: 'POST',
            body: { role },
        })
    }

    async function updateMemberRole(orgId: string, userId: string, role: string): Promise<Member> {
        return apiFetch<Member>(`/organisations/${orgId}/members/${userId}`, {
            method: 'PATCH',
            body: { role },
        })
    }

    async function removeMember(orgId: string, userId: string) {
        await apiFetch(`/organisations/${orgId}/members/${userId}`, { method: 'DELETE' })
    }

    async function inviteMember(orgId: string, email: string, role: string): Promise<Invitation> {
        return apiFetch<Invitation>(`/organisations/${orgId}/invitations`, {
            method: 'POST',
            body: { email, role },
        })
    }

    async function cancelInvitation(orgId: string, invitationId: string) {
        await apiFetch(`/organisations/${orgId}/invitations/${invitationId}`, { method: 'DELETE' })
    }

    async function resendInvitation(orgId: string, invitationId: string): Promise<Invitation> {
        return apiFetch<Invitation>(`/organisations/${orgId}/invitations/${invitationId}/resend`, { method: 'POST' })
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
        renameOrganisation,
        deleteOrganisation,
        fetchMemberRows,
        joinOrganisation,
        updateMemberRole,
        removeMember,
        inviteMember,
        cancelInvitation,
        resendInvitation,
        requireOrganisation,
    }
})
