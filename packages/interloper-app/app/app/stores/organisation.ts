import type { Invitation, Member, Membership, Organisation, OrgMember } from '~/types/organisation'

export const useOrganisationStore = defineStore('organisation', () => {
    const { apiFetch, fetchAll } = useApi()
    const userStore = useUserStore()
    const adminStore = useAdminStore()

    /**********************
     * State
     **********************/
    /**
     * Organisations the user can act in beyond `/auth/me`'s memberships: one a super-admin opened
     * without belonging to it, or one updated here since `/auth/me` was read.
     */
    const discovered = ref<Membership[]>([])
    /** The slug of the last organisation route: pages outside any organisation (settings, admin) keep it. */
    const slug = ref<string | null>(null)
    const router = useRouter()
    watch(() => router.currentRoute.value.params.org, (value) => {
        if (typeof value === 'string') slug.value = value
    }, { immediate: true, flush: 'sync' })

    /** Aborts every call `useOrgApi` made under the active organisation, once the route leaves it. */
    let calls = new AbortController()

    /**********************
     * Getters
     **********************/
    /** The organisation the route names, `null` until one it names is known. */
    const organisation = computed(() => slug.value ? _find(slug.value) ?? null : null)
    /** The role the user acts with in the organisation. */
    const role = computed(() => organisation.value?.role ?? null)

    // A synchronous flush so a response for the organisation the route just left cannot land first.
    watch(() => organisation.value?.id, (orgId) => {
        calls.abort()
        calls = new AbortController()
        if (!orgId || !userStore.user || userStore.user.last_organisation_id === orgId) return
        userStore.user.last_organisation_id = orgId
        apiFetch('/auth/me/last-organisation', { method: 'PUT', body: { org_id: orgId } }).catch(() => {})
    }, { immediate: true, flush: 'sync' })

    /**********************
     * Internals
     **********************/
    function _find(slug: string): Membership | undefined {
        return discovered.value.find(m => m.slug === slug) ?? userStore.user?.memberships.find(m => m.slug === slug)
    }

    /**
     * A slug the memberships do not know: one joined or renamed since `/auth/me` was read,
     * or, for a super-admin, any live organisation in the admin listing.
     */
    async function _lookUp(slug: string): Promise<Membership | undefined> {
        await userStore.fetchMe()
        const member = _find(slug)
        if (member || !userStore.isSuperAdmin) return member
        const listed = (await adminStore.listOrganisations()).find(o => o.slug === slug && !o.deleted_at)
        if (!listed) return undefined
        const opened: Membership = { id: listed.id, name: listed.name, slug: listed.slug, role: 'admin' }
        discovered.value.push(opened)
        return opened
    }

    /**********************
     * Actions
     **********************/
    /**
     * The organisation a slug names, looked up when the memberships do not know it; `null` when the
     * user cannot act in it. The route that names it makes it the active one.
     */
    async function resolve(slug: string): Promise<Membership | null> {
        return _find(slug) ?? await _lookUp(slug) ?? null
    }

    /** The signal `useOrgApi` gives each call: aborted once the route leaves the active organisation. */
    function signal(): AbortSignal {
        return calls.signal
    }

    /** Where `/` sends the user: the last organisation they used, else their first, else `/welcome`. */
    function home(): string {
        const memberships = userStore.user?.memberships ?? []
        const last = memberships.find(m => m.id === userStore.user?.last_organisation_id) ?? memberships[0]
        return last ? `/${last.slug}` : '/welcome'
    }

    /** A path inside the active organisation: `path('/graph')` is `/<slug>/graph`, `path('/')` its landing page. */
    function path(subpath: string): string {
        const base = `/${requireOrganisation().slug}`
        return subpath === '/' ? base : `${base}${subpath}`
    }

    async function createOrganisation(name: string): Promise<Organisation> {
        const created = await apiFetch<Organisation>('/organisations', {
            method: 'POST',
            body: { name },
        })
        await userStore.fetchMe()
        return created
    }

    /**
     * Rename an organisation or change its slug. The update is known under its new slug at once,
     * while its old one still resolves from the memberships until `/auth/me` is read again, so the
     * route can move from one to the other without passing through an unknown organisation.
     */
    async function updateOrganisation(orgId: string, patch: { name?: string, slug?: string }): Promise<Organisation> {
        const updated = await apiFetch<Organisation>(`/organisations/${orgId}`, {
            method: 'PATCH',
            body: patch,
        })
        const role = discovered.value.find(m => m.id === orgId)?.role
            ?? userStore.user?.memberships.find(m => m.id === orgId)?.role
        if (role) {
            discovered.value = [
                ...discovered.value.filter(m => m.id !== orgId),
                { id: updated.id, name: updated.name, slug: updated.slug, role },
            ]
        }
        return updated
    }

    /** Deletes the organisation and all its data; `name` must repeat the exact name. */
    async function deleteOrganisation(orgId: string, name: string) {
        await apiFetch(`/organisations/${orgId}`, {
            method: 'DELETE',
            body: { name },
        })
        await userStore.fetchMe()
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
        const member = await apiFetch<Member>(`/organisations/${orgId}/members`, {
            method: 'POST',
            body: { role },
        })
        await userStore.fetchMe()
        return member
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

    function requireOrganisation(): Membership {
        if (!organisation.value)
            throw new Error('Organisation not loaded')
        return organisation.value
    }

    return {
        organisation,
        role,
        resolve,
        signal,
        home,
        path,
        createOrganisation,
        updateOrganisation,
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
