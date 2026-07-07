import type { AdminOrganisation } from '~/types/admin'

interface MemberResponse {
    id: string
    email: string
    name: string | null
    avatar_url: string | null
    role: string
}

interface InvitationResponse {
    id: string
    email: string
    role: string
    created_at: string | null
    expires_at: string
}

/** Cross-organisation management API, restricted to super-admins server-side. */
export const useAdminStore = defineStore('admin', () => {
    const { apiFetch } = useApi()

    function listOrganisations() {
        return apiFetch<AdminOrganisation[]>('/admin/organisations')
    }

    function createOrganisation(name: string) {
        return apiFetch<AdminOrganisation>('/admin/organisations', {
            method: 'POST',
            body: { name },
        })
    }

    function renameOrganisation(orgId: string, name: string) {
        return apiFetch<AdminOrganisation>(`/admin/organisations/${orgId}`, {
            method: 'PATCH',
            body: { name },
        })
    }

    function listMembers(orgId: string) {
        return apiFetch<MemberResponse[]>(`/admin/organisations/${orgId}/members`)
    }

    function joinOrganisation(orgId: string, role: string = 'admin') {
        return apiFetch<MemberResponse>(`/admin/organisations/${orgId}/members`, {
            method: 'POST',
            body: { role },
        })
    }

    function updateMemberRole(orgId: string, userId: string, role: string) {
        return apiFetch(`/admin/organisations/${orgId}/members/${userId}`, {
            method: 'PATCH',
            body: { role },
        })
    }

    function removeMember(orgId: string, userId: string) {
        return apiFetch(`/admin/organisations/${orgId}/members/${userId}`, {
            method: 'DELETE',
        })
    }

    function listInvitations(orgId: string) {
        return apiFetch<InvitationResponse[]>(`/admin/organisations/${orgId}/invitations`)
    }

    function inviteMember(orgId: string, email: string, role: string) {
        return apiFetch<InvitationResponse>(`/admin/organisations/${orgId}/invitations`, {
            method: 'POST',
            body: { email, role },
        })
    }

    function cancelInvitation(orgId: string, invitationId: string) {
        return apiFetch(`/admin/organisations/${orgId}/invitations/${invitationId}`, {
            method: 'DELETE',
        })
    }

    return {
        listOrganisations,
        createOrganisation,
        renameOrganisation,
        listMembers,
        joinOrganisation,
        updateMemberRole,
        removeMember,
        listInvitations,
        inviteMember,
        cancelInvitation,
    }
})
