import type { AdminConfig, AdminOrganisation, AdminQuotaLimits, AdminQuotas, AdminUser } from '~/types/admin'

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

    function getConfig() {
        return apiFetch<AdminConfig>('/admin/config')
    }

    function getQuotas() {
        return apiFetch<AdminQuotas>('/admin/quotas')
    }

    /** Set an org's quota overrides; null clears a field (falls back to the default). */
    function updateOrgQuota(orgId: string, limits: AdminQuotaLimits) {
        return apiFetch<AdminQuotaLimits>(`/admin/organisations/${orgId}/quota`, {
            method: 'PATCH',
            body: limits,
        })
    }

    function listUsers() {
        return apiFetch<AdminUser[]>('/admin/users')
    }

    function deleteUser(userId: string) {
        return apiFetch(`/admin/users/${userId}`, { method: 'DELETE' })
    }

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

    /** Deletes the organisation and all its data; `name` must repeat the exact name. */
    function deleteOrganisation(orgId: string, name: string) {
        return apiFetch(`/admin/organisations/${orgId}`, {
            method: 'DELETE',
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
        getConfig,
        getQuotas,
        updateOrgQuota,
        listUsers,
        deleteUser,
        listOrganisations,
        createOrganisation,
        renameOrganisation,
        deleteOrganisation,
        listMembers,
        joinOrganisation,
        updateMemberRole,
        removeMember,
        listInvitations,
        inviteMember,
        cancelInvitation,
    }
})
