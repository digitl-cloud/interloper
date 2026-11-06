import type {
    AdminActivityEntry,
    AdminConfig,
    AdminOAuthClient,
    AdminOrganisation,
    AdminQuotaLimits,
    AdminQuotas,
    AdminUser,
    CreatedOAuthClient,
} from '~/types/admin'

/** Cross-organisation management API, restricted to super-admins server-side. */
export const useAdminStore = defineStore('admin', () => {
    const { apiFetch, fetchAll } = useApi()

    function getConfig() {
        return apiFetch<AdminConfig>('/admin/config')
    }

    function getQuotas() {
        return apiFetch<AdminQuotas>('/admin/quotas')
    }

    /** Derived activity feed for one organisation, newest first. */
    function getOrgActivity(orgId: string) {
        return fetchAll<AdminActivityEntry>(`/admin/organisations/${orgId}/activity`)
    }

    /** Set an org's quota overrides; null clears a field (falls back to the default). */
    function updateOrgQuota(orgId: string, limits: Partial<AdminQuotaLimits>) {
        return apiFetch<AdminQuotaLimits>(`/admin/organisations/${orgId}/quota`, {
            method: 'PATCH',
            body: limits,
        })
    }

    function listUsers() {
        return fetchAll<AdminUser>('/admin/users')
    }

    function setSuperAdmin(userId: string, isSuperAdmin: boolean) {
        return apiFetch<AdminUser>(`/admin/users/${userId}`, {
            method: 'PATCH',
            body: { is_super_admin: isSuperAdmin },
        })
    }

    function deleteUser(userId: string) {
        return apiFetch(`/admin/users/${userId}`, { method: 'DELETE' })
    }

    function listOrganisations() {
        return fetchAll<AdminOrganisation>('/admin/organisations')
    }

    function createOrganisation(name: string) {
        return apiFetch<AdminOrganisation>('/admin/organisations', {
            method: 'POST',
            body: { name },
        })
    }

    /** The OAuth clients registered by an operator; self-registered clients are not listed. */
    function listOAuthClients() {
        return apiFetch<AdminOAuthClient[]>('/admin/oauth-clients')
    }

    function createOAuthClient(name: string, redirectUris: string[]) {
        return apiFetch<CreatedOAuthClient>('/admin/oauth-clients', {
            method: 'POST',
            body: { name, redirect_uris: redirectUris },
        })
    }

    /** Delete a client and end every grant it holds, for every user. */
    function deleteOAuthClient(clientId: string) {
        return apiFetch(`/admin/oauth-clients/${encodeURIComponent(clientId)}`, { method: 'DELETE' })
    }

    return {
        getConfig,
        getQuotas,
        getOrgActivity,
        updateOrgQuota,
        listUsers,
        setSuperAdmin,
        deleteUser,
        listOrganisations,
        createOrganisation,
        listOAuthClients,
        createOAuthClient,
        deleteOAuthClient,
    }
})
