<script setup lang="ts">
import type { OrgMember } from '~/types/organisation'

definePageMeta({ title: 'Organization' })

interface Invitation {
    id: string
    email: string
    role: string
    created_at: string | null
    expires_at: string
}

interface MemberResponse {
    id: string
    email: string
    name: string | null
    avatar_url: string | null
    role: string
}

const { apiFetch } = useApi()
const userStore = useUserStore()
const toast = useToast()

const rows = ref<OrgMember[]>([])
const loading = ref(false)
const inviteOpen = ref(false)

const isAdmin = computed(() => userStore.user?.role === 'admin')

async function loadData() {
    loading.value = true
    try {
        const [members, invitations] = await Promise.all([
            apiFetch<MemberResponse[]>('/organisations/members'),
            isAdmin.value ? apiFetch<Invitation[]>('/organisations/invitations') : Promise.resolve([]),
        ])

        const memberRows: OrgMember[] = members.map(m => ({
            id: m.id,
            email: m.email,
            name: m.name,
            avatar_url: m.avatar_url,
            role: m.role,
            status: 'active' as const,
        }))

        const inviteRows: OrgMember[] = invitations.map(i => ({
            id: i.id,
            email: i.email,
            name: null,
            avatar_url: null,
            role: i.role,
            status: 'invited' as const,
        }))

        rows.value = [...memberRows, ...inviteRows]
    }
    catch (err) {
        console.error('[Organization] Failed to load members', err)
    }
    finally {
        loading.value = false
    }
}

async function removeMember(member: OrgMember) {
    try {
        await apiFetch(`/organisations/members/${member.id}`, { method: 'DELETE' })
        toast.add({ title: `${member.name || member.email} removed`, color: 'success' })
        await loadData()
    }
    catch (err: any) {
        toast.add({ title: err?.data?.detail || 'Failed to remove member', color: 'error' })
    }
}

async function cancelInvite(member: OrgMember) {
    try {
        await apiFetch(`/organisations/invitations/${member.id}`, { method: 'DELETE' })
        toast.add({ title: `Invitation to ${member.email} cancelled`, color: 'success' })
        await loadData()
    }
    catch (err: any) {
        toast.add({ title: err?.data?.detail || 'Failed to cancel invitation', color: 'error' })
    }
}

async function resendInvite(member: OrgMember) {
    try {
        await apiFetch(`/organisations/invitations/${member.id}/resend`, { method: 'POST' })
        toast.add({ title: `Invitation resent to ${member.email}`, color: 'success' })
    }
    catch (err: any) {
        toast.add({ title: err?.data?.detail || 'Failed to resend invitation', color: 'error' })
    }
}

const organisationStore = useOrganisationStore()

onMounted(loadData)
watch(() => organisationStore.organisation, loadData)
</script>

<template>
    <div>
        <OrganizationMembersTable :members="rows"
                                  :loading="loading"
                                  :is-admin="isAdmin"
                                  @remove-member="removeMember"
                                  @cancel-invite="cancelInvite"
                                  @resend-invite="resendInvite">
            <template v-if="isAdmin" #toolbar>
                <UButton icon="i-lucide-user-plus"
                         label="Invite"
                         @click="inviteOpen = true" />
            </template>
        </OrganizationMembersTable>

        <OrganizationInviteModal v-model:open="inviteOpen"
                                 @invited="loadData" />
    </div>
</template>
