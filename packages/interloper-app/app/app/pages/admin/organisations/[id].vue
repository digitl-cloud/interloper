<script setup lang="ts">
import type { BreadcrumbItem } from '@nuxt/ui'
import type { OrgMember } from '~/types/organisation'

definePageMeta({ title: 'Manage organisation', middleware: 'super-admin' })

const route = useRoute()
const orgId = computed(() => route.params.id as string)

const adminStore = useAdminStore()
const userStore = useUserStore()
const toast = useToast()

const rows = ref<OrgMember[]>([])
const orgName = ref<string | null>(null)
const loading = ref(false)
const inviteOpen = ref(false)

const isMember = computed(() =>
    rows.value.some(r => r.status === 'active' && r.id === userStore.user?.id))

const inviteEndpoint = computed(() => `/admin/organisations/${orgId.value}/invitations`)

const breadcrumbs = computed<BreadcrumbItem[]>(() => [
    { label: 'Platform admin', icon: 'i-lucide-shield' },
    { label: 'Organisations', icon: 'i-lucide-building-2', to: '/admin' },
    { label: orgName.value ?? '…' },
])

async function loadData() {
    loading.value = true
    try {
        const [members, invitations, organisations] = await Promise.all([
            adminStore.listMembers(orgId.value),
            adminStore.listInvitations(orgId.value),
            adminStore.listOrganisations(),
        ])

        orgName.value = organisations.find(o => o.id === orgId.value)?.name ?? null

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
        console.error('[Admin] Failed to load members', err)
    }
    finally {
        loading.value = false
    }
}

async function removeMember(member: OrgMember) {
    try {
        await adminStore.removeMember(orgId.value, member.id)
        toast.add({ title: `${member.name || member.email} removed`, color: 'success' })
        await loadData()
    }
    catch (err: any) {
        toast.add({ title: err?.data?.detail || 'Failed to remove member', color: 'error' })
    }
}

async function cancelInvite(member: OrgMember) {
    try {
        await adminStore.cancelInvitation(orgId.value, member.id)
        toast.add({ title: `Invitation to ${member.email} cancelled`, color: 'success' })
        await loadData()
    }
    catch (err: any) {
        toast.add({ title: err?.data?.detail || 'Failed to cancel invitation', color: 'error' })
    }
}

async function joinOrganisation() {
    try {
        await adminStore.joinOrganisation(orgId.value)
        toast.add({ title: `Joined ${orgName.value ?? 'organisation'}`, color: 'success' })
        await loadData()
    }
    catch (err: any) {
        toast.add({ title: err?.data?.detail || 'Failed to join organisation', color: 'error' })
    }
}

async function resendInvite(member: OrgMember) {
    try {
        await adminStore.cancelInvitation(orgId.value, member.id)
        await adminStore.inviteMember(orgId.value, member.email, member.role)
        toast.add({ title: `Invitation resent to ${member.email}`, color: 'success' })
        await loadData()
    }
    catch (err: any) {
        toast.add({ title: err?.data?.detail || 'Failed to resend invitation', color: 'error' })
    }
}

onMounted(loadData)
watch(orgId, loadData)
</script>

<template>
    <div class="flex flex-col flex-1 min-h-0">
        <div class="px-4 pt-4 pb-2 shrink-0">
            <UBreadcrumb :items="breadcrumbs" />
        </div>
        <OrganizationMembersTable :members="rows"
                                  :loading="loading"
                                  is-admin
                                  @remove-member="removeMember"
                                  @cancel-invite="cancelInvite"
                                  @resend-invite="resendInvite">
            <template #toolbar>
                <UButton v-if="!loading && !isMember"
                         icon="i-lucide-log-in"
                         label="Join"
                         variant="outline"
                         @click="joinOrganisation" />
                <UButton icon="i-lucide-user-plus"
                         label="Invite"
                         @click="inviteOpen = true" />
            </template>
        </OrganizationMembersTable>

        <OrganizationInviteModal v-model:open="inviteOpen"
                                 :endpoint="inviteEndpoint"
                                 @invited="loadData" />
    </div>
</template>
