<script setup lang="ts">
import type { OrgMember } from '~/types/organisation'

const organisationStore = useOrganisationStore()
const toast = useToast()
const views = useOrganizationViews()

const rows = ref<OrgMember[]>([])
const loading = ref(false)
const inviteOpen = ref(false)

const isAdmin = computed(() => organisationStore.role === 'admin')
const orgId = computed(() => organisationStore.requireOrganisation().id)

async function loadData() {
    loading.value = true
    try {
        rows.value = await organisationStore.fetchMemberRows(orgId.value, isAdmin.value)
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
        await organisationStore.removeMember(orgId.value, member.id)
        toast.add({ title: `${member.name || member.email} removed`, color: 'success' })
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to remove member'))
    }
}

async function cancelInvite(member: OrgMember) {
    try {
        await organisationStore.cancelInvitation(orgId.value, member.id)
        toast.add({ title: `Invitation to ${member.email} cancelled`, color: 'success' })
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to cancel invitation'))
    }
}

async function resendInvite(member: OrgMember) {
    try {
        await organisationStore.resendInvitation(orgId.value, member.id)
        toast.add({ title: `Invitation resent to ${member.email}`, color: 'success' })
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to resend invitation'))
    }
}

onMounted(loadData)
</script>

<template>
    <UDashboardPanel id="organization-members">
        <template #header>
            <AppNavbar title="Organization" />
            <UDashboardToolbar>
                <template #left>
                    <PageTabs :items="views" />
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <div class="mx-auto flex w-full max-w-5xl flex-col gap-4 sm:gap-6">
                <OrganizationMembersTable :members="rows"
                                          :loading="loading"
                                          :is-admin="isAdmin"
                                          @remove-member="removeMember"
                                          @cancel-invite="cancelInvite"
                                          @resend-invite="resendInvite">
                    <template v-if="isAdmin" #actions>
                        <UButton icon="i-lucide-user-plus"
                                 label="Invite"
                                 @click="inviteOpen = true" />
                    </template>
                </OrganizationMembersTable>

                <OrganizationInviteModal v-model:open="inviteOpen"
                                         :org-id="orgId"
                                         @invited="loadData" />
            </div>
        </template>
    </UDashboardPanel>
</template>
