<script setup lang="ts">
import type { OrgMember } from '~/types/organisation'

const userStore = useUserStore()
const organisationStore = useOrganisationStore()
const toast = useToast()

const rows = ref<OrgMember[]>([])
const loading = ref(false)
const inviteOpen = ref(false)

const isAdmin = computed(() => userStore.user?.role === 'admin')
const orgId = computed(() => organisationStore.organisation?.id)

async function loadData() {
    if (!orgId.value) return
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
        await organisationStore.removeMember(orgId.value!, member.id)
        toast.add({ title: `${member.name || member.email} removed`, color: 'success' })
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to remove member'))
    }
}

async function cancelInvite(member: OrgMember) {
    try {
        await organisationStore.cancelInvitation(orgId.value!, member.id)
        toast.add({ title: `Invitation to ${member.email} cancelled`, color: 'success' })
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to cancel invitation'))
    }
}

async function resendInvite(member: OrgMember) {
    try {
        await organisationStore.resendInvitation(orgId.value!, member.id)
        toast.add({ title: `Invitation resent to ${member.email}`, color: 'success' })
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to resend invitation'))
    }
}

onMounted(loadData)
watch(() => organisationStore.organisation, loadData)

</script>

<template>
    <UDashboardPanel id="organization">
        <template #header>
            <AppNavbar title="Organization" />
        </template>
        <template #body>
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

            <OrganizationInviteModal v-if="orgId"
                                     v-model:open="inviteOpen"
                                     :org-id="orgId"
                                     @invited="loadData" />
        </template>
    </UDashboardPanel>
</template>
