<script setup lang="ts">
import type { OrgMember } from '~/types/organisation'

const organisationStore = useOrganisationStore()
const toast = useToast()
const appConfig = useAppConfig()
const views = useOrganizationViews()

const rows = ref<OrgMember[]>([])
const loading = ref(false)
const loadError = ref<unknown>(null)
const inviteOpen = ref(false)

const isAdmin = computed(() => organisationStore.role === 'admin')
const orgId = computed(() => organisationStore.requireOrganisation().id)

async function loadData() {
    loading.value = true
    loadError.value = null
    try {
        rows.value = await organisationStore.fetchMemberRows(orgId.value, isAdmin.value)
    }
    catch (err) {
        loadError.value = err
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
        toast.add(errorToast(err, "Couldn't remove the member"))
    }
}

async function cancelInvite(member: OrgMember) {
    try {
        await organisationStore.cancelInvitation(orgId.value, member.id)
        toast.add({ title: `Invitation to ${member.email} cancelled`, color: 'success' })
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, "Couldn't cancel the invitation"))
    }
}

async function resendInvite(member: OrgMember) {
    try {
        await organisationStore.resendInvitation(orgId.value, member.id)
        toast.add({ title: `Invitation resent to ${member.email}`, color: 'success' })
    }
    catch (err) {
        toast.add(errorToast(err, "Couldn't resend the invitation"))
    }
}

onMounted(loadData)
</script>

<template>
    <UDashboardPanel id="organization-members">
        <template #header>
            <AppNavbar title="Organisation" />
            <UDashboardToolbar>
                <template #left>
                    <PageTabs :items="views" />
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <LoadError v-if="loadError"
                       title="Couldn't load the members"
                       :error="loadError"
                       @retry="loadData" />
            <OrganizationMembersTable :members="rows"
                                      :loading="loading"
                                      :is-admin="isAdmin"
                                      @remove-member="removeMember"
                                      @cancel-invite="cancelInvite"
                                      @resend-invite="resendInvite">
                <template v-if="isAdmin" #actions>
                    <UButton :icon="appConfig.icons.invite"
                             label="Invite"
                             @click="inviteOpen = true" />
                </template>
            </OrganizationMembersTable>

            <OrganizationInviteModal v-model:open="inviteOpen"
                                     :org-id="orgId"
                                     @invited="loadData" />
        </template>
    </UDashboardPanel>
</template>
