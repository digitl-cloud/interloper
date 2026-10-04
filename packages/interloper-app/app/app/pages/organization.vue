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

/** Access-level explainer cards — matches the app's real role vocabulary. */
const ROLE_CARDS = [
    {
        name: 'Admin',
        icon: 'i-lucide-shield-check',
        tile: ROLE_TINTS.admin!,
        desc: 'Full control — manage members, invitations, connections and every pipeline.',
    },
    {
        name: 'Editor',
        icon: 'i-lucide-wrench',
        tile: ROLE_TINTS.editor!,
        desc: 'Build and run the data layer: create sources, destinations, connections and jobs, and trigger runs.',
    },
    {
        name: 'Viewer',
        icon: 'i-lucide-eye',
        tile: ROLE_TINTS.viewer!,
        desc: 'Read-only access to the catalog, graph and run history. Cannot change anything.',
    },
]
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

            <UCard title="Access levels"
                   description="What each role can do in this workspace.">
                <div class="grid grid-cols-1 gap-3 sm:grid-cols-2">
                    <div v-for="role in ROLE_CARDS"
                         :key="role.name"
                         class="rounded-lg bg-default p-4 ring ring-default">
                        <div class="flex items-center gap-2.5">
                            <div class="flex size-8 items-center justify-center rounded-lg"
                                 :class="role.tile">
                                <UIcon :name="role.icon"
                                       class="size-4" />
                            </div>
                            <div class="text-base font-semibold text-highlighted">{{ role.name }}</div>
                        </div>
                        <p class="mt-2.5 text-sm leading-normal text-muted">{{ role.desc }}</p>
                    </div>
                </div>
            </UCard>

            <OrganizationInviteModal v-if="orgId"
                                     v-model:open="inviteOpen"
                                     :org-id="orgId"
                                     @invited="loadData" />
        </template>
    </UDashboardPanel>
</template>
