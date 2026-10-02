<script setup lang="ts">
import type { OrgMember } from '~/types/organisation'

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
    catch (err) {
        toast.add(errorToast(err, 'Failed to remove member'))
    }
}

async function cancelInvite(member: OrgMember) {
    try {
        await apiFetch(`/organisations/invitations/${member.id}`, { method: 'DELETE' })
        toast.add({ title: `Invitation to ${member.email} cancelled`, color: 'success' })
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to cancel invitation'))
    }
}

async function resendInvite(member: OrgMember) {
    try {
        await apiFetch(`/organisations/invitations/${member.id}/resend`, { method: 'POST' })
        toast.add({ title: `Invitation resent to ${member.email}`, color: 'success' })
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to resend invitation'))
    }
}

const organisationStore = useOrganisationStore()

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

            <OrganizationInviteModal v-model:open="inviteOpen"
                                     @invited="loadData" />
        </template>
    </UDashboardPanel>
</template>
