<script setup lang="ts">
import type { OrgMember } from '~/types/organisation'

definePageMeta({
    title: 'Organization',
    pageHeader: {
        eyebrow: 'Workspace',
        title: 'Members',
        description: 'Invite your team to your workspace and give everyone the right level of access. '
            + 'Roles control who can build pipelines, who can only read data, and who can manage the workspace.',
    },
})

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

        <div class="mt-11 mb-3.5">
            <div class="eyebrow text-primary">
                Access levels
            </div>
            <h2 class="text-[19px] font-bold tracking-[-0.015em] text-highlighted mt-2">
                What each role can do
            </h2>
        </div>
        <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <div v-for="role in ROLE_CARDS"
                 :key="role.name"
                 class="border border-default rounded-xl p-[18px] bg-default">
                <div class="flex items-center gap-2.5">
                    <div class="size-[34px] rounded-lg flex items-center justify-center"
                         :class="role.tile">
                        <UIcon :name="role.icon"
                               class="size-[18px]" />
                    </div>
                    <div class="text-[15px] font-bold text-highlighted">{{ role.name }}</div>
                </div>
                <p class="text-[13px] text-muted leading-normal mt-2.5">{{ role.desc }}</p>
            </div>
        </div>

        <OrganizationInviteModal v-model:open="inviteOpen"
                                 @invited="loadData" />
    </div>
</template>
