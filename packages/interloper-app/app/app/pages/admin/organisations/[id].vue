<script setup lang="ts">
import { h } from 'vue'
import type { NavigationMenuItem, TableColumn } from '@nuxt/ui'
import { UBadge } from '#components'
import type { AdminActivityEntry, AdminOrganisation, AdminOrgQuotaStatus, AdminQuotas } from '~/types/admin'
import type { Organisation, OrgMember } from '~/types/organisation'

definePageMeta({ layout: 'admin', middleware: 'super-admin' })

const route = useRoute()
const orgId = computed(() => route.params.id as string)

const adminStore = useAdminStore()
const userStore = useUserStore()
const toast = useToast()
const { switchToOrg } = useOrgSwitch()

const rows = ref<OrgMember[]>([])
const org = ref<AdminOrganisation | null>(null)
const quotas = ref<AdminQuotas | null>(null)
const activity = ref<AdminActivityEntry[]>([])
const loading = ref(false)
const inviteOpen = ref(false)

const isMember = computed(() =>
    rows.value.some(r => r.status === 'active' && r.id === userStore.user?.id))

const inviteEndpoint = computed(() => `/admin/organisations/${orgId.value}/invitations`)

const quotaRow = computed<AdminOrgQuotaStatus | null>(() =>
    quotas.value?.organisations.find(row => row.id === orgId.value) ?? null)

async function loadData() {
    loading.value = true
    try {
        const [members, invitations, organisations, quotasResp, activityResp] = await Promise.all([
            adminStore.listMembers(orgId.value),
            adminStore.listInvitations(orgId.value),
            adminStore.listOrganisations(),
            adminStore.getQuotas(),
            adminStore.getOrgActivity(orgId.value),
        ])

        org.value = organisations.find(o => o.id === orgId.value) ?? null
        quotas.value = quotasResp
        activity.value = activityResp

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
        console.error('[Admin] Failed to load organisation', err)
    }
    finally {
        loading.value = false
    }
}

// -- Tabs -----------------------------------------------------------------------

const TAB_VALUES = ['usage', 'members', 'activity', 'settings']
/** The `?tab=` view; a missing or unknown value opens the first tab. */
const tab = computed(() => {
    const value = route.query.tab as string
    return TAB_VALUES.includes(value) ? value : 'usage'
})
const tabs = computed<NavigationMenuItem[]>(() => [
    { label: 'Usage & quotas', icon: 'i-lucide-gauge', to: { query: { tab: 'usage' } }, active: tab.value === 'usage' },
    {
        label: 'Members',
        icon: 'i-lucide-users',
        badge: loading.value ? undefined : rows.value.length,
        to: { query: { tab: 'members' } },
        active: tab.value === 'members',
    },
    { label: 'Activity', icon: 'i-lucide-activity', to: { query: { tab: 'activity' } }, active: tab.value === 'activity' },
    {
        label: 'Settings',
        icon: 'i-lucide-sliders-horizontal',
        to: { query: { tab: 'settings' } },
        active: tab.value === 'settings',
    },
])

// -- Members --------------------------------------------------------------------

async function removeMember(member: OrgMember) {
    try {
        await adminStore.removeMember(orgId.value, member.id)
        toast.add({ title: `${member.name || member.email} removed`, color: 'success' })
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to remove member'))
    }
}

async function cancelInvite(member: OrgMember) {
    try {
        await adminStore.cancelInvitation(orgId.value, member.id)
        toast.add({ title: `Invitation to ${member.email} cancelled`, color: 'success' })
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to cancel invitation'))
    }
}

async function joinOrganisation() {
    try {
        await adminStore.joinOrganisation(orgId.value)
        toast.add({ title: `Joined ${org.value?.name ?? 'organisation'}`, color: 'success' })
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to join organisation'))
    }
}

async function resendInvite(member: OrgMember) {
    try {
        await adminStore.cancelInvitation(orgId.value, member.id)
        await adminStore.inviteMember(orgId.value, member.email, member.role)
        toast.add({ title: `Invitation resent to ${member.email}`, color: 'success' })
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to resend invitation'))
    }
}

// -- Usage & quotas ---------------------------------------------------------------

const periodLabel = computed(() => {
    if (!quotas.value) return ''
    return new Date(quotas.value.period_start).toLocaleDateString(undefined, { month: 'long', year: 'numeric' })
})

const usageTiles = computed(() => {
    const row = quotaRow.value
    if (!row) return []
    const eff = row.effective
    return [
        {
            label: 'Sources',
            value: row.sources.toLocaleString(),
            sub: eff.max_sources != null ? `of ${eff.max_sources.toLocaleString()} allowed` : 'no limit set',
            used: row.sources,
            limit: eff.max_sources ?? null,
        },
        {
            label: 'Assets / source',
            value: row.max_assets_per_source.toLocaleString(),
            sub: eff.max_assets_per_source != null
                ? `largest source, of ${eff.max_assets_per_source.toLocaleString()}`
                : 'largest source',
            used: row.max_assets_per_source,
            limit: eff.max_assets_per_source ?? null,
        },
        {
            label: 'Successful runs',
            value: row.successful_runs.toLocaleString(),
            sub: eff.max_successful_runs_per_month != null
                ? `of ${eff.max_successful_runs_per_month.toLocaleString()} this period`
                : 'this period',
            used: row.successful_runs,
            limit: eff.max_successful_runs_per_month ?? null,
        },
        {
            label: 'Reserved runs',
            value: row.reserved_runs.toLocaleString(),
            sub: 'queued against the ledger',
            used: row.reserved_runs,
            limit: null,
        },
    ]
})

const limitRows = computed(() => {
    const row = quotaRow.value
    if (!row) return []
    return (quotas.value?.fields ?? []).map((field) => {
        const override = row.limits[field.key]
        const effective = row.effective[field.key]
        return {
            key: field.key,
            label: field.label,
            value: effective != null ? effective.toLocaleString() : 'Unlimited',
            overridden: override != null,
            note: override != null
                ? `Instance default is ${field.default != null ? field.default.toLocaleString() : 'unlimited'}`
                : 'Follows the instance default',
        }
    })
})

const limitColumns: TableColumn<(typeof limitRows.value)[number]>[] = [
    {
        accessorKey: 'label',
        header: 'Limit',
        meta: { class: { td: 'whitespace-nowrap font-medium text-highlighted' } },
    },
    {
        accessorKey: 'value',
        header: 'Value',
        meta: { class: { td: 'w-full' } },
        cell: ({ row }) => h('span', [
            h('span', { class: 'font-mono text-sm font-medium text-highlighted' }, row.original.value),
            h('span', { class: 'ml-2 text-xs text-dimmed' }, row.original.note),
        ]),
    },
    {
        accessorKey: 'overridden',
        header: 'Source',
        cell: ({ row }) => h(UBadge, {
            label: row.original.overridden ? 'Override' : 'Inherited',
            color: row.original.overridden ? 'info' : 'neutral',
            size: 'sm',
        }),
    },
]

/** Day-of-period progress for the usage strip (quota counters reset monthly). */
const periodElapsed = computed(() => {
    if (!quotas.value) return null
    const start = new Date(quotas.value.period_start)
    const end = new Date(start.getFullYear(), start.getMonth() + 1, 0)
    const day = Math.min(new Date().getDate(), end.getDate())
    return { label: `Day ${day} of ${end.getDate()}`, pct: Math.round((day / end.getDate()) * 100) }
})

const ledgerInSync = computed(() =>
    quotaRow.value != null && quotaRow.value.successful_runs === quotaRow.value.recomputed_successful_runs)

// Edit drawer — the AdminQuotaDrawer owns the form; we just open it.
const editOpen = ref(false)

async function reloadQuotas() {
    quotas.value = await adminStore.getQuotas()
}

// -- Activity ---------------------------------------------------------------------

const ACTIVITY_ICONS: Record<string, string> = {
    org_created: 'i-lucide-building-2',
    org_deleted: 'i-lucide-trash-2',
    member_joined: 'i-lucide-user-plus',
    invitation_sent: 'i-lucide-mail',
    source_added: 'i-lucide-plug',
    runs_completed: 'i-lucide-check-circle',
}

// -- Settings ----------------------------------------------------------------------

const renameValue = ref('')
watch(org, value => {
    renameValue.value = value?.name ?? ''
})
const renaming = ref(false)

async function submitRename() {
    const name = renameValue.value.trim()
    if (!name || name === org.value?.name) return
    renaming.value = true
    try {
        await adminStore.renameOrganisation(orgId.value, name)
        toast.add({ title: 'Organisation renamed', color: 'success' })
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to rename organisation'))
    }
    finally {
        renaming.value = false
    }
}

async function openWorkspace() {
    if (!org.value) return
    await switchToOrg({ id: org.value.id, name: org.value.name } as Organisation)
    await navigateTo('/')
}

const deleteConfirmName = ref('')
const deleting = ref(false)
const deleteOpen = ref(false)

async function submitDelete() {
    const target = org.value
    if (!target || deleteConfirmName.value !== target.name) return
    deleting.value = true
    try {
        await adminStore.deleteOrganisation(target.id, deleteConfirmName.value)
        toast.add({ title: `Organisation "${target.name}" deleted`, color: 'success' })
        await navigateTo('/admin/organisations')
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to delete organisation'))
    }
    finally {
        deleting.value = false
    }
}

onMounted(loadData)
watch(orgId, loadData)
</script>

<template>
    <UDashboardPanel id="admin-organisation">
        <template #header>
            <AppNavbar>
                <template #title>
                    <ULink to="/admin/organisations"
                           class="text-base font-medium text-muted hover:text-highlighted">Organisations</ULink>
                    <span class="text-base text-dimmed">/</span>
                    <span class="truncate text-base font-semibold">{{ org?.name ?? '…' }}</span>
                </template>
            </AppNavbar>
            <UDashboardToolbar>
                <template #left>
                    <UNavigationMenu :items="tabs"
                                     highlight
                                     class="-mx-1 flex-1" />
                </template>
                <template #right>
                    <UButton v-if="isMember"
                             icon="i-lucide-external-link"
                             label="Open workspace"
                             color="neutral"
                             variant="outline"
                             @click="openWorkspace" />
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <div class="mx-auto flex w-full max-w-5xl flex-col gap-4 sm:gap-6">
                <template v-if="tab === 'usage'">
                    <div class="grid grid-cols-2 gap-4 sm:gap-6 xl:grid-cols-4">
                        <UCard v-for="tile in usageTiles"
                               :key="tile.label"
                               :ui="{ body: 'flex flex-col gap-3' }">
                            <div class="text-sm text-muted">{{ tile.label }}</div>
                            <div class="flex min-w-0 items-baseline gap-2">
                                <span class="text-3xl font-semibold tabular-nums text-highlighted">{{ tile.value }}</span>
                                <span class="truncate text-sm text-muted">{{ tile.sub }}</span>
                            </div>
                            <AdminUsageMeter v-if="tile.limit != null"
                                             :used="tile.used"
                                             :limit="tile.limit"
                                             :show-label="false" />
                        </UCard>
                    </div>

                    <UCard v-if="periodElapsed">
                        <div class="flex items-baseline gap-2">
                            <span class="flex-1 truncate text-sm font-medium text-highlighted">Period elapsed · {{ periodLabel }}</span>
                            <span class="whitespace-nowrap text-xs text-muted">{{ periodElapsed.label }}</span>
                            <span class="whitespace-nowrap text-xs font-semibold text-primary">{{ periodElapsed.pct }}%</span>
                        </div>
                        <div class="mt-2 h-1.5 overflow-hidden rounded-full bg-accented">
                            <div class="h-full rounded-full bg-primary"
                                 :style="{ width: periodElapsed.pct + '%' }" />
                        </div>
                    </UCard>

                    <section v-if="quotaRow"
                             class="flex flex-col gap-3">
                        <CardHeader title="Ledger"
                                    :description="ledgerInSync
                                        ? 'The runs counter and a recount from the runs table agree.'
                                        : 'The runs counter and a recount from the runs table disagree — inspect recent runs.'" />
                        <UCard :ui="{ body: 'p-0 sm:p-0' }">
                            <div class="divide-y divide-default">
                                <div class="flex items-center gap-3 px-4 py-3 sm:px-6">
                                    <span class="w-56 shrink-0 text-sm text-muted">Status</span>
                                    <UBadge :label="ledgerInSync ? 'In sync' : 'Drift'"
                                            :color="ledgerInSync ? 'success' : 'warning'"
                                            :icon="ledgerInSync ? 'i-lucide-check' : 'i-lucide-triangle-alert'" />
                                </div>
                                <div class="flex items-center gap-3 px-4 py-3 sm:px-6">
                                    <span class="w-56 shrink-0 text-sm text-muted">Counter</span>
                                    <span class="font-mono text-sm font-medium">{{ quotaRow.successful_runs.toLocaleString() }}</span>
                                </div>
                                <div class="flex items-center gap-3 px-4 py-3 sm:px-6">
                                    <span class="w-56 shrink-0 text-sm text-muted">Runs table</span>
                                    <span class="font-mono text-sm font-medium">{{ quotaRow.recomputed_successful_runs.toLocaleString() }}</span>
                                </div>
                                <div class="flex items-center gap-3 px-4 py-3 sm:px-6">
                                    <span class="w-56 shrink-0 text-sm text-muted">Reserved</span>
                                    <span class="font-mono text-sm font-medium">{{ quotaRow.reserved_runs.toLocaleString() }}</span>
                                </div>
                            </div>
                        </UCard>
                    </section>

                    <section class="flex flex-col gap-3">
                        <CardHeader title="Limits"
                                    :description="`Current period: ${periodLabel}`">
                            <UButton icon="i-lucide-pencil"
                                     label="Edit limits"
                                     color="neutral"
                                     variant="outline"
                                     size="sm"
                                     @click="editOpen = true" />
                        </CardHeader>
                        <UCard>
                            <UTable :data="limitRows"
                                    :columns="limitColumns" />
                        </UCard>
                    </section>
                </template>

                <section v-else-if="tab === 'activity'"
                         class="flex flex-col gap-3">
                    <CardHeader title="Activity"
                                description="Derived from membership, invitation, quota and run records" />
                    <UCard :ui="{ body: 'p-0 sm:p-0' }">
                        <div class="divide-y divide-default">
                            <div v-if="activity.length === 0"
                                 class="px-4 py-5 text-sm text-muted sm:px-6">
                                Nothing recorded yet.
                            </div>
                            <div v-for="entry in activity"
                                 :key="entry.kind + entry.when"
                                 class="flex items-start gap-3 px-4 py-3 sm:px-6">
                                <span class="mt-0.5 flex size-6 shrink-0 items-center justify-center rounded-md bg-elevated text-muted">
                                    <UIcon :name="ACTIVITY_ICONS[entry.kind] ?? 'i-lucide-circle'"
                                           class="size-3.5" />
                                </span>
                                <div class="min-w-0 flex-1">
                                    <div class="text-sm leading-snug">{{ entry.title }}</div>
                                    <div class="mt-0.5 text-xs text-dimmed">
                                        <template v-if="entry.detail">{{ entry.detail }} · </template>{{ timeSince(new Date(entry.when)) }} ago
                                    </div>
                                </div>
                            </div>
                        </div>
                    </UCard>
                </section>

                <template v-else-if="tab === 'settings'">
                    <section class="flex flex-col gap-3">
                        <CardHeader title="General"
                                    description="Naming and your own access to this organisation." />
                        <UCard :ui="{ body: 'p-0 sm:p-0' }">
                            <div class="divide-y divide-default">
                                <div class="flex items-center gap-4 px-4 py-4 sm:px-6">
                                    <div class="min-w-0 flex-1">
                                        <div class="text-sm font-medium text-highlighted">Organisation name</div>
                                        <div class="mt-0.5 text-sm text-muted">Members see this name everywhere in the app.</div>
                                    </div>
                                    <UInput v-model="renameValue"
                                            class="w-60 max-w-[50%]"
                                            @keydown.enter="submitRename" />
                                    <UButton label="Save"
                                             :disabled="!renameValue.trim() || renameValue.trim() === org?.name || renaming"
                                             :loading="renaming"
                                             @click="submitRename" />
                                </div>
                                <div class="flex items-center gap-4 px-4 py-4 sm:px-6">
                                    <div class="min-w-0 flex-1">
                                        <div class="text-sm font-medium text-highlighted">Your membership</div>
                                        <div class="mt-0.5 text-sm text-muted">
                                            <template v-if="isMember">You are an active member, so you can open this workspace directly.</template>
                                            <template v-else>You are not a member of this organisation. Join it to open its workspace.</template>
                                        </div>
                                    </div>
                                    <UButton v-if="isMember"
                                             icon="i-lucide-log-in"
                                             label="Open workspace"
                                             color="neutral"
                                             variant="outline"
                                             @click="openWorkspace" />
                                    <UButton v-else
                                             icon="i-lucide-log-in"
                                             label="Join"
                                             color="neutral"
                                             variant="outline"
                                             @click="joinOrganisation" />
                                </div>
                            </div>
                        </UCard>
                    </section>

                    <section class="flex flex-col gap-3">
                        <CardHeader description="Irreversible actions. Proceed only if you are certain.">
                            <template #title>
                                <span class="text-error">Danger zone</span>
                            </template>
                        </CardHeader>
                        <UCard :ui="{ root: 'ring-error/40', body: 'p-0 sm:p-0' }">
                            <div class="flex items-center gap-4 px-4 py-4 sm:px-6">
                                <div class="min-w-0 flex-1">
                                    <div class="text-sm font-medium text-highlighted">Delete organisation</div>
                                    <div class="mt-0.5 text-sm text-muted">
                                        Permanently deletes {{ org?.name ?? 'this organisation' }} with all its members,
                                        invitations, components and execution history.
                                    </div>
                                </div>
                                <UButton label="Delete this organisation"
                                         color="error"
                                         @click="deleteOpen = true" />
                            </div>
                        </UCard>
                    </section>

                    <UModal v-model:open="deleteOpen"
                            :title="`Delete ${org?.name ?? 'organisation'}?`"
                            description="This permanently deletes the organisation with all its members, invitations, components and execution history. This action cannot be undone.">
                        <template #body>
                            <UFormField :label="`Type “${org?.name}” to confirm`">
                                <UInput v-model="deleteConfirmName"
                                        :placeholder="org?.name"
                                        class="w-full"
                                        @keydown.enter="submitDelete" />
                            </UFormField>
                        </template>
                        <template #footer>
                            <div class="flex w-full justify-end gap-2">
                                <UButton label="Cancel"
                                         color="neutral"
                                         variant="outline"
                                         @click="deleteOpen = false" />
                                <UButton label="Delete organisation"
                                         color="error"
                                         :disabled="deleteConfirmName !== org?.name || deleting"
                                         :loading="deleting"
                                         @click="submitDelete" />
                            </div>
                        </template>
                    </UModal>
                </template>

                <template v-else>
                    <OrganizationMembersTable :members="rows"
                                              :loading="loading"
                                              is-admin
                                              @remove-member="removeMember"
                                              @cancel-invite="cancelInvite"
                                              @resend-invite="resendInvite">
                        <template #actions>
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
                </template>
            </div>

            <AdminQuotaDrawer v-model:open="editOpen"
                              :org-id="orgId"
                              :org-name="org?.name ?? ''"
                              :limits="quotaRow?.limits ?? null"
                              :fields="quotas?.fields ?? []"
                              @saved="reloadQuotas" />
        </template>
    </UDashboardPanel>
</template>
