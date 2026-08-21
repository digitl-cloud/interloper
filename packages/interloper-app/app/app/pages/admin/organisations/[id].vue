<script setup lang="ts">
import type { TabsItem } from '@nuxt/ui'
import type { AdminActivityEntry, AdminOrganisation, AdminOrgQuotaStatus, AdminQuotas } from '~/types/admin'
import type { Organisation, OrgMember } from '~/types/organisation'

definePageMeta({ title: 'Manage organisation', layout: 'admin', middleware: 'super-admin', customNavbar: true, fullBleed: true })

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

const tab = ref('members')
const tabItems = computed<TabsItem[]>(() => [
    { label: `Members (${rows.value.length})`, icon: 'i-lucide-users', value: 'members' },
    { label: 'Usage & quotas', icon: 'i-lucide-gauge', value: 'usage' },
    { label: 'Activity', icon: 'i-lucide-activity', value: 'activity' },
    { label: 'Settings', icon: 'i-lucide-sliders-horizontal', value: 'settings' },
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
    <div class="flex flex-col flex-1 min-h-0">
        <NavTitle>
            <ULink to="/admin/organisations"
                   class="text-[15px] font-medium text-muted hover:text-highlighted">Organisations</ULink>
            <span class="text-[15px] text-dimmed">/</span>
            <span class="truncate text-[15px] font-semibold">{{ org?.name ?? '…' }}</span>
        </NavTitle>
        <NavActions v-if="isMember">
            <UButton icon="i-lucide-external-link"
                     label="Open workspace"
                     color="neutral"
                     variant="outline"
                     size="sm"
                     @click="openWorkspace" />
        </NavActions>

        <!-- Tab strip flush under the navbar, rule across the full panel. -->
        <UTabs v-model="tab"
               :items="tabItems"
               :content="false"
               variant="link"
               class="shrink-0"
               :ui="{ list: 'px-6' }" />

        <div class="flex-1 min-h-0 overflow-y-auto">
            <div class="mx-auto w-full max-w-[1040px] px-6 py-8">

                <div v-if="tab === 'members'"
                     class="flex flex-col min-h-0">
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

                <div v-else-if="tab === 'usage'"
                     class="flex flex-col gap-7">
                    <!-- Usage strip: fused stat cells over a period-elapsed bar. -->
                    <div class="overflow-hidden rounded-lg border border-default bg-(--ui-border)">
                        <div class="grid grid-cols-2 xl:grid-cols-4 gap-px">
                            <div v-for="tile in usageTiles"
                                 :key="tile.label"
                                 class="flex flex-col gap-2.5 bg-muted p-4">
                                <div class="text-xs uppercase tracking-wider text-dimmed">{{ tile.label }}</div>
                                <div class="flex items-baseline gap-2 min-w-0">
                                    <span class="text-2xl font-semibold tracking-tight tabular-nums">{{ tile.value }}</span>
                                    <span class="truncate text-[12.5px] text-muted">{{ tile.sub }}</span>
                                </div>
                                <AdminUsageMeter v-if="tile.limit != null"
                                                 :used="tile.used"
                                                 :limit="tile.limit"
                                                 :show-label="false" />
                            </div>
                        </div>
                        <div v-if="periodElapsed"
                             class="border-t border-default bg-default px-4 py-3.5">
                            <div class="flex items-baseline gap-2">
                                <span class="flex-1 truncate text-[13.5px] font-medium">Period elapsed · {{ periodLabel }}</span>
                                <span class="whitespace-nowrap text-[12.5px] text-muted">{{ periodElapsed.label }}</span>
                                <span class="whitespace-nowrap text-[12.5px] font-semibold text-primary">{{ periodElapsed.pct }}%</span>
                            </div>
                            <div class="mt-2 h-1.5 overflow-hidden rounded-full bg-accented">
                                <div class="h-full rounded-full bg-primary"
                                     :style="{ width: periodElapsed.pct + '%' }" />
                            </div>
                        </div>
                    </div>

                    <PanelCard v-if="quotaRow"
                             title="Ledger"
                             :description="ledgerInSync
                                 ? 'The runs counter and a recount from the runs table agree.'
                                 : 'The runs counter and a recount from the runs table disagree — inspect recent runs.'">
                        <div class="flex items-start gap-3 px-4 py-3">
                            <span class="w-56 shrink-0 text-sm text-muted">Status</span>
                            <UBadge :label="ledgerInSync ? 'In sync' : 'Drift'"
                                    :color="ledgerInSync ? 'success' : 'warning'"
                                    :icon="ledgerInSync ? 'i-lucide-check' : 'i-lucide-triangle-alert'" />
                        </div>
                        <div class="flex items-start gap-3 px-4 py-3">
                            <span class="w-56 shrink-0 text-sm text-muted">Counter</span>
                            <span class="font-mono text-[13px] font-medium">{{ quotaRow.successful_runs.toLocaleString() }}</span>
                        </div>
                        <div class="flex items-start gap-3 px-4 py-3">
                            <span class="w-56 shrink-0 text-sm text-muted">Runs table</span>
                            <span class="font-mono text-[13px] font-medium">{{ quotaRow.recomputed_successful_runs.toLocaleString() }}</span>
                        </div>
                        <div class="flex items-start gap-3 px-4 py-3">
                            <span class="w-56 shrink-0 text-sm text-muted">Reserved</span>
                            <span class="font-mono text-[13px] font-medium">{{ quotaRow.reserved_runs.toLocaleString() }}</span>
                        </div>
                    </PanelCard>

                    <section>
                        <div class="mb-3 flex items-center gap-2.5">
                            <div class="min-w-0">
                                <div class="text-[15px] font-semibold text-highlighted">Limits</div>
                            </div>
                            <span class="ml-auto text-xs text-dimmed">Current period: {{ periodLabel }}</span>
                            <UButton icon="i-lucide-pencil"
                                     label="Edit limits"
                                     color="neutral"
                                     variant="outline"
                                     size="sm"
                                     @click="editOpen = true" />
                        </div>
                        <div class="overflow-hidden rounded-lg border border-default">
                            <div class="flex items-center gap-4 border-b border-default bg-muted px-4 py-3 text-sm font-semibold text-highlighted">
                                <span class="w-56 shrink-0">Limit</span>
                                <span class="flex-1">Value</span>
                                <span class="w-24">Source</span>
                            </div>
                            <div v-for="row in limitRows"
                                 :key="row.key"
                                 class="flex items-center gap-4 border-b border-default px-4 py-3 last:border-b-0">
                                <span class="w-56 shrink-0 text-sm font-medium">{{ row.label }}</span>
                                <span class="flex-1 min-w-0">
                                    <span class="font-mono text-[13px] font-medium">{{ row.value }}</span>
                                    <span class="ml-2 text-xs text-dimmed">{{ row.note }}</span>
                                </span>
                                <span class="w-24">
                                    <UBadge :label="row.overridden ? 'Override' : 'Inherited'"
                                            :color="row.overridden ? 'info' : 'neutral'"
                                            size="sm" />
                                </span>
                            </div>
                        </div>
                    </section>
                </div>

                <PanelCard v-else-if="tab === 'activity'"
                         title="Activity"
                         description="Derived from membership, invitation, quota and run records">
                    <div v-if="activity.length === 0"
                         class="px-4 py-6 text-sm text-muted">
                        Nothing recorded yet.
                    </div>
                    <div v-for="entry in activity"
                         :key="entry.kind + entry.when"
                         class="flex items-start gap-3 px-4 py-3">
                        <span class="mt-0.5 flex size-6 shrink-0 items-center justify-center rounded-md bg-elevated text-muted">
                            <UIcon :name="ACTIVITY_ICONS[entry.kind] ?? 'i-lucide-circle'"
                                   class="size-3.5" />
                        </span>
                        <div class="flex-1 min-w-0">
                            <div class="text-[13.5px] leading-snug">{{ entry.title }}</div>
                            <div class="mt-0.5 text-xs text-dimmed">
                                <template v-if="entry.detail">{{ entry.detail }} · </template>{{ timeSince(new Date(entry.when)) }} ago
                            </div>
                        </div>
                    </div>
                </PanelCard>

                <div v-else-if="tab === 'settings'"
                     class="flex flex-col gap-8">
                    <PanelCard title="General"
                             description="Naming and your own access to this organisation.">
                        <div class="flex items-center gap-4 px-4 py-3.5">
                            <div class="flex-1 min-w-0">
                                <div class="text-sm font-medium">Organisation name</div>
                                <div class="mt-0.5 text-[13px] text-dimmed">Members see this name everywhere in the app.</div>
                            </div>
                            <UInput v-model="renameValue"
                                    class="w-60 max-w-[50%]"
                                    @keydown.enter="submitRename" />
                            <UButton label="Save"
                                     :disabled="!renameValue.trim() || renameValue.trim() === org?.name || renaming"
                                     :loading="renaming"
                                     @click="submitRename" />
                        </div>
                        <div class="flex items-center gap-4 px-4 py-3.5">
                            <div class="flex-1 min-w-0">
                                <div class="text-sm font-medium">Your membership</div>
                                <div class="mt-0.5 text-[13px] text-dimmed">
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
                    </PanelCard>

                    <PanelCard tone="danger"
                             icon="i-lucide-octagon-alert"
                             icon-class="text-error"
                             title="Danger zone"
                             description="Irreversible actions. Proceed only if you are certain.">
                        <div class="flex items-center gap-4 px-4 py-3.5">
                            <div class="flex-1 min-w-0">
                                <div class="text-sm font-medium">Delete organisation</div>
                                <div class="mt-0.5 text-[13px] text-dimmed">
                                    Permanently deletes {{ org?.name ?? 'this organisation' }} with all its members,
                                    invitations, components and execution history.
                                </div>
                            </div>
                            <UButton label="Delete this organisation"
                                     color="error"
                                     @click="deleteOpen = true" />
                        </div>
                    </PanelCard>

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
                </div>

            </div>
        </div>

        <AdminQuotaDrawer v-model:open="editOpen"
                          :org-id="orgId"
                          :org-name="org?.name ?? ''"
                          :limits="quotaRow?.limits ?? null"
                          :fields="quotas?.fields ?? []"
                          @saved="reloadQuotas" />
    </div>
</template>
