<script setup lang="ts">
import type { BreadcrumbItem, TabsItem } from '@nuxt/ui'
import type { AdminActivityEntry, AdminOrganisation, AdminOrgQuotaStatus, AdminQuotas } from '~/types/admin'
import type { Organisation, OrgMember } from '~/types/organisation'

definePageMeta({ title: 'Manage organisation', layout: 'admin', middleware: 'super-admin', titleInBreadcrumb: true })

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

const breadcrumbs = computed<BreadcrumbItem[]>(() => [
    titleCrumb('Organisations', '/admin/organisations'),
    entityCrumb(org.value?.name ?? '…', 'i-lucide-building-2'),
])

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
            limit: eff.max_sources,
        },
        {
            label: 'Assets / source',
            value: row.max_assets_per_source.toLocaleString(),
            sub: eff.max_assets_per_source != null
                ? `largest source, of ${eff.max_assets_per_source.toLocaleString()}`
                : 'largest source',
            used: row.max_assets_per_source,
            limit: eff.max_assets_per_source,
        },
        {
            label: 'Successful runs',
            value: row.successful_runs.toLocaleString(),
            sub: eff.max_successful_runs_per_month != null
                ? `of ${eff.max_successful_runs_per_month.toLocaleString()} this period`
                : 'this period',
            used: row.successful_runs,
            limit: eff.max_successful_runs_per_month,
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

const LIMIT_FIELDS = [
    { key: 'max_sources', label: 'Max sources' },
    { key: 'max_assets_per_source', label: 'Max assets per source' },
    { key: 'max_successful_runs_per_month', label: 'Max successful runs / month' },
] as const

type LimitKey = typeof LIMIT_FIELDS[number]['key']

const limitRows = computed(() => {
    const row = quotaRow.value
    const defaults = quotas.value?.defaults
    if (!row || !defaults) return []
    return LIMIT_FIELDS.map(({ key, label }) => {
        const override = row.limits[key]
        const effective = row.effective[key]
        return {
            key,
            label,
            value: effective != null ? effective.toLocaleString() : 'Unlimited',
            overridden: override != null,
            note: override != null
                ? `Instance default is ${defaults[key] != null ? defaults[key]!.toLocaleString() : 'unlimited'}`
                : 'Follows the instance default',
        }
    })
})

const ledgerInSync = computed(() =>
    quotaRow.value != null && quotaRow.value.successful_runs === quotaRow.value.recomputed_successful_runs)

// Edit modal — string-typed fields so an emptied input means "inherit".
const editOpen = ref(false)
const editForm = ref<Record<LimitKey, string>>({
    max_sources: '',
    max_assets_per_source: '',
    max_successful_runs_per_month: '',
})
const saving = ref(false)

function defaultPlaceholder(key: LimitKey): string {
    const value = quotas.value?.defaults[key]
    return value != null ? `default: ${value}` : 'default: unlimited'
}

function openEdit() {
    const limits = quotaRow.value?.limits
    editForm.value = {
        max_sources: limits?.max_sources?.toString() ?? '',
        max_assets_per_source: limits?.max_assets_per_source?.toString() ?? '',
        max_successful_runs_per_month: limits?.max_successful_runs_per_month?.toString() ?? '',
    }
    editOpen.value = true
}

async function submitEdit() {
    saving.value = true
    try {
        await adminStore.updateOrgQuota(orgId.value, {
            max_sources: editForm.value.max_sources === '' ? null : Number(editForm.value.max_sources),
            max_assets_per_source: editForm.value.max_assets_per_source === ''
                ? null
                : Number(editForm.value.max_assets_per_source),
            max_successful_runs_per_month: editForm.value.max_successful_runs_per_month === ''
                ? null
                : Number(editForm.value.max_successful_runs_per_month),
        })
        toast.add({ title: 'Quota limits updated', color: 'success' })
        editOpen.value = false
        quotas.value = await adminStore.getQuotas()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to update quota limits'))
    }
    finally {
        saving.value = false
    }
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
        <div class="pb-4 shrink-0">
            <PageBreadcrumb :items="breadcrumbs" />
        </div>

        <div class="flex items-center gap-3.5 mb-5">
            <span class="flex size-11 shrink-0 items-center justify-center rounded-xl text-white text-[17px] font-bold"
                  :style="{ background: avatarColor(orgId) }">
                {{ (org?.name ?? '?').charAt(0).toUpperCase() }}
            </span>
            <div class="min-w-0">
                <h1 class="text-[21px] font-bold tracking-tight truncate">{{ org?.name ?? '…' }}</h1>
                <div class="flex flex-wrap items-center gap-x-2 gap-y-0.5 text-[13px] text-muted mt-0.5">
                    <span>{{ rows.filter(r => r.status === 'active').length }} members</span>
                    <span class="size-[3px] rounded-full bg-accented" />
                    <span>{{ quotaRow?.sources ?? 0 }} sources</span>
                    <span class="size-[3px] rounded-full bg-accented" />
                    <span>Created {{ formatDay(org?.created_at) }}</span>
                    <span class="size-[3px] rounded-full bg-accented" />
                    <span class="font-mono text-xs">{{ orgId.slice(0, 8) }}</span>
                </div>
            </div>
        </div>

        <UTabs v-model="tab"
               :items="tabItems"
               :content="false"
               variant="link"
               class="mb-5 shrink-0" />

        <div v-if="tab === 'members'"
             class="flex flex-col flex-1 min-h-0">
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
             class="flex flex-col gap-3">
            <div class="grid grid-cols-2 xl:grid-cols-4 gap-3">
                <div v-for="tile in usageTiles"
                     :key="tile.label"
                     class="rounded-lg border border-default bg-default px-4 py-3.5">
                    <div class="text-[11.5px] font-semibold uppercase tracking-wider text-dimmed">{{ tile.label }}</div>
                    <div class="text-[23px] font-bold tracking-tight tabular-nums mt-1.5">{{ tile.value }}</div>
                    <div class="text-xs text-muted mt-0.5">{{ tile.sub }}</div>
                    <AdminUsageMeter :used="tile.used"
                                     :limit="tile.limit"
                                     :show-label="false"
                                     class="mt-2.5" />
                </div>
            </div>

            <section class="overflow-hidden rounded-lg border border-default bg-default">
                <div class="flex items-center gap-2 border-b border-default px-4 py-3">
                    <span class="text-sm font-semibold">Limits</span>
                    <span class="text-xs text-dimmed">Current period: {{ periodLabel }}</span>
                    <UButton icon="i-lucide-pencil"
                             label="Edit limits"
                             variant="outline"
                             size="sm"
                             class="ml-auto"
                             @click="openEdit" />
                </div>
                <div v-for="row in limitRows"
                     :key="row.key"
                     class="flex items-center gap-4 px-4 py-3 border-b border-muted">
                    <span class="w-64 shrink-0 text-[13.5px] text-muted">{{ row.label }}</span>
                    <span class="font-mono text-[13px] font-semibold">{{ row.value }}</span>
                    <UBadge :label="row.overridden ? 'Override' : 'Inherited'"
                            :color="row.overridden ? 'info' : 'neutral'"
                            variant="subtle"
                            size="sm" />
                    <span class="text-xs text-dimmed">{{ row.note }}</span>
                </div>
                <div v-if="quotaRow"
                     class="flex items-center gap-2.5 px-4 py-3 bg-elevated/40 text-[13.5px]">
                    <UIcon :name="ledgerInSync ? 'i-lucide-check-circle' : 'i-lucide-triangle-alert'"
                           class="size-4 shrink-0"
                           :class="ledgerInSync ? 'text-success' : 'text-warning'" />
                    <span v-if="ledgerInSync">
                        Ledger in sync — the runs counter and the runs table both report
                        <b>{{ quotaRow.successful_runs.toLocaleString() }}</b> successful runs.
                    </span>
                    <span v-else>
                        Ledger drift — the counter reads <b>{{ quotaRow.successful_runs.toLocaleString() }}</b>,
                        the runs table gives <b>{{ quotaRow.recomputed_successful_runs.toLocaleString() }}</b>.
                    </span>
                </div>
            </section>
        </div>

        <div v-else-if="tab === 'activity'"
             class="overflow-hidden rounded-lg border border-default bg-default">
            <div class="border-b border-default px-4 py-3">
                <div class="text-sm font-semibold">Activity</div>
                <div class="text-xs text-dimmed mt-0.5">
                    Derived from membership, invitation, source and run records
                </div>
            </div>
            <div v-if="activity.length === 0"
                 class="px-4 py-6 text-sm text-muted">
                Nothing recorded yet.
            </div>
            <div v-for="entry in activity"
                 :key="entry.kind + entry.when"
                 class="flex items-start gap-3 px-4 py-3 border-b border-muted last:border-b-0">
                <span class="flex size-6.5 shrink-0 items-center justify-center rounded-lg bg-elevated text-muted mt-0.5">
                    <UIcon :name="ACTIVITY_ICONS[entry.kind] ?? 'i-lucide-circle'"
                           class="size-3.5" />
                </span>
                <div class="flex-1 min-w-0">
                    <div class="text-[13.5px] leading-snug">{{ entry.title }}</div>
                    <div v-if="entry.detail"
                         class="text-xs text-dimmed mt-0.5">{{ entry.detail }}</div>
                </div>
                <span class="shrink-0 text-xs text-dimmed whitespace-nowrap mt-0.5">
                    {{ timeSince(new Date(entry.when)) }} ago
                </span>
            </div>
        </div>

        <div v-else-if="tab === 'settings'"
             class="flex flex-col gap-3 max-w-[660px]">
            <section class="rounded-lg border border-default bg-default p-4">
                <div class="text-sm font-semibold">Organisation name</div>
                <p class="text-[13px] text-muted leading-normal mt-1">
                    Shown across the app and in invitation emails.
                </p>
                <div class="flex items-center gap-2 mt-3">
                    <UInput v-model="renameValue"
                            class="flex-1"
                            @keydown.enter="submitRename" />
                    <UButton label="Save"
                             :disabled="!renameValue.trim() || renameValue.trim() === org?.name || renaming"
                             :loading="renaming"
                             @click="submitRename" />
                </div>
            </section>

            <section class="rounded-lg border border-default bg-default p-4">
                <div class="text-sm font-semibold">Your membership</div>
                <p class="text-[13px] text-muted leading-normal mt-1">
                    <template v-if="isMember">
                        You are an active member of this organisation, so you can open its workspace directly.
                    </template>
                    <template v-else>
                        You are not a member of this organisation. Join it to open its workspace.
                    </template>
                </p>
                <div class="mt-3">
                    <UButton v-if="isMember"
                             icon="i-lucide-log-in"
                             label="Open workspace"
                             variant="outline"
                             @click="openWorkspace" />
                    <UButton v-else
                             icon="i-lucide-log-in"
                             label="Join"
                             variant="outline"
                             @click="joinOrganisation" />
                </div>
            </section>

            <section class="rounded-lg border border-error/40 bg-error/5 p-4">
                <div class="flex items-center gap-2 text-sm font-semibold text-error">
                    <UIcon name="i-lucide-octagon-alert"
                           class="size-4" />
                    Delete organisation
                </div>
                <p class="text-[13px] leading-relaxed mt-1 text-error/90">
                    This permanently deletes {{ org?.name ?? 'this organisation' }} with all its members,
                    invitations, components, and execution history. This action cannot be undone.
                </p>
                <div class="flex items-center gap-2 mt-3">
                    <UInput v-model="deleteConfirmName"
                            :placeholder="`Type “${org?.name}” to confirm`"
                            class="flex-1"
                            @keydown.enter="submitDelete" />
                    <UButton label="Delete"
                             color="error"
                             :disabled="deleteConfirmName !== org?.name || deleting"
                             :loading="deleting"
                             @click="submitDelete" />
                </div>
            </section>
        </div>

        <UModal v-model:open="editOpen"
                :title="`Quota limits — ${org?.name}`"
                :ui="{ footer: 'justify-end' }">
            <template #body>
                <div class="flex flex-col gap-3">
                    <p class="text-sm text-muted">
                        Overrides for this organisation. Leave a field empty to inherit the instance default.
                    </p>
                    <UFormField v-for="field in LIMIT_FIELDS"
                                :key="field.key"
                                :label="field.label">
                        <UInput v-model="editForm[field.key]"
                                type="number"
                                min="0"
                                :placeholder="defaultPlaceholder(field.key)"
                                class="w-full"
                                @keydown.enter="submitEdit" />
                    </UFormField>
                </div>
            </template>
            <template #footer>
                <UButton label="Cancel"
                         color="neutral"
                         variant="outline"
                         @click="editOpen = false" />
                <UButton label="Save"
                         :disabled="saving"
                         :loading="saving"
                         @click="submitEdit" />
            </template>
        </UModal>
    </div>
</template>
