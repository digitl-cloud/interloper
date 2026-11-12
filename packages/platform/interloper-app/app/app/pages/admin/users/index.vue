<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { AdminUser } from '~/types/admin'

definePageMeta({
    layout: 'admin',
    middleware: 'super-admin',
})

const UAvatar = resolveComponent('UAvatar')
const UBadge = resolveComponent('UBadge')
const EntityBadge = resolveComponent('EntityBadge')

const adminStore = useAdminStore()
const userStore = useUserStore()
const appConfig = useAppConfig()
const toast = useToast()
const { confirm } = useConfirm()

const rows = ref<AdminUser[]>([])
const loading = ref(false)
const error = ref<Error | null>(null)

const ALL_ORGS = 'all'
const orgFilter = useQueryParam('org', ALL_ORGS)

const orgOptions = computed(() => {
    const seen = new Map<string, string>()
    for (const user of rows.value)
        for (const org of user.organisations) seen.set(org.id, org.name)
    return [
        { label: 'All organisations', value: ALL_ORGS },
        ...[...seen]
            .map(([id, name]) => ({ label: name, value: id }))
            .sort((a, b) => a.label.localeCompare(b.label)),
    ]
})

const filteredRows = computed(() => orgFilter.value === ALL_ORGS
    ? rows.value
    : rows.value.filter(user => user.organisations.some(org => org.id === orgFilter.value)))

async function loadData() {
    loading.value = true
    error.value = null
    try {
        rows.value = await adminStore.listUsers()
    }
    catch (err) {
        error.value = err as Error
    }
    finally {
        loading.value = false
    }
}

async function deleteUser(user: AdminUser) {
    const confirmed = await confirm({
        title: 'Delete user',
        description: 'This permanently deletes {subject}, along with their sessions, tokens, '
            + 'organisation memberships, and the invitations they sent. This action cannot be undone.',
        subject: { name: user.name || user.email, icon: appConfig.icons.user },
    })
    if (!confirmed) return

    try {
        await adminStore.deleteUser(user.id)
        toast.add({ title: `${user.name || user.email} deleted`, color: 'success' })
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, "Couldn't delete the user"))
    }
}

async function setSuperAdmin(user: AdminUser, isSuperAdmin: boolean) {
    const confirmed = await confirm(isSuperAdmin
        ? {
                title: 'Make super admin',
                description: '{subject} will manage every organisation, user and quota on this instance. '
                    + 'Every super admin is notified by email.',
                subject: { name: user.name || user.email, icon: appConfig.icons.user },
                confirmLabel: 'Make super admin',
                confirmColor: 'primary',
            }
        : {
                title: 'Remove super admin',
                description: '{subject} loses access to the admin portal. Their organisation memberships are kept.',
                subject: { name: user.name || user.email, icon: appConfig.icons.user },
                confirmLabel: 'Remove super admin',
            })
    if (!confirmed) return

    try {
        const updated = await adminStore.setSuperAdmin(user.id, isSuperAdmin)
        rows.value = rows.value.map(row => row.id === updated.id ? updated : row)
        toast.add({
            title: isSuperAdmin
                ? `${user.name || user.email} is now a super admin`
                : `${user.name || user.email} is no longer a super admin`,
            color: 'success',
        })
    }
    catch (err) {
        toast.add(errorToast(err, "Couldn't update the super admin access"))
    }
}

function rowActions(user: AdminUser): DropdownMenuItem[][] {
    // No self-service changes: the backend rejects them too.
    if (user.id === userStore.user?.id) return []
    return [
        [
            user.is_super_admin
                ? {
                        label: 'Remove super admin',
                        icon: appConfig.icons.admin,
                        onSelect: () => setSuperAdmin(user, false),
                    }
                : {
                        label: 'Make super admin',
                        icon: appConfig.icons.admin,
                        onSelect: () => setSuperAdmin(user, true),
                    },
        ],
        [
            {
                label: 'Delete user',
                icon: appConfig.icons.delete,
                color: 'error' as const,
                onSelect: () => deleteUser(user),
            },
        ],
    ]
}

const columns: TableColumn<AdminUser>[] = [
    {
        accessorKey: 'name',
        header: 'Name',
        cell: ({ row }) => {
            const user = row.original
            const avatar = h(UAvatar, { src: user.avatar_url ?? undefined, alt: user.name || user.email, size: 'sm' })
            const name = user.name
                ? h('span', { class: 'font-medium text-highlighted' }, user.name)
                : h('span', { class: 'text-dimmed' }, '—')
            return h('div', { class: 'flex items-center gap-3' }, [avatar, name])
        },
    },
    {
        accessorKey: 'email',
        header: 'Email',
        cell: ({ row }) => h('span', { class: 'text-muted' }, row.original.email),
    },
    {
        id: 'organisations',
        header: 'Organisations',
        accessorFn: row => row.organisations.map(org => org.name).join(', '),
        cell: ({ row }) => {
            const orgs = row.original.organisations
            const first = orgs[0]
            return first
                ? h(EntityBadge, { icon: appConfig.icons.organisation, label: first.name, extra: orgs.length - 1 })
                : h('span', { class: 'text-dimmed' }, '—')
        },
    },
    {
        accessorKey: 'is_super_admin',
        header: 'Super admin',
        cell: ({ row }) => row.original.is_super_admin
            ? h(UBadge, { label: 'Super admin', icon: appConfig.icons.admin, color: 'primary' })
            : h('span', { class: 'text-dimmed' }, '—'),
    },
    {
        accessorKey: 'created_at',
        header: 'Joined',
        cell: ({ row }) => h('span', { class: 'text-muted tabular-nums' }, formatDay(row.original.created_at)),
    },
]

onMounted(loadData)
</script>

<template>
    <UDashboardPanel id="admin-users">
        <template #header>
            <AppNavbar title="Users" />
        </template>
        <template #body>
            <DataTable :columns="columns"
                       fill
                       :data="filteredRows"
                       :loading="loading"
                       :error="error"
                       :row-actions="rowActions"
                       no-actions
                       no-row-click
                       search-placeholder="Search users…"
                       @retry="loadData">
                <template #filters>
                    <USelect v-model="orgFilter"
                             :items="orgOptions"
                             value-key="value"
                             :icon="appConfig.icons.organisation"
                             class="w-52" />
                </template>
            </DataTable>
        </template>
    </UDashboardPanel>
</template>
