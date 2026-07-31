<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { AdminUser } from '~/types/admin'

definePageMeta({
    title: 'Users',
    layout: 'admin',
    middleware: 'super-admin',
})

const UAvatar = resolveComponent('UAvatar')
const UBadge = resolveComponent('UBadge')
const EntityBadge = resolveComponent('EntityBadge')

const adminStore = useAdminStore()
const userStore = useUserStore()
const toast = useToast()
const { confirm } = useConfirm()

const rows = ref<AdminUser[]>([])
const loading = ref(false)

const ALL_ORGS = 'all'
const orgFilter = ref(ALL_ORGS)

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
    try {
        rows.value = await adminStore.listUsers()
    }
    catch (err) {
        console.error('[Admin] Failed to load users', err)
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
        subject: { name: user.name || user.email, icon: 'i-lucide-user' },
        confirmColor: 'error',
    })
    if (!confirmed) return

    try {
        await adminStore.deleteUser(user.id)
        toast.add({ title: `${user.name || user.email} deleted`, color: 'success' })
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to delete user'))
    }
}

function rowActions(user: AdminUser): DropdownMenuItem[][] {
    // No self-service deletion — the backend rejects it too.
    if (user.id === userStore.user?.id) return []
    return [
        [
            {
                label: 'Delete user',
                icon: 'i-lucide-trash-2',
                color: 'error' as const,
                onSelect: () => deleteUser(user),
            },
        ],
    ]
}

function initials(user: AdminUser): string {
    const name = user.name?.trim()
    if (name) {
        const parts = name.split(/\s+/)
        const first = parts[0]?.[0] ?? ''
        const last = parts.length > 1 ? (parts[parts.length - 1]?.[0] ?? '') : ''
        return (last ? first + last : first).toUpperCase()
    }
    return user.email?.charAt(0).toUpperCase() ?? '?'
}

const columns: TableColumn<AdminUser>[] = [
    {
        accessorKey: 'name',
        header: 'Name',
        cell: ({ row }) => {
            const user = row.original
            const avatar = h(UAvatar, {
                src: user.avatar_url ?? undefined,
                alt: user.name ?? user.email,
                text: initials(user),
                size: 'sm',
            })
            const name = user.name
                ? h('span', { class: 'font-semibold text-highlighted' }, user.name)
                : h('span', { class: 'text-dimmed' }, '—')
            return h('div', { class: 'flex items-center gap-3' }, [avatar, name])
        },
    },
    {
        accessorKey: 'email',
        header: 'Email',
    },
    {
        id: 'organisations',
        header: 'Organisations',
        accessorFn: row => row.organisations.map(org => org.name).join(', '),
        cell: ({ row }) => {
            const orgs = row.original.organisations
            const first = orgs[0]
            return first
                ? h(EntityBadge, { icon: 'i-lucide-building-2', label: first.name, extra: orgs.length - 1 })
                : h('span', { class: 'text-dimmed' }, '—')
        },
    },
    {
        accessorKey: 'is_super_admin',
        header: 'Super admin',
        cell: ({ row }) => row.original.is_super_admin
            ? h(UBadge, { label: 'Super admin', icon: 'i-lucide-shield', color: 'primary', variant: 'subtle' })
            : h('span', { class: 'text-dimmed' }, '—'),
    },
    {
        accessorKey: 'created_at',
        header: 'Joined',
        cell: ({ row }) => row.original.created_at
            ? new Date(row.original.created_at).toLocaleDateString()
            : '—',
    },
]

onMounted(loadData)
</script>

<template>
    <div class="flex flex-col flex-1 min-h-0">
        <DataTable :columns="columns"
                   :data="filteredRows"
                   :loading="loading"
                   :row-actions="rowActions"
                   no-actions
                   search-placeholder="Search users...">
            <template #toolbar>
                <USelect v-model="orgFilter"
                         :items="orgOptions"
                         value-key="value"
                         icon="i-lucide-building-2"
                         class="w-52" />
            </template>
        </DataTable>
    </div>
</template>
