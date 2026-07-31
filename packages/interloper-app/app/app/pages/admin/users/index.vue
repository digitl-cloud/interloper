<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import type { AdminUser } from '~/types/admin'

definePageMeta({
    title: 'Users',
    layout: 'admin',
    middleware: 'super-admin',
    pageHeader: {
        title: 'Users',
        description: 'Everyone with a profile on the platform, across all organisations.',
    },
})

const UAvatar = resolveComponent('UAvatar')
const UBadge = resolveComponent('UBadge')

const adminStore = useAdminStore()

const rows = ref<AdminUser[]>([])
const loading = ref(false)

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
        accessorKey: 'organisation_count',
        header: 'Organisations',
        cell: ({ row }) => row.original.organisation_count,
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
                   :data="rows"
                   :loading="loading"
                   no-actions
                   search-placeholder="Search users..." />
    </div>
</template>
