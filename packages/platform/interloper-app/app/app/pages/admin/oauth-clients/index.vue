<script setup lang="ts">
import { h } from 'vue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { AdminOAuthClient } from '~/types/admin'

definePageMeta({
    layout: 'admin',
    middleware: 'super-admin',
})

const adminStore = useAdminStore()
const appConfig = useAppConfig()
const toast = useToast()
const { confirm } = useConfirm()

const rows = ref<AdminOAuthClient[]>([])
const loading = ref(false)
const error = ref<Error | null>(null)
const createOpen = ref(false)

async function loadData() {
    loading.value = true
    error.value = null
    try {
        rows.value = await adminStore.listOAuthClients()
    }
    catch (err) {
        error.value = err as Error
    }
    finally {
        loading.value = false
    }
}

async function deleteClient(client: AdminOAuthClient) {
    const confirmed = await confirm({
        title: 'Delete OAuth client',
        description: 'This permanently deletes {subject}. Every user who authorised it is signed out of it: '
            + 'its access and refresh tokens are revoked, its grants end, and the conversations it started '
            + 'are deleted. The app it belongs to stops working until it is given a new client. '
            + 'This action cannot be undone.',
        subject: { name: client.name, icon: appConfig.icons.app },
        confirmLabel: 'Delete client',
    })
    if (!confirmed) return

    try {
        await adminStore.deleteOAuthClient(client.id)
        toast.add({ title: `${client.name} deleted`, color: 'success' })
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, "Couldn't delete the client"))
    }
}

function rowActions(client: AdminOAuthClient): DropdownMenuItem[][] {
    return [
        [
            {
                label: 'Delete client',
                icon: appConfig.icons.delete,
                color: 'error' as const,
                onSelect: () => deleteClient(client),
            },
        ],
    ]
}

const columns: TableColumn<AdminOAuthClient>[] = [
    {
        accessorKey: 'name',
        header: 'Name',
        cell: ({ row }) => h('span', { class: 'font-medium text-highlighted' }, row.original.name),
    },
    {
        accessorKey: 'id',
        header: 'Client ID',
        cell: ({ row }) => h('span', { class: 'text-muted tabular-nums slashed-zero' }, row.original.id),
    },
    {
        id: 'redirect_uris',
        header: 'Redirect URIs',
        accessorFn: row => row.redirect_uris.join(' '),
        cell: ({ row }) => h('div', { class: 'flex flex-col gap-0.5' },
            row.original.redirect_uris.map(uri => h('span', { class: 'text-muted' }, uri))),
    },
    {
        accessorKey: 'created_at',
        header: 'Created',
        cell: ({ row }) => h('span', { class: 'text-muted tabular-nums' }, formatDay(row.original.created_at)),
    },
]

onMounted(loadData)
</script>

<template>
    <UDashboardPanel id="admin-oauth-clients">
        <template #header>
            <AppNavbar title="OAuth clients" />
        </template>
        <template #body>
            <DataTable :columns="columns"
                       fill
                       :data="rows"
                       :loading="loading"
                       :error="error"
                       :row-actions="rowActions"
                       no-actions
                       no-row-click
                       search-placeholder="Search clients…"
                       @retry="loadData">
                <template #actions>
                    <UButton :icon="appConfig.ui.icons.plus"
                             label="New client"
                             @click="createOpen = true" />
                </template>
            </DataTable>

            <AdminOAuthClientCreateModal v-model:open="createOpen"
                                         @created="loadData" />
        </template>
    </UDashboardPanel>
</template>
