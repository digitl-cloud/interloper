<script setup lang="ts">
import { h } from 'vue'
import type { TableColumn, DropdownMenuItem, BreadcrumbItem } from '@nuxt/ui'
import type { AdminOrganisation } from '~/types/admin'

definePageMeta({ title: 'Organisations', middleware: 'super-admin' })

const breadcrumbs: BreadcrumbItem[] = [
    { label: 'Platform admin', icon: 'i-lucide-shield' },
    { label: 'Organisations', icon: 'i-lucide-building-2' },
]

const adminStore = useAdminStore()
const toast = useToast()

const rows = ref<AdminOrganisation[]>([])
const loading = ref(false)

// Create / rename modal state
const formOpen = ref(false)
const formMode = ref<'create' | 'rename'>('create')
const formName = ref('')
const formTarget = ref<AdminOrganisation | null>(null)
const submitting = ref(false)

async function loadData() {
    loading.value = true
    try {
        rows.value = await adminStore.listOrganisations()
    }
    catch (err) {
        console.error('[Admin] Failed to load organisations', err)
    }
    finally {
        loading.value = false
    }
}

function openCreate() {
    formMode.value = 'create'
    formName.value = ''
    formTarget.value = null
    formOpen.value = true
}

function openRename(org: AdminOrganisation) {
    formMode.value = 'rename'
    formName.value = org.name
    formTarget.value = org
    formOpen.value = true
}

async function submitForm() {
    const name = formName.value.trim()
    if (!name) return

    submitting.value = true
    try {
        if (formMode.value === 'create') {
            await adminStore.createOrganisation(name)
            toast.add({ title: `Organisation "${name}" created`, color: 'success' })
        }
        else if (formTarget.value) {
            await adminStore.renameOrganisation(formTarget.value.id, name)
            toast.add({ title: 'Organisation renamed', color: 'success' })
        }
        formOpen.value = false
        await loadData()
    }
    catch (err: any) {
        toast.add({ title: err?.data?.detail || 'Operation failed', color: 'error' })
    }
    finally {
        submitting.value = false
    }
}

function rowActions(org: AdminOrganisation): DropdownMenuItem[][] {
    return [
        [
            {
                label: 'Manage members',
                icon: 'i-lucide-users',
                onSelect: () => navigateTo(`/admin/organisations/${org.id}`),
            },
            {
                label: 'Rename',
                icon: 'i-lucide-pencil',
                onSelect: () => openRename(org),
            },
        ],
    ]
}

const columns: TableColumn<AdminOrganisation>[] = [
    {
        accessorKey: 'name',
        header: 'Name',
    },
    {
        accessorKey: 'member_count',
        header: 'Members',
        cell: ({ row }) => row.original.member_count,
    },
    {
        accessorKey: 'created_at',
        header: 'Created',
        cell: ({ row }) => row.original.created_at
            ? new Date(row.original.created_at).toLocaleDateString()
            : '—',
    },
    {
        id: 'open',
        header: '',
        cell: ({ row }) => h(resolveComponent('UButton'), {
            icon: 'i-lucide-arrow-right',
            color: 'neutral',
            variant: 'ghost',
            size: 'xs',
            onClick: () => navigateTo(`/admin/organisations/${row.original.id}`),
        }),
        size: 50,
        enableSorting: false,
        enableGlobalFilter: false,
    },
]

onMounted(loadData)
</script>

<template>
    <div class="flex flex-col flex-1 min-h-0">
        <div class="px-4 pt-4 pb-2 shrink-0">
            <UBreadcrumb :items="breadcrumbs" />
        </div>
        <DataTable :columns="columns"
                   :data="rows"
                   :loading="loading"
                   :row-actions="rowActions"
                   no-actions
                   search-placeholder="Search organisations...">
            <template #toolbar>
                <UButton icon="i-lucide-plus"
                         label="New organisation"
                         @click="openCreate" />
            </template>
        </DataTable>

        <UModal v-model:open="formOpen"
                :title="formMode === 'create' ? 'New organisation' : 'Rename organisation'"
                :ui="{ footer: 'justify-end' }">
            <template #body>
                <UInput v-model="formName"
                        placeholder="Organisation name"
                        autofocus
                        class="w-full"
                        @keydown.enter="submitForm" />
            </template>
            <template #footer>
                <UButton label="Cancel"
                         color="neutral"
                         variant="outline"
                         @click="formOpen = false" />
                <UButton :label="formMode === 'create' ? 'Create' : 'Save'"
                         :disabled="!formName.trim() || submitting"
                         :loading="submitting"
                         @click="submitForm" />
            </template>
        </UModal>
    </div>
</template>
