<script setup lang="ts">
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { AdminOrganisation } from '~/types/admin'

definePageMeta({
    title: 'Organisations',
    layout: 'admin',
    middleware: 'super-admin',
})

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
    catch (err) {
        toast.add(errorToast(err, 'Operation failed'))
    }
    finally {
        submitting.value = false
    }
}

function openOrg(org: AdminOrganisation) {
    navigateTo(`/admin/organisations/${org.id}`)
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
]

onMounted(loadData)
</script>

<template>
    <div class="flex flex-col flex-1 min-h-0">
        <DataTable :columns="columns"
                   :data="rows"
                   :loading="loading"
                   :row-actions="rowActions"
                   no-actions
                   search-placeholder="Search organisations..."
                   @edit="openOrg">
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
