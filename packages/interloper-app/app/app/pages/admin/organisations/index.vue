<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { AdminOrganisation } from '~/types/admin'

definePageMeta({
    title: 'Organisations',
    layout: 'admin',
    middleware: 'super-admin',
})

const UBadge = resolveComponent('UBadge')

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

// Delete modal state — confirmed by typing the organisation's exact name.
const deleteOpen = ref(false)
const deleteTarget = ref<AdminOrganisation | null>(null)
const deleteConfirmName = ref('')
const deleting = ref(false)

function openCreate() {
    formMode.value = 'create'
    formName.value = ''
    formTarget.value = null
    formOpen.value = true
}

function openDelete(org: AdminOrganisation) {
    deleteTarget.value = org
    deleteConfirmName.value = ''
    deleteOpen.value = true
}

async function submitDelete() {
    const target = deleteTarget.value
    if (!target || deleteConfirmName.value !== target.name) return

    deleting.value = true
    try {
        await adminStore.deleteOrganisation(target.id, deleteConfirmName.value)
        toast.add({ title: `Organisation "${target.name}" deleted`, color: 'success' })
        deleteOpen.value = false
        await loadData()
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to delete organisation'))
    }
    finally {
        deleting.value = false
    }
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
    if (org.deleted_at) return
    navigateTo(`/admin/organisations/${org.id}`)
}

function rowActions(org: AdminOrganisation): DropdownMenuItem[][] {
    if (org.deleted_at) return []
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
        [
            {
                label: 'Delete organisation',
                icon: 'i-lucide-trash-2',
                color: 'error' as const,
                onSelect: () => openDelete(org),
            },
        ],
    ]
}

const columns: TableColumn<AdminOrganisation>[] = [
    {
        accessorKey: 'name',
        header: 'Name',
        cell: ({ row }) => {
            const org = row.original
            if (!org.deleted_at) return org.name
            return h('div', { class: 'flex items-center gap-2' }, [
                h('span', { class: 'text-dimmed line-through' }, org.name),
                h(UBadge, { label: 'Deleted', color: 'neutral', size: 'sm' }),
            ])
        },
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

        <UModal v-model:open="deleteOpen"
                title="Delete organisation"
                :ui="{ footer: 'justify-end' }">
            <template #body>
                <div class="flex flex-col gap-3">
                    <p class="text-sm text-muted">
                        This permanently deletes
                        <EntityBadge icon="i-lucide-building-2"
                                     :label="deleteTarget?.name ?? ''" />
                        with all its members, invitations, components, and execution history.
                        This action cannot be undone.
                    </p>
                    <UInput v-model="deleteConfirmName"
                            :placeholder="`Type “${deleteTarget?.name}” to confirm`"
                            autofocus
                            class="w-full"
                            @keydown.enter="submitDelete" />
                </div>
            </template>
            <template #footer>
                <UButton label="Cancel"
                         color="neutral"
                         variant="outline"
                         @click="deleteOpen = false" />
                <UButton label="Delete"
                         color="error"
                         :disabled="deleteConfirmName !== deleteTarget?.name || deleting"
                         :loading="deleting"
                         @click="submitDelete" />
            </template>
        </UModal>

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
