<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import cronstrue from 'cronstrue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { Job } from '~/types/job'

definePageMeta({ title: 'Jobs' })

const UBadge = resolveComponent('UBadge')

const jobsStore = useJobsStore()
const sourcesStore = useSourcesStore()
const toast = useToast()

const drawerOpen = ref(false)
const stepperRef = ref<any>(null)
const editingJob = ref<Job | null>(null)
const runModalOpen = ref(false)
const runModalJob = ref<Job | null>(null)

jobsStore.fetch()
sourcesStore.fetch()

function cronLabel(cron: string): string {
    try {
        return cronstrue.toString(cron, { use24HourTimeFormat: true })
    }
    catch {
        return cron
    }
}

function handleCreate() {
    editingJob.value = null
    drawerOpen.value = true
}

function handleEdit(job: Job) {
    editingJob.value = job
    drawerOpen.value = true
}

function openRun(job: Job) {
    runModalJob.value = job
    runModalOpen.value = true
}

const columns: TableColumn<Job>[] = [
    { accessorKey: 'select' as any, header: '' },
    {
        accessorKey: 'name',
        header: 'Name',
        cell: ({ row }) => h('span', { class: 'font-medium' }, row.original.name),
    },
    {
        accessorKey: 'cron',
        header: 'Schedule',
        cell: ({ row }) => h('span', {
            class: 'text-muted',
            title: row.original.cron,
        }, cronLabel(row.original.cron)),
    },
    {
        accessorKey: 'source_ids',
        header: 'Sources',
        cell: ({ row }) => h(UBadge, {
            color: 'neutral',
            variant: 'subtle',
        }, () => `${row.original.source_ids.length} source${row.original.source_ids.length !== 1 ? 's' : ''}`),
    },
    {
        accessorKey: 'enabled',
        header: 'Status',
        cell: ({ row }) => h(UBadge, {
            color: row.original.enabled ? 'success' : 'neutral',
            variant: 'subtle',
        }, () => row.original.enabled ? 'Enabled' : 'Disabled'),
    },
    {
        accessorKey: 'last_run_at',
        header: 'Last run',
        accessorFn: (row: Job) => row.last_run_at ? formatDate(row.last_run_at) : '—',
    },
    {
        accessorKey: 'created_at',
        header: 'Created',
        accessorFn: (row: Job) => row.created_at ? formatDate(row.created_at) : '—',
    },
]

function rowActions(job: Job): DropdownMenuItem[][] {
    return [[
        {
            label: 'Run',
            icon: 'i-lucide-play',
            onSelect: () => openRun(job),
        },
    ]]
}

function handleSaved() {
    jobsStore.fetch()
    drawerOpen.value = false
}

async function handleDelete(ids: string[]) {
    try {
        await jobsStore.remove(ids)
        toast.add({ title: `${ids.length} job${ids.length > 1 ? 's' : ''} deleted`, color: 'success' })
    }
    catch {
        toast.add({ title: 'Failed to delete job', color: 'error' })
    }
}
</script>

<template>
    <div>
        <div class="flex flex-col h-full">
            <DataTable :columns="columns"
                       :data="jobsStore.jobs"
                       :loading="jobsStore.loading"
                       :row-actions="rowActions"
                       search-placeholder="Search jobs..."
                       @delete="handleDelete"
                       @edit="handleEdit">
                <template #toolbar>
                    <UButton icon="i-lucide-plus"
                             label="New job"
                             @click="handleCreate" />
                </template>
            </DataTable>
        </div>

        <UDrawer v-model:open="drawerOpen"
                 direction="right"
                 :handle="false"
                 :handle-only="true"
                 :title="stepperRef?.title ?? 'New Job'"
                 :ui="{ content: 'w-[36rem]', description: 'sr-only' }">
            <template #description>
                {{ editingJob ? 'Edit job configuration' : 'Configure a new job' }}
            </template>
            <template #body>
                <JobsStepper v-if="drawerOpen"
                             :key="editingJob?.id ?? 'new'"
                             ref="stepperRef"
                             :job="editingJob"
                             @created="handleSaved"
                             @updated="handleSaved" />
            </template>
            <template #footer>
                <StepperNav v-if="stepperRef"
                            :can-proceed="stepperRef.canProceed"
                            :has-prev="stepperRef.hasPrev"
                            :submitting="stepperRef.submitting"
                            :submit-label="stepperRef.submitLabel"
                            @next="stepperRef.next()"
                            @prev="stepperRef.prev()" />
            </template>
        </UDrawer>

        <JobsRunModal v-if="runModalJob"
                       v-model:open="runModalOpen"
                       :job="runModalJob" />
    </div>
</template>
