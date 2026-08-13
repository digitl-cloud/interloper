<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import cronstrue from 'cronstrue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { ComponentRecord } from '~/types/component'
import { jobCron, jobEnabled, jobTargetIds } from '~/types/component'

definePageMeta({ title: 'Jobs' })

const UBadge = resolveComponent('UBadge')
const EntityBadge = resolveComponent('EntityBadge')

const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const toast = useToast()

const jobs = computed(() => componentsStore.byKind('job'))

const stepperRef = ref<any>(null)

const {
    open: drawerOpen,
    editing: editingJob,
    openCreate: handleCreate,
    openEdit: handleEdit,
} = useWizardDrawer<ComponentRecord>()
const runModalOpen = ref(false)
const runModalJob = ref<ComponentRecord | null>(null)

componentsStore.fetchAll()
componentsStore.fetchRelations()

function cronLabel(cron: string): string {
    try {
        return cronstrue.toString(cron, { use24HourTimeFormat: true })
    }
    catch {
        return cron
    }
}

function openRun(job: ComponentRecord) {
    runModalJob.value = job
    runModalOpen.value = true
}

// Deep link (command palette): /jobs?run=<id> opens the run modal for that
// job as soon as it lands in the store.
const route = useRoute()
const router = useRouter()
watchEffect(() => {
    const id = route.query.run
    if (typeof id !== 'string') return
    const job = componentsStore.byId(id)
    if (!job || job.kind !== 'job') return
    openRun(job)
    router.replace({ query: { ...route.query, run: undefined } })
})

const columns = computed<TableColumn<ComponentRecord>[]>(() => [
    { accessorKey: 'select' as any, header: '' },
    {
        accessorKey: 'name',
        header: 'Name',
        cell: ({ row }) => h('span', { class: 'font-medium' }, row.original.name ?? ''),
    },
    {
        accessorKey: 'cron',
        header: 'Schedule',
        cell: ({ row }) => h('span', {
            class: 'text-muted',
            title: jobCron(row.original),
        }, cronLabel(jobCron(row.original))),
    },
    {
        accessorKey: 'source_ids',
        header: 'Sources',
        cell: ({ row }) => {
            const sources = jobTargetIds(row.original, 'source')
                .map(id => componentsStore.byId(id))
                .filter((s): s is ComponentRecord => !!s)
            if (sources.length === 0) return h('span', { class: 'text-muted' }, '—')
            const first = sources[0]!
            return h(EntityBadge, {
                icon: componentIcon(first.key),
                label: first.name ?? first.key,
                extra: sources.length - 1,
            })
        },
    },
    {
        accessorKey: 'enabled',
        header: 'Status',
        cell: ({ row }) => h(UBadge, {
            color: jobEnabled(row.original) ? 'success' : 'neutral',
        }, () => jobEnabled(row.original) ? 'Enabled' : 'Disabled'),
    },
    ...stateSchemaColumns(catalogStore.definitionsForKind('job')[0]),
    {
        accessorKey: 'created_at',
        header: 'Created',
        accessorFn: (row: ComponentRecord) => row.created_at ? formatDate(row.created_at) : '—',
    },
])

function rowActions(job: ComponentRecord): DropdownMenuItem[][] {
    return [[
        {
            label: 'Run',
            icon: 'i-lucide-play',
            onSelect: () => openRun(job),
        },
    ]]
}

function handleSaved() {
    componentsStore.fetchAll(['job'])
    drawerOpen.value = false
}

async function handleDelete(ids: string[]) {
    try {
        await componentsStore.remove(ids)
        toast.add({ title: `${ids.length} job${ids.length > 1 ? 's' : ''} deleted`, color: 'success' })
    }
    catch (e) {
        toast.add(inUseToast(e, 'Job') ?? errorToast(e, 'Failed to delete job'))
    }
}
</script>

<template>
    <div>
        <div class="flex flex-col flex-1 min-h-0">
            <DataTable :columns="columns"
                       :data="jobs"
                       :loading="componentsStore.loading"
                       :row-actions="rowActions"
                       :delete-impact="componentsStore.deleteImpact"
                       search-placeholder="Search jobs..."
                       @delete="handleDelete"
                       @edit="handleEdit">
                <template #toolbar>
                    <UButton icon="i-lucide-plus"
                             label="New job"
                             @click="handleCreate" />
                </template>

                <template #empty>
                    <EmptyState icon="i-lucide-calendar-clock"
                                title="No jobs yet"
                                description="A job is a scheduled pipeline. It runs your sources on a cron schedule and materializes the results into your destination — automatically, with no manual triggering. Every run is recorded under Executions.">
                        <UButton icon="i-lucide-plus"
                                 label="New job"
                                 class="mt-5"
                                 @click="handleCreate" />
                    </EmptyState>
                </template>
            </DataTable>
        </div>

        <WizardDrawer v-model:open="drawerOpen"
                      :default-title="editingJob ? 'Edit Job' : 'New Job'"
                      description="Configure job"
                      :stepper="stepperRef">
            <JobsStepper v-if="drawerOpen"
                         :key="editingJob?.id ?? 'new'"
                         ref="stepperRef"
                         :job="editingJob"
                         @created="handleSaved"
                         @updated="handleSaved" />
        </WizardDrawer>

        <ExecutionsRunModal v-if="runModalJob"
                  v-model:open="runModalOpen"
                  :target="runModalJob" />
    </div>
</template>
