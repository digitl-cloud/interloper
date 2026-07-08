<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import cronstrue from 'cronstrue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { ComponentRecord } from '~/types/component'
import { jobCron, jobEnabled, jobTargetIds } from '~/types/component'

definePageMeta({ title: 'Jobs' })

const UBadge = resolveComponent('UBadge')

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

componentsStore.fetchAll(['job', 'source'])

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

/** Onboarding cards shown in the empty state. */
const SETUP_STEPS = [
    { to: '/sources', icon: 'i-lucide-plug', title: 'Step 1 · Connect a source', sub: 'Pull from Google Ads, Meta, Bing & more', link: 'Go to Sources' },
    { to: '/destinations', icon: 'i-lucide-database', title: 'Step 2 · Add a destination', sub: 'Choose where assets are materialized', link: 'Go to Destinations' },
]

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
            const count = jobTargetIds(row.original, 'source').length
            return h(UBadge, {
                color: 'neutral',
                variant: 'subtle',
            }, () => `${count} source${count !== 1 ? 's' : ''}`)
        },
    },
    {
        accessorKey: 'enabled',
        header: 'Status',
        cell: ({ row }) => h(UBadge, {
            color: jobEnabled(row.original) ? 'success' : 'neutral',
            variant: 'subtle',
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
    catch {
        toast.add({ title: 'Failed to delete job', color: 'error' })
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
                                description="A job is a scheduled pipeline. It runs your sources on a cron schedule and materializes the results into your destination — automatically, with no manual triggering. Every run is recorded under Executions." />

                    <div class="mt-9">
                        <div class="eyebrow text-primary">
                            Before your first job
                        </div>
                        <h2 class="text-[19px] font-bold tracking-[-0.015em] text-highlighted mt-2">
                            A job needs something to run
                        </h2>
                        <p class="text-[14.5px] text-muted leading-relaxed max-w-[640px] mt-2">
                            Set up at least one source to pull from and one destination to write to.
                            Once both exist, you can wire them into a scheduled job.
                        </p>
                    </div>

                    <div class="grid grid-cols-1 sm:grid-cols-2 gap-3.5 mt-5">
                        <NuxtLink v-for="step in SETUP_STEPS"
                                  :key="step.to"
                                  :to="step.to"
                                  class="block border border-default rounded-[14px] p-5 bg-default shadow-xs transition hover:border-primary/40 hover:shadow-md hover:-translate-y-0.5">
                            <div class="flex items-center gap-3">
                                <div class="size-[42px] rounded-[11px] bg-primary/10 text-primary flex items-center justify-center">
                                    <UIcon :name="step.icon"
                                           class="size-[21px]" />
                                </div>
                                <div>
                                    <div class="text-[15.5px] font-semibold text-highlighted">{{ step.title }}</div>
                                    <div class="text-[12.5px] text-dimmed mt-0.5">{{ step.sub }}</div>
                                </div>
                            </div>
                            <div class="flex items-center gap-1.5 mt-3.5 text-primary text-[13.5px] font-semibold">
                                {{ step.link }}
                                <UIcon name="i-lucide-arrow-right"
                                       class="size-3" />
                            </div>
                        </NuxtLink>
                    </div>

                    <div class="flex items-center gap-3 border border-default rounded-[14px] px-5 py-4 bg-(--ui-bg-band) mt-4">
                        <div class="size-[30px] shrink-0 rounded-full bg-elevated flex items-center justify-center font-mono text-[13px] font-semibold text-muted">
                            3
                        </div>
                        <div class="flex-1 text-sm text-toned">
                            With a source and destination ready, create a job to run them on a schedule.
                        </div>
                        <UButton icon="i-lucide-plus"
                                 label="New job"
                                 size="md"
                                 @click="handleCreate" />
                    </div>

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

        <RunModal v-if="runModalJob"
                  v-model:open="runModalOpen"
                  :target="runModalJob" />
    </div>
</template>
