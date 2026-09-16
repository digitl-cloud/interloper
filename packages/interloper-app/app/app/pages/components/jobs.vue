<script setup lang="ts">
import { h, resolveComponent } from 'vue'
import type { TableColumn, DropdownMenuItem } from '@nuxt/ui'
import type { ComponentRecord, RelationRef } from '~/types/component'
import { jobCron, jobEnabled, jobTimezone, relationRefs } from '~/types/component'

definePageMeta({ title: 'Components', fullBleed: true })

const USwitch = resolveComponent('USwitch')
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

componentsStore.fetchAll(['job'])

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

/** Jobs whose enabled flag is mid-flight, so the switch can't be double-fired. */
const toggling = ref(new Set<string>())

/**
 * Flip a job's schedule on or off from the table. The flag lives in the job's
 * config, so the whole config rides along: a PUT replaces the facet it carries.
 */
async function setEnabled(job: ComponentRecord, enabled: boolean) {
    toggling.value = new Set(toggling.value).add(job.id)
    try {
        await componentsStore.update(job.id, { config: { ...(job.config ?? {}), enabled } })
        toast.add({
            title: `${job.name ?? 'Job'} ${enabled ? 'enabled' : 'disabled'}`,
            color: enabled ? 'success' : 'neutral',
        })
    }
    catch (e) {
        toast.add(errorToast(e, `Failed to ${enabled ? 'enable' : 'disable'} job`))
    }
    finally {
        const pending = new Set(toggling.value)
        pending.delete(job.id)
        toggling.value = pending
    }
}

/** The components a job targets, in relation order, as its bindings name them. */
function targetsOf(job: ComponentRecord): RelationRef[] {
    return relationRefs(job, 'targets')
}

/** A job's last run reads as one badge — its age coloured by how the run ended. */
function lastRunColumn(): TableColumn<ComponentRecord> {
    return {
        id: 'last_run_at',
        header: 'Last run',
        accessorFn: (row: ComponentRecord) => row.state?.last_run_at ?? '',
        cell: ({ row }) => {
            const state = row.original.state ?? {}
            if (!state.last_run_at) return h('span', { class: 'text-dimmed' }, '—')
            const status = state.last_run_status as string | undefined
            return h(UBadge, { color: statusColor(status), icon: statusIcon(status) }, () =>
                `${timeSince(new Date(state.last_run_at))} ago`)
        },
    }
}

const columns = computed<TableColumn<ComponentRecord>[]>(() => [
    nameColumn('Job'),
    {
        id: 'schedule',
        header: 'Schedule',
        accessorFn: (row: ComponentRecord) => scheduleSummary(jobCron(row), jobTimezone(row)),
        cell: ({ row }) => h('span', {
            class: 'text-muted',
            title: jobCron(row.original),
        }, scheduleSummary(jobCron(row.original), jobTimezone(row.original))),
    },
    {
        id: 'targets',
        header: 'Targets',
        accessorFn: (row: ComponentRecord) => targetsOf(row).map(t => t.dst_name ?? t.dst_key).join(', '),
        cell: ({ row }) => {
            const targets = targetsOf(row.original)
            if (targets.length === 0) return h('span', { class: 'text-muted' }, '—')
            const first = targets[0]!
            return h(EntityBadge, {
                icon: componentIcon(first.dst_key),
                label: first.dst_name ?? first.dst_key,
                extra: targets.length - 1,
            })
        },
    },
    ...stateSchemaColumns(catalogStore.definitionsForKind('job')[0])
        .map(column => (column.id === 'last_run_at' ? lastRunColumn() : column)),
    dateColumn<ComponentRecord>('created_at', 'Created'),
    {
        id: 'enabled',
        header: 'Enabled',
        accessorFn: (row: ComponentRecord) => String(jobEnabled(row)),
        cell: ({ row }) => h('div', {
            // The row itself opens the edit drawer; the switch owns its click.
            onClick: (event: Event) => event.stopPropagation(),
        }, [
            h(USwitch, {
                modelValue: jobEnabled(row.original),
                disabled: toggling.value.has(row.original.id),
                'aria-label': jobEnabled(row.original) ? 'Disable job' : 'Enable job',
                'onUpdate:modelValue': (value: boolean) => setEnabled(row.original, value),
            }),
        ]),
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

const enabled = ref<boolean | null>(null)
</script>

<template>
    <div>
        <NavActions>
            <UButton icon="i-lucide-plus"
                     label="New job"
                     @click="handleCreate" />
        </NavActions>
        <NavComponentsHub>
            <DataTable :columns="columns"
                       :data="jobs"
                       :filter="row => enabled === null || jobEnabled(row) === enabled"
                       :loading="componentsStore.loading"
                       :error="componentsStore.error"
                       :row-actions="rowActions"
                       :delete-impact="componentsStore.deleteImpact"
                       search-placeholder="Search jobs..."
                       @delete="handleDelete"
                       @edit="handleEdit"
                       @retry="componentsStore.fetchAll(['job'])">
                <template #filters>
                    <EnabledFilter v-model="enabled" />
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
        </NavComponentsHub>

        <WizardDrawer v-model:open="drawerOpen"
                      :default-title="editingJob ? 'Edit Job' : 'New Job'"
                      description="Configure job"
                      :stepper="stepperRef">
            <WizardDefinitionStepper v-if="drawerOpen"
                                     :key="editingJob?.id ?? 'new'"
                                     ref="stepperRef"
                                     kind="job"
                                     definition-key="cron_job"
                                     noun="Job"
                                     :component="editingJob"
                                     :relation-steps="[{ name: 'targets', standaloneAssetsOnly: true }]"
                                     :exclude="['lookback', 'offset']"
                                     @created="handleSaved"
                                     @updated="handleSaved">
                <template #details="{ relations, extra, configData }">
                    <JobsWindowSection v-model:config="extra.config"
                                       v-model:valid="extra.valid"
                                       :target-ids="relations.targets ?? []"
                                       :job="editingJob"
                                       :timezone="typeof configData.timezone === 'string' ? configData.timezone : undefined" />
                </template>
            </WizardDefinitionStepper>
        </WizardDrawer>

        <ExecutionsRunModal v-if="runModalJob"
                  v-model:open="runModalOpen"
                  :target="runModalJob" />
    </div>
</template>
