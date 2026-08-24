<script setup lang="ts">
/**
 * Two-step form for creating and editing jobs.
 *
 * Step 1: Source selection (standalone mode only)
 * Step 2: Job details (name, cron, tags, enabled, lookback/offset)
 *
 * Container-agnostic: the parent wraps this in a UDrawer, modal, or
 * any other container. Navigation state is exposed via defineExpose.
 */
import type { StepperItem } from '@nuxt/ui'
import type { ComponentRecord } from '~/types/component'
import { jobCron, jobLookback, jobOffset, jobTargetIds } from '~/types/component'
import type { SourceDefinition } from '~/types/catalog'
import cronstrue from 'cronstrue'
import { KEY_PATTERNS, type PartitionGranularity } from '~/composables/partitionGranularity'

const props = withDefaults(defineProps<{
    /** Pass an existing job to edit, or null to create. */
    job: ComponentRecord | null
    /** 'standalone' saves to API, 'collect' emits config without saving. */
    mode?: 'standalone' | 'collect'
    /** Asset keys selected by the parent (collect mode). Used to derive partitioning. */
    assetKeys?: string[]
}>(), {
    mode: 'standalone',
    assetKeys: () => [],
})

const emit = defineEmits<{
    created: []
    updated: []
    collected: [config: { name: string; cron: string; tags: string[]; enabled: boolean; partitioned: boolean; lookback: number | null; offset: number }]
}>()

const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const toast = useToast()

const sources = computed(() => componentsStore.byKind('source'))

// ── Form state ──────────────────────────────────────────────────
const name = ref('')
const cron = ref('')
/**
 * Config fields rendered from `cron_job`'s definition rather than by hand.
 * The bespoke ones stay hand-built: `cron` has presets and a human-readable
 * rendering, `partitioned` is derived from the targets rather than asked.
 * The window fields render in their own Partitions section; everything else
 * (`tags`, `enabled`, and any field added to `CronJob` later) lands in the
 * general form automatically.
 */
const JOB_KEY = 'cron_job'
const BESPOKE_FIELDS = ['cron', 'partitioned']
const WINDOW_FIELDS = ['lookback', 'offset']
const configData = ref<Record<string, unknown>>({})
const configValid = ref(true)
const partitionConfig = ref<Record<string, unknown>>({})
const partitionConfigValid = ref(true)
const selectedSourceIds = ref<string[]>([])
/** Asset targets carried through unchanged — the stepper only edits source targets. */
const selectedAssetIds = ref<string[]>([])
const submitting = ref(false)

const isEditing = computed(() => !!props.job)

/** `cron_job`'s config schema, the source of every non-bespoke field below. */
const jobSchema = computed(() => catalogStore.catalog[JOB_KEY]?.config_schema)

/**
 * The partitions a run today would cover, mirroring the backend's
 * `TimePartitionWindow.lookback`: `offset` partitions back from the current
 * one, spanning `lookback` of them, stepped in the granularity the selected
 * sources' assets declare.
 */
const lookback = computed(() => Number(partitionConfig.value.lookback ?? 0))
const offset = computed(() => Number(partitionConfig.value.offset ?? 1))

function periodKey(granularity: PartitionGranularity, periodsBack: number): string {
    const now = new Date()
    switch (granularity) {
        case 'hour': now.setUTCHours(now.getUTCHours() - periodsBack); return now.toISOString().slice(0, 13)
        case 'day': now.setUTCDate(now.getUTCDate() - periodsBack); return now.toISOString().slice(0, 10)
        case 'month': now.setUTCMonth(now.getUTCMonth() - periodsBack); return now.toISOString().slice(0, 7)
        case 'year': return String(now.getUTCFullYear() - periodsBack)
    }
}

const windowPreview = computed(() => {
    const span = lookback.value
    if (!Number.isFinite(span) || span < 1 || offset.value < 0) return null
    const end = periodKey(windowGranularity.value, offset.value)
    const start = periodKey(windowGranularity.value, offset.value + span - 1)
    return span === 1 ? end : `${start} to ${end}`
})

// ── Data fetching ────────────────────────────────────────────────

onMounted(async () => {
    await componentsStore.fetchAll(['source'])
    if (props.job) {
        name.value = props.job.name ?? ''
        cron.value = jobCron(props.job)
        configData.value = Object.fromEntries(
            Object.entries(props.job.config ?? {})
                .filter(([key]) => !BESPOKE_FIELDS.includes(key) && !WINDOW_FIELDS.includes(key)),
        )
        partitionConfig.value = { lookback: jobLookback(props.job), offset: jobOffset(props.job) }
        selectedSourceIds.value = jobTargetIds(props.job, 'source')
        selectedAssetIds.value = jobTargetIds(props.job, 'asset')
    }
})

// ── Partitioning (auto-detected) ────────────────────────────────

function sourceHasPartitionedAssets(defn: SourceDefinition): boolean {
    return defn.assets.some(a => a.partitioning != null)
}

const partitioned = computed(() => {
    if (props.mode === 'collect') {
        if (props.assetKeys.length === 0) return false
        const assetKeySet = new Set(props.assetKeys)
        return catalogStore.sourceDefinitions.some(sd =>
            sd.assets.some(a => assetKeySet.has(a.key) && a.partitioning != null),
        )
    }
    for (const sourceId of selectedSourceIds.value) {
        const source = componentsStore.byId(sourceId)
        if (!source) continue
        const defn = catalogStore.getSourceDefinition(source.key)
        if (defn && sourceHasPartitionedAssets(defn)) return true
    }
    return false
})

/** The selected sources' granularity, for the window preview's step size. */
const windowGranularity = computed<PartitionGranularity>(() => {
    const found = new Set<string>()
    for (const sourceId of selectedSourceIds.value) {
        const source = componentsStore.byId(sourceId)
        const defn = source ? catalogStore.getSourceDefinition(source.key) : undefined
        for (const asset of defn?.assets ?? []) {
            if (asset.partitioning == null) continue
            const granularity = asset.partitioning.granularity
            found.add(typeof granularity === 'string' ? granularity : 'day')
        }
    }
    const [only] = found
    return found.size === 1 && only !== undefined && only in KEY_PATTERNS
        ? only as PartitionGranularity
        : 'day'
})

// ── Cron helpers ────────────────────────────────────────────────
const cronPresets = [
    { label: 'Every hour', value: '0 * * * *' },
    { label: 'Every 6 hours', value: '0 */6 * * *' },
    { label: 'Daily at midnight', value: '0 0 * * *' },
    { label: 'Daily at 6 AM', value: '0 6 * * *' },
    { label: 'Weekly (Monday)', value: '0 0 * * 1' },
    { label: 'Monthly (1st)', value: '0 0 1 * *' },
]

const cronDescription = computed(() => {
    if (!cron.value) return ''
    try {
        return cronstrue.toString(cron.value, { use24HourTimeFormat: true })
    }
    catch {
        return ''
    }
})

// ── Stepper ─────────────────────────────────────────────────────
const steps = computed<StepperItem[]>(() => {
    const items: StepperItem[] = []
    if (props.mode !== 'collect') {
        items.push({ title: 'Sources', icon: 'i-lucide-plug', slot: 'sources' as const })
    }
    items.push({ title: 'Details', icon: 'i-lucide-settings-2', slot: 'details' as const })
    return items
})

const { activeStep, hasPrev, hasNext, isLastStep, reset: resetStepper, next: nextStep, prev: prevStep } = useStepperFlow(computed(() => steps.value.length))

const displaySteps = useCheckedSteps(steps, activeStep)

/** Recap of the sources chosen on step 1 (standalone mode only). */
const recapRows = computed(() => {
    if (props.mode === 'collect') return []
    const names = selectedSourceIds.value
        .map(id => componentsStore.byId(id)?.name)
        .filter(Boolean)
    return [{
        icon: 'i-lucide-plug',
        label: 'Sources',
        value: names.length ? names.join(', ') : 'None',
    }]
})

// ── Validation ──────────────────────────────────────────────────
const detailsValid = computed(() =>
    !!name.value.trim() && !!cron.value.trim() && configValid.value
    && (!partitioned.value || partitionConfigValid.value),
)

const canProceed = computed(() => {
    const slot = steps.value[activeStep.value]?.slot
    if (slot === 'sources') return selectedSourceIds.value.length > 0
    if (slot === 'details') return detailsValid.value
    return false
})

// ── Submit ──────────────────────────────────────────────────────
async function submit() {
    if (props.mode === 'collect') {
        emit('collected', {
            name: name.value.trim(),
            cron: cron.value.trim(),
            tags: [...(configData.value.tags as string[] ?? [])],
            enabled: (configData.value.enabled as boolean) ?? true,
            partitioned: partitioned.value,
            lookback: partitioned.value ? (lookback.value || null) : null,
            offset: partitioned.value ? offset.value : 1,
        })
        return
    }

    submitting.value = true
    try {
        const targetIds = selectedSourceIds.value.concat(selectedAssetIds.value)
        const input = {
            name: name.value.trim(),
            config: {
                ...configData.value,
                cron: cron.value.trim(),
                partitioned: partitioned.value,
                ...(partitioned.value ? partitionConfig.value : { lookback: null, offset: 1 }),
            },
            relations: {
                target: targetIds.map(id => ({ dst_id: id })),
            },
        }

        if (props.job) {
            await componentsStore.update(props.job.id, input)
            toast.add({ title: 'Job updated', color: 'success' })
            emit('updated')
        }
        else {
            await componentsStore.create({ ...input, kind: 'job', key: 'cron_job' })
            toast.add({ title: 'Job created', color: 'success' })
            emit('created')
        }
    }
    catch (e) {
        toast.add(errorToast(e, `Failed to ${isEditing.value ? 'update' : 'create'} job`))
    }
    finally {
        submitting.value = false
    }
}

function handleNext() {
    if (isLastStep.value) submit()
    else nextStep()
}

// ── Expose navigation state ──────────────────────────────────────

const title = computed(() => isEditing.value ? 'Edit Job' : 'New Job')
const submitLabel = computed(() => {
    if (!isLastStep.value) return 'Next'
    if (props.mode === 'collect') return 'Confirm Job'
    return isEditing.value ? 'Save Job' : 'Create Job'
})

defineExpose({ canProceed, hasPrev, isLastStep, submitting, submitLabel, title, next: handleNext, prev: prevStep })
</script>

<template>
    <UStepper v-model="activeStep"
              :items="displaySteps"
              :linear="steps.length > 1"
              :disabled="steps.length > 1"
              class="w-full">
        <!-- Sources (first in standalone, skipped in collect mode) -->
        <template #sources>
            <JobsSourceSelect v-model="selectedSourceIds"
                              :sources="sources" />
        </template>

        <!-- Details -->
        <template #details>
            <div class="flex flex-col gap-6">
                <WizardRecap v-if="recapRows.length"
                             :rows="recapRows" />

                <USeparator label="Configuration" />

                <UFormField label="Job name"
                            required>
                    <UInput v-model="name"
                            placeholder="My job"
                            class="w-full" />
                </UFormField>

                <SchemaForm v-if="jobSchema"
                            v-model:data="configData"
                            v-model:is-valid="configValid"
                            :schema="jobSchema"
                            :component-key="JOB_KEY"
                            :exclude="[...BESPOKE_FIELDS, ...WINDOW_FIELDS]" />

                <USeparator label="Schedule" />

                <UFormField label="Cron expression"
                            required
                            :description="cronDescription">
                    <UInput v-model="cron"
                            placeholder="0 0 * * *"
                            class="w-full font-mono" />
                </UFormField>

                <div class="flex flex-wrap gap-1.5">
                    <UButton v-for="preset in cronPresets"
                             :key="preset.value"
                             size="xs"
                             variant="soft"
                             color="neutral"
                             :label="preset.label"
                             @click="cron = preset.value" />
                </div>

                <USeparator label="Partitions" />

                <template v-if="partitioned">
                    <div class="flex items-center gap-2 text-sm text-muted">
                        <UIcon name="i-lucide-calendar-days"
                               class="size-4 shrink-0" />
                        <span>Partitioned — selected sources contain date-partitioned assets.</span>
                    </div>

                    <SchemaForm v-if="jobSchema"
                                v-model:data="partitionConfig"
                                v-model:is-valid="partitionConfigValid"
                                :schema="jobSchema"
                                :component-key="JOB_KEY"
                                :include="WINDOW_FIELDS" />

                    <p v-if="windowPreview"
                       class="text-sm text-muted">
                        Run today, this covers {{ windowPreview }}.
                    </p>
                </template>

            </div>
        </template>
    </UStepper>
</template>
