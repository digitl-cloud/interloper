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
import { jobCron, jobEnabled, jobLookback, jobOffset, jobTags, jobTargetIds } from '~/types/component'
import type { SourceDefinition } from '~/types/catalog'
import cronstrue from 'cronstrue'

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
const tags = ref<string[]>([])
const enabled = ref(true)
const lookback = ref<number | null>(null)
const offset = ref<number>(1)
const selectedSourceIds = ref<string[]>([])
/** Asset targets carried through unchanged — the stepper only edits source targets. */
const selectedAssetIds = ref<string[]>([])
const submitting = ref(false)

const isEditing = computed(() => !!props.job)

/**
 * The dates a run today would cover, mirroring `lookback_window` on the
 * backend: `offset` partitions back from today, spanning `lookback` of them.
 * Daily is the only granularity an asset can declare, so days are safe here.
 */
const windowPreview = computed(() => {
    const span = lookback.value ?? 0
    if (span < 1 || offset.value < 0) return null
    const end = new Date()
    end.setUTCDate(end.getUTCDate() - offset.value)
    const start = new Date(end)
    start.setUTCDate(start.getUTCDate() - (span - 1))
    const iso = (d: Date) => d.toISOString().slice(0, 10)
    return span === 1 ? iso(end) : `${iso(start)} to ${iso(end)}`
})

// ── Data fetching ────────────────────────────────────────────────

onMounted(async () => {
    await componentsStore.fetchAll(['source'])
    if (props.job) {
        name.value = props.job.name ?? ''
        cron.value = jobCron(props.job)
        tags.value = [...jobTags(props.job)]
        enabled.value = jobEnabled(props.job)
        lookback.value = jobLookback(props.job)
        offset.value = jobOffset(props.job)
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
    !!name.value.trim() && !!cron.value.trim(),
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
            tags: [...tags.value],
            enabled: enabled.value,
            partitioned: partitioned.value,
            lookback: partitioned.value ? (lookback.value ?? null) : null,
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
                cron: cron.value.trim(),
                tags: tags.value,
                enabled: enabled.value,
                partitioned: partitioned.value,
                lookback: partitioned.value ? (lookback.value ?? null) : null,
                offset: partitioned.value ? offset.value : 1,
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

                <UFormField label="Tags">
                    <UInputTags v-model="tags"
                                placeholder="Add a tag..."
                                class="w-full" />
                </UFormField>

                <div class="flex items-center justify-between">
                    <div>
                        <p class="text-sm font-medium">Enabled</p>
                        <p class="text-xs text-muted">Job will run on the configured schedule.</p>
                    </div>
                    <USwitch v-model="enabled" />
                </div>

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

                    <div class="grid grid-cols-2 gap-3">
                        <UFormField label="Lookback"
                                    description="Partitions covered on every run.">
                            <UInput v-model.number="lookback"
                                    type="number"
                                    :min="1"
                                    :max="365"
                                    placeholder="1"
                                    class="w-full" />
                        </UFormField>

                        <UFormField label="Offset"
                                    description="Partitions to stay behind the current one.">
                            <UInput v-model.number="offset"
                                    type="number"
                                    :min="0"
                                    :max="365"
                                    placeholder="1"
                                    class="w-full" />
                        </UFormField>
                    </div>

                    <p v-if="windowPreview"
                       class="text-sm text-muted">
                        Run today, this covers {{ windowPreview }}.
                    </p>
                </template>

            </div>
        </template>
    </UStepper>
</template>
