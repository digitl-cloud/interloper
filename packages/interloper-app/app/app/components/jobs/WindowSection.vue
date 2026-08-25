<script setup lang="ts">
/**
 * The job wizard's Partitions section, plugged into the definition-driven
 * wizard's `#details` extension slot: whether the selected targets are
 * partitioned (derived, never asked), the generated window fields, and the
 * preview of the partitions a run today would cover.
 */
import type { ComponentRecord } from '~/types/component'
import { jobLookback, jobOffset } from '~/types/component'
import { KEY_PATTERNS, targetGranularities, type PartitionGranularity } from '~/composables/partitionGranularity'

const props = defineProps<{
    /** The selected target component ids (live, from the wizard). */
    targetIds: string[]
    /** The job being edited, or null — seeds the window fields. */
    job: ComponentRecord | null
}>()

/** The wizard's extension contract: config merged on submit, valid gating it. */
const config = defineModel<Record<string, unknown>>('config', { default: () => ({}) })
const valid = defineModel<boolean>('valid', { default: true })

const JOB_KEY = 'cron_job'
const WINDOW_FIELDS = ['lookback', 'offset']

const catalogStore = useCatalogStore()
const jobSchema = computed(() => catalogStore.catalog[JOB_KEY]?.config_schema)

const partitionConfig = ref<Record<string, unknown>>(
    props.job ? { lookback: jobLookback(props.job), offset: jobOffset(props.job) } : {},
)
const partitionConfigValid = ref(true)

/** Granularities the selected targets declare; empty = nothing partitioned. */
const granularitySet = computed<Set<string>>(() => targetGranularities(props.targetIds))

const partitioned = computed(() => granularitySet.value.size > 0)

/** The selected targets' granularity, for the window preview's step size. */
const windowGranularity = computed<PartitionGranularity>(() => {
    const [only] = granularitySet.value
    return granularitySet.value.size === 1 && only !== undefined && only in KEY_PATTERNS
        ? only as PartitionGranularity
        : 'day'
})

/**
 * The partitions a run today would cover, mirroring the backend's
 * `TimePartitionWindow.lookback`: `offset` partitions back from the current
 * one, spanning `lookback` of them, stepped in the granularity the selected
 * targets declare.
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

// Feed the wizard's extension contract.
watch(
    [partitioned, partitionConfig, partitionConfigValid],
    () => {
        config.value = partitioned.value ? { ...partitionConfig.value } : { lookback: null, offset: 1 }
        valid.value = !partitioned.value || partitionConfigValid.value
    },
    { deep: true, immediate: true },
)
</script>

<template>
    <USeparator label="Partitions" />

    <template v-if="partitioned">
        <div class="flex items-center gap-2 text-sm text-muted">
            <UIcon name="i-lucide-calendar-days"
                   class="size-4 shrink-0" />
            <span>Partitioned: the selected targets contain time-partitioned assets.</span>
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
</template>
