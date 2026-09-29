<script setup lang="ts">
/**
 * The job wizard's Partitioning section, plugged into the definition-driven
 * wizard's `#section-partitioning` slot: whether the selected targets are
 * partitioned (derived, never asked), the section's generated window fields,
 * and the preview of the partitions a run today would cover.
 */
import type { ComponentRecord } from '~/types/component'
import { jobLookback, jobOffset } from '~/types/component'
import { KEY_PATTERNS, periodKey, targetGranularities, type PartitionGranularity } from '~/composables/partitionGranularity'

const props = defineProps<{
    keys: string[]
    schema: Record<string, any>
    componentKey?: string
    /** The selected target component ids (live, from the wizard). */
    targetIds: string[]
    /** The job being edited, or null — seeds the window fields. */
    job: ComponentRecord | null
    /** The job timezone picked in the wizard — the preview's calendar. */
    timezone?: string
}>()

/** The wizard's extension contract: config merged on submit, valid gating it. */
const config = defineModel<Record<string, unknown>>('config', { default: () => ({}) })
const valid = defineModel<boolean>('valid', { default: true })

// A stored null lookback means "no window": the job was saved while its targets
// were unpartitioned. Leaving it unset lets the field start from the schema
// default instead of blank.
const partitionConfig = ref<Record<string, unknown>>(
    props.job ? { lookback: jobLookback(props.job) ?? undefined, offset: jobOffset(props.job) } : {},
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

const windowPreview = computed(() => {
    const span = lookback.value
    if (!Number.isFinite(span) || span < 1 || offset.value < 0) return null
    const zone = props.timezone || 'UTC'
    const end = periodKey(windowGranularity.value, offset.value, zone)
    const start = periodKey(windowGranularity.value, offset.value + span - 1, zone)
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
    <template v-if="partitioned">
        <div class="flex items-center gap-2 text-sm text-muted">
            <UIcon name="i-lucide-calendar-days"
                   class="size-4 shrink-0" />
            <span>Partitioned: the selected targets contain time-partitioned assets.</span>
        </div>

        <SchemaForm v-model:data="partitionConfig"
                    v-model:is-valid="partitionConfigValid"
                    :schema="schema"
                    :component-key="componentKey"
                    :include="keys"
                    nested />

        <p v-if="windowPreview"
           class="text-sm text-muted">
            Run today, this covers {{ windowPreview }}.
        </p>
    </template>
</template>
