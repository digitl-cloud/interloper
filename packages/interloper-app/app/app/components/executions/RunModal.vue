<script setup lang="ts">
/**
 * Modal for triggering a run of a runnable component (job, source, or asset).
 *
 * Jobs: same date → single run, date range → backfill.
 * Sources/assets: single partition date → run (backfills are job-only).
 * Mirrors the MaterializeModal layout from the old app.
 */
import { today, getLocalTimeZone } from '@internationalized/date'
import type { DateRange } from 'reka-ui'
import type { ComponentRecord } from '~/types/component'
import { jobPartitioned } from '~/types/component'

const open = defineModel<boolean>('open', { default: false })

const props = withDefaults(defineProps<{
    target: ComponentRecord
    /** Whether the target takes a partition date. Defaults to the job's config for jobs. */
    partitioned?: boolean
}>(), {
    // Vue casts an absent Boolean prop to false; default it to undefined so the
    // ?? fallback to the job's config below can actually kick in.
    partitioned: undefined,
})

const runsStore = useRunsStore()
const backfillsStore = useBackfillsStore()
const toast = useToast()

const isJob = computed(() => props.target.kind === 'job')
const partitioned = computed(() => props.partitioned ?? (isJob.value && jobPartitioned(props.target)))

const submitting = ref(false)
const failFast = ref(false)

const now = today(getLocalTimeZone())

const dateRange = shallowRef<DateRange>({ start: now, end: now })

/** Single-date proxy for non-job targets (no backfills, so no range picking). */
const singleDate = computed({
    get: () => dateRange.value.start,
    set: (d) => { dateRange.value = { start: d, end: d } },
})

const startISO = computed(() => dateRange.value.start?.toString())
const endISO = computed(() => dateRange.value.end?.toString())
const isRange = computed(() => startISO.value !== endISO.value)

interface Preset {
    label: string
    icon: string
    range: () => DateRange
}

const presets: Preset[] = [
    {
        label: 'Today',
        icon: 'i-lucide-calendar',
        range: () => {
            const t = today(getLocalTimeZone())
            return { start: t, end: t }
        },
    },
    {
        label: 'Yesterday',
        icon: 'i-lucide-calendar-minus',
        range: () => {
            const t = today(getLocalTimeZone()).subtract({ days: 1 })
            return { start: t, end: t }
        },
    },
    {
        label: 'Last 7 days',
        icon: 'i-lucide-calendar-range',
        range: () => {
            const t = today(getLocalTimeZone())
            return { start: t.subtract({ days: 6 }), end: t }
        },
    },
    {
        label: 'Last 30 days',
        icon: 'i-lucide-calendar-range',
        range: () => {
            const t = today(getLocalTimeZone())
            return { start: t.subtract({ days: 29 }), end: t }
        },
    },
    {
        label: 'This month',
        icon: 'i-lucide-calendar-days',
        range: () => {
            const t = today(getLocalTimeZone())
            return { start: t.set({ day: 1 }), end: t }
        },
    },
    {
        label: 'Last month',
        icon: 'i-lucide-calendar-fold',
        range: () => {
            const t = today(getLocalTimeZone())
            const firstOfLast = t.subtract({ months: 1 }).set({ day: 1 })
            const lastOfLast = t.set({ day: 1 }).subtract({ days: 1 })
            return { start: firstOfLast, end: lastOfLast }
        },
    },
]

/** Non-job targets can't backfill — hide multi-day presets. */
const visiblePresets = computed(() =>
    isJob.value
        ? presets
        : presets.filter((p) => {
            const r = p.range()
            return r.start?.toString() === r.end?.toString()
        }),
)

const activePreset = computed(() =>
    presets.find((p) => {
        const r = p.range()
        return r.start?.toString() === startISO.value && r.end?.toString() === endISO.value
    })?.label,
)

// Reset state when modal opens
watch(open, (isOpen) => {
    if (isOpen) {
        const t = today(getLocalTimeZone())
        dateRange.value = { start: t, end: t }
        failFast.value = false
        submitting.value = false
    }
})

async function onSubmit() {
    if (!startISO.value || !endISO.value) return
    submitting.value = true
    try {
        if (isRange.value) {
            const backfillId = await backfillsStore.createBackfill({
                componentId: props.target.id,
                startDate: startISO.value,
                endDate: endISO.value,
                failFast: failFast.value,
            })
            toast.add({ title: `Backfill queued (${backfillId.slice(0, 8)})`, color: 'success' })
        }
        else {
            const runId = await runsStore.createRun(
                props.target.id,
                partitioned.value ? startISO.value : undefined,
            )
            toast.add({ title: `Run queued (${runId.slice(0, 8)})`, color: 'success' })
        }
        open.value = false
    }
    catch {
        toast.add({ title: `Failed to queue ${isRange.value ? 'backfill' : 'run'}`, color: 'error' })
    }
    finally {
        submitting.value = false
    }
}
</script>

<template>
    <UModal v-model:open="open"
            :ui="{ footer: 'justify-end' }">
        <template #title>
            <span>Run</span>
            <UBadge color="neutral"
                    variant="subtle"
                    class="ml-1.5">
                {{ props.target.name ?? props.target.key }}
            </UBadge>
        </template>

        <template #body>
            <div class="flex">
                <!-- Presets -->
                <div class="flex flex-col gap-1 border-r border-default pr-4">
                    <UButton v-for="preset in visiblePresets"
                             :key="preset.label"
                             :icon="preset.icon"
                             :label="preset.label"
                             :color="activePreset === preset.label ? 'primary' : 'neutral'"
                             :variant="activePreset === preset.label ? 'soft' : 'ghost'"
                             block
                             class="justify-start"
                             @click="dateRange = preset.range()" />
                </div>

                <!-- Calendar -->
                <div class="flex flex-col w-full pl-4">
                    <UCalendar v-if="isJob"
                               v-model="dateRange"
                               range />
                    <UCalendar v-else
                               v-model="singleDate" />
                    <p class="text-xs text-muted mt-3">
                        <template v-if="isRange">
                            This will create a <strong>backfill</strong> with daily runs from {{ startISO }} to {{ endISO }}.
                        </template>
                        <template v-else>
                            This will queue a single <strong>run</strong> for {{ startISO }}.
                        </template>
                    </p>
                </div>
            </div>

            <!-- Options -->
            <div v-if="isRange"
                 class="mt-4 border-t border-default pt-4">
                <div class="flex items-center gap-2 text-xs text-muted mb-2 font-medium">
                    Options
                </div>
                <USwitch v-model="failFast"
                         label="Stop on failure"
                         description="Cancel remaining runs if one fails"
                         size="xs" />
            </div>
        </template>

        <template #footer>
            <UButton label="Cancel"
                     color="neutral"
                     variant="outline"
                     @click="open = false" />
            <UButton :label="isRange ? 'Start Backfill' : 'Start Run'"
                     :loading="submitting"
                     :disabled="!startISO || !endISO"
                     @click="onSubmit" />
        </template>
    </UModal>
</template>
