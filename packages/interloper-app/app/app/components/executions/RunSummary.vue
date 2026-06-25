<script setup lang="ts">
import type { Run } from '~/types/run'
import type { AssetExecution } from '~/types/asset_execution'
import type { RunStatusBucket } from '~/composables/runStats'

const props = defineProps<{
    run: Run
    assetExecutions: AssetExecution[]
}>()

/** Active status bucket key (e.g. "failed"); filters the assets + events. */
const statusFilter = defineModel<string | null>('statusFilter', { default: null })

const stats = useRunStats(toRef(props, 'run'), toRef(props, 'assetExecutions'))

/** Toggle a status filter; empty buckets aren't selectable (nothing to show). */
function toggleStatus(bucket: RunStatusBucket) {
    if (bucket.count === 0) return
    statusFilter.value = statusFilter.value === bucket.key ? null : bucket.key
}

/** One-line breakdown, omitting empty buckets (e.g. "11 succeeded · 1 failed · 3 not run"). */
const summaryLine = computed(() => {
    const s = stats.value
    const parts: string[] = []
    if (s.succeeded) parts.push(`${s.succeeded} succeeded`)
    if (s.running) parts.push(`${s.running} running`)
    if (s.failed) parts.push(`${s.failed} failed`)
    if (s.notRun) parts.push(`${s.notRun} not run`)
    return parts.join(' · ') || 'No assets'
})

const barSegments = computed(() => stats.value.buckets.filter(b => b.count > 0))
const legendBuckets = computed(() => stats.value.buckets.filter(b => b.core || b.count > 0))

function clockTime(value: string | null): string {
    if (!value) return '—'
    return new Date(value).toLocaleTimeString('en-US', { hour12: false })
}

const metaItems = computed(() => [
    { label: 'Duration', icon: 'i-lucide-timer', value: stats.value.duration ?? '—' },
    { label: 'Started', icon: 'i-lucide-play', value: clockTime(props.run.started_at) },
    { label: 'Job', icon: 'i-lucide-calendar-clock', value: props.run.job?.name ?? 'Manual' },
    { label: 'Attempt', icon: 'i-lucide-rotate-ccw', value: String(props.run.attempt) },
])
</script>

<template>
    <div class="flex flex-col gap-4 rounded-lg border border-default bg-muted p-4 lg:flex-row lg:items-stretch">
        <!-- Counts + proportion bar + legend -->
        <div class="flex flex-1 flex-col justify-between gap-3">
            <div class="flex items-baseline justify-between gap-3">
                <div class="flex items-baseline gap-2">
                    <span class="text-3xl font-semibold tabular-nums">{{ stats.total }}</span>
                    <span class="text-sm text-muted">assets</span>
                </div>
                <span class="text-sm text-muted">{{ summaryLine }}</span>
            </div>

            <div class="flex h-2 w-full overflow-hidden rounded-full bg-accented">
                <div v-for="seg in barSegments"
                     :key="seg.key"
                     :class="seg.colorClass"
                     :style="{ width: seg.percent + '%' }" />
            </div>

            <div class="flex flex-wrap gap-1.5">
                <button v-for="b in legendBuckets"
                        :key="b.key"
                        type="button"
                        :disabled="b.count === 0"
                        :aria-pressed="statusFilter === b.key"
                        class="flex items-center gap-1.5 rounded-md px-2 py-1 text-xs transition-colors"
                        :class="[
                            b.count === 0 ? 'cursor-default opacity-50' : 'cursor-pointer hover:bg-elevated',
                            statusFilter === b.key
                                ? 'bg-elevated ring-1 ring-inset ring-primary'
                                : 'bg-accented',
                            statusFilter && statusFilter !== b.key ? 'opacity-60' : '',
                        ]"
                        @click="toggleStatus(b)">
                    <span class="size-2 rounded-full"
                          :class="b.colorClass" />
                    <span class="text-muted">{{ b.label }}</span>
                    <span class="font-medium tabular-nums">{{ b.count }}</span>
                </button>
            </div>
        </div>

        <!-- Meta grid -->
        <div class="grid shrink-0 grid-cols-2 gap-2 lg:w-80">
            <div v-for="m in metaItems"
                 :key="m.label"
                 class="flex flex-col gap-1 rounded-md border border-default bg-default px-3 py-2">
                <div class="flex items-center gap-1.5 text-[10px] font-medium uppercase tracking-wide text-muted">
                    <UIcon :name="m.icon"
                           class="size-3" />
                    {{ m.label }}
                </div>
                <span class="truncate text-sm font-medium"
                      :title="m.value">{{ m.value }}</span>
            </div>
        </div>
    </div>
</template>
