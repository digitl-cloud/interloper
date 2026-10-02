<script setup lang="ts">
import type { RunStats, RunStatusBucket } from '~/composables/runStats'

const props = withDefaults(defineProps<{
    stats: RunStats
    /** What the total counts, e.g. "assets" or "runs". */
    noun?: string
}>(), { noun: 'assets' })

/** Active status bucket key (e.g. "failed"); the host filters what it shows by it. */
const statusFilter = defineModel<string | null>('statusFilter', { default: null })

const legend = computed(() => props.stats.buckets.filter(b => b.core || b.count > 0))

/** Toggle a status filter; empty buckets aren't selectable (nothing to show). */
function toggle(bucket: RunStatusBucket) {
    if (bucket.count === 0) return
    statusFilter.value = statusFilter.value === bucket.key ? null : bucket.key
}
</script>

<template>
    <div class="flex flex-col gap-2.5">
        <div class="flex items-center gap-3.5">
            <div class="flex shrink-0 items-baseline gap-1">
                <span class="text-base font-semibold tabular-nums">{{ stats.total }}</span>
                <span class="text-xs text-muted">{{ noun }}</span>
            </div>
            <UProgressGroup :items="progressSegments(stats)"
                            :max="stats.total"
                            size="md"
                            class="flex-1" />
        </div>

        <div class="flex flex-wrap gap-1.5">
            <UButton v-for="bucket in legend"
                     :key="bucket.key"
                     size="xs"
                     color="neutral"
                     variant="soft"
                     :active="statusFilter === bucket.key"
                     active-color="primary"
                     active-variant="outline"
                     :disabled="bucket.count === 0"
                     :aria-pressed="statusFilter === bucket.key"
                     :class="statusFilter && statusFilter !== bucket.key ? 'opacity-60' : ''"
                     @click="toggle(bucket)">
                <template #leading>
                    <span class="size-2 rounded-full"
                          :class="bucket.colorClass" />
                </template>
                {{ bucket.label }}
                <span class="font-semibold tabular-nums">{{ bucket.count }}</span>
            </UButton>
        </div>
    </div>
</template>
