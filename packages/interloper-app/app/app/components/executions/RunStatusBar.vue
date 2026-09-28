<script setup lang="ts">
import type { RunStats, RunStatusBucket } from '~/composables/runStats'

const props = defineProps<{
    stats: RunStats
}>()

/** Active status bucket key (e.g. "failed"); filters the assets, the timeline and the events. */
const statusFilter = defineModel<string | null>('statusFilter', { default: null })

const legend = computed(() => props.stats.buckets.filter(b => b.core || b.count > 0))

/** Toggle a status filter; empty buckets aren't selectable (nothing to show). */
function toggle(bucket: RunStatusBucket) {
    if (bucket.count === 0) return
    statusFilter.value = statusFilter.value === bucket.key ? null : bucket.key
}
</script>

<template>
    <div class="flex shrink-0 flex-col gap-2.5 border-b border-default px-4 py-3">
        <div class="flex items-center gap-3.5">
            <div class="flex shrink-0 items-baseline gap-[5px]">
                <span class="text-[15px] font-semibold tabular-nums">{{ stats.total }}</span>
                <span class="text-[12.5px] text-muted">assets</span>
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
