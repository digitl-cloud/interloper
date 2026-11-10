<script setup lang="ts">
/**
 * Usage-against-limit meter: colored bar + percentage label.
 * Green below 75%, amber from 75%, red from 90% — the shared quota
 * pressure thresholds. Renders nothing without a positive limit
 * (unlimited quotas have no meaningful percentage).
 */
const props = withDefaults(defineProps<{
    used: number
    limit: number | null
    /** Hide the percentage label (tile bars carry their own labels). */
    showLabel?: boolean
}>(), { showLabel: true })

const pct = computed(() => {
    if (props.limit == null || props.limit <= 0) return null
    return Math.round((props.used / props.limit) * 100)
})

const tone = computed(() => {
    if (pct.value == null) return ''
    if (pct.value >= 90) return 'text-error'
    if (pct.value >= 75) return 'text-warning'
    return 'text-success'
})

const fill = computed(() => {
    if (pct.value == null) return ''
    if (pct.value >= 90) return 'bg-error'
    if (pct.value >= 75) return 'bg-warning'
    return 'bg-success'
})
</script>

<template>
    <div v-if="pct != null"
         class="flex items-center gap-2.5 min-w-0">
        <div class="h-1.5 flex-1 min-w-0 rounded-full bg-elevated overflow-hidden">
            <div class="h-full rounded-full"
                 :class="fill"
                 :style="{ width: `${Math.min(pct, 100)}%` }" />
        </div>
        <span v-if="showLabel"
              class="text-xs font-semibold tabular-nums w-9 text-right"
              :class="tone">{{ pct }}%</span>
    </div>
</template>
