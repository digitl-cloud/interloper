<script setup lang="ts">
/**
 * Usage-against-limit meter: a progress bar and the percentage.
 * Success below 75%, warning from 75%, error from 90%: the shared quota
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

const color = computed(() => {
    if (pct.value == null) return 'success' as const
    if (pct.value >= 90) return 'error' as const
    if (pct.value >= 75) return 'warning' as const
    return 'success' as const
})

const TEXT = { success: 'text-success', warning: 'text-warning', error: 'text-error' } as const
</script>

<template>
    <div v-if="pct != null"
         class="flex min-w-0 items-center gap-2">
        <UProgress :model-value="Math.min(pct, 100)"
                   :color="color"
                   size="sm"
                   class="min-w-0 flex-1" />
        <span v-if="showLabel"
              class="w-9 text-right text-xs font-semibold tabular-nums"
              :class="TEXT[color]">{{ pct }}%</span>
    </div>
</template>
