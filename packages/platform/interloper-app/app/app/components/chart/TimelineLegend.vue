<script setup lang="ts">
/**
 * The key to an execution timeline: a swatch shaped like a bar for each status
 * colour its bars take, in the status table's words, and the dashed outline of
 * a scheduled firing when the timeline shows them.
 */
withDefaults(defineProps<{
    /** Show the scheduled-firing outline after the statuses. */
    scheduled?: boolean
}>(), { scheduled: false })

const items = TIMELINE_STATUSES.map(status => ({ status, label: statusMeta(status).label, color: timelineBarColor(status) }))
</script>

<template>
    <div class="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-dimmed">
        <span v-for="item in items"
              :key="item.status"
              class="inline-flex items-center gap-1.5">
            <span class="h-2 w-3.5 rounded-md"
                  :style="{ backgroundColor: item.color }" />{{ item.label }}
        </span>
        <span v-if="scheduled"
              class="inline-flex items-center gap-1.5">
            <span class="h-2 w-3.5 rounded-md border border-dashed"
                  :style="SCHEDULED_BAR_STYLE" />Scheduled
        </span>
    </div>
</template>
